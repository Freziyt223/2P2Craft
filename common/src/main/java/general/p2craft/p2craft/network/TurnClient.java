package general.p2craft.p2craft.network;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Мінімальний TURN-клієнт (RFC 5766, UDP, IPv4, long-term credentials).
 * Працює на тому ж UdpTransport, що й Client: відповіді сервера приходять у цикл Client,
 * який передає їх сюди через handle().
 *
 * allocate() та createPermission() блокують потік до відповіді сервера, тому
 * їх не можна викликати з мережевого потоку (тобто з колбеків Listener).
 */
public class TurnClient implements AutoCloseable {
    public record Config(String host, int port, String username, String password) {}

    public interface DataHandler {
        void onData(InetSocketAddress from, byte[] data);
    }

    private static final int MAGIC_COOKIE = 0x2112A442;

    private static final int ALLOCATE_REQUEST = 0x0003;
    private static final int ALLOCATE_SUCCESS = 0x0103;
    private static final int REFRESH_REQUEST = 0x0004;
    private static final int CREATE_PERMISSION_REQUEST = 0x0008;
    private static final int CREATE_PERMISSION_SUCCESS = 0x0108;
    private static final int SEND_INDICATION = 0x0016;
    private static final int DATA_INDICATION = 0x0017;

    private static final int ATTR_USERNAME = 0x0006;
    private static final int ATTR_MESSAGE_INTEGRITY = 0x0008;
    private static final int ATTR_ERROR_CODE = 0x0009;
    private static final int ATTR_LIFETIME = 0x000D;
    private static final int ATTR_REALM = 0x0014;
    private static final int ATTR_NONCE = 0x0015;
    private static final int ATTR_XOR_PEER_ADDRESS = 0x0012;
    private static final int ATTR_DATA = 0x0013;
    private static final int ATTR_XOR_RELAYED_ADDRESS = 0x0016;
    private static final int ATTR_REQUESTED_TRANSPORT = 0x0019;

    private static final int REQUEST_ATTEMPTS = 3;
    private static final int REQUEST_TIMEOUT_MS = 1500;
    private static final int ALLOCATION_LIFETIME_S = 600;
    private static final int MAINTENANCE_INTERVAL_S = 180;

    private record Message(int type, byte[] tid, Map<Integer, byte[]> attrs) {
        boolean isError() {
            return (type & 0x0110) == 0x0110;
        }

        int errorCode() {
            byte[] v = attrs.get(ATTR_ERROR_CODE);
            if (v == null || v.length < 4) return -1;
            return (v[2] & 0x07) * 100 + (v[3] & 0xFF);
        }
    }

    private final UdpTransport transport;
    private final Config config;
    private final DataHandler handler;
    private final InetAddress serverAddress;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, CompletableFuture<Message>> pending = new ConcurrentHashMap<>();
    private final Set<InetAddress> permissions = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "p2craft-turn");
        t.setDaemon(true);
        return t;
    });

    private volatile String realm;
    private volatile byte[] nonce;
    private volatile byte[] key;
    private volatile InetSocketAddress relayedAddress;

    public TurnClient(UdpTransport transport, Config config, DataHandler handler) throws UnknownHostException {
        this.transport = transport;
        this.config = config;
        this.handler = handler;
        this.serverAddress = InetAddress.getByName(config.host());
    }

    /** Адреса, яку треба передати іншому піру (його connectViaRelay). */
    public InetSocketAddress getRelayedAddress() {
        return relayedAddress;
    }

    // ------------------------------------------------------------ public API

    public void allocate() throws IOException {
        List<byte[]> attrs = List.of(attr(ATTR_REQUESTED_TRANSPORT, new byte[]{17, 0, 0, 0}));

        // 1. Запит без авторизації -> очікуємо 401 з REALM/NONCE
        Message first = transact(ALLOCATE_REQUEST, attrs, false);

        Message result;
        if (first.type() == ALLOCATE_SUCCESS) {
            result = first;
        } else if (first.isError() && first.errorCode() == 401) {
            updateAuth(first);
            result = authed(ALLOCATE_REQUEST, attrs);
        } else {
            throw new IOException("TURN allocate failed: " + errorText(first));
        }

        if (result.type() != ALLOCATE_SUCCESS)
            throw new IOException("TURN allocate failed: " + errorText(result));

        byte[] relayed = result.attrs().get(ATTR_XOR_RELAYED_ADDRESS);
        if (relayed == null) throw new IOException("TURN response has no relayed address");

        relayedAddress = decodeXorAddress(relayed);

        scheduler.scheduleWithFixedDelay(
            this::maintain, MAINTENANCE_INTERVAL_S, MAINTENANCE_INTERVAL_S, TimeUnit.SECONDS
        );
    }

    /** Дозволяє IP піра надсилати дані на наш relay (і приймати від нього). */
    public void createPermission(InetAddress peer) throws IOException {
        Message r = authed(CREATE_PERMISSION_REQUEST, List.of(attr(ATTR_XOR_PEER_ADDRESS, encodeXorAddress(peer, 0))));

        if (r.type() != CREATE_PERMISSION_SUCCESS)
            throw new IOException("TURN CreatePermission failed: " + errorText(r));

        permissions.add(peer);
    }

    /** Надсилає дані піру через relay (Send indication, відповіді не чекає). */
    public void sendData(InetAddress peer, int port, byte[] data) throws IOException {
        byte[] tid = newTid();

        byte[] message = build(SEND_INDICATION, tid, List.of(
            attr(ATTR_XOR_PEER_ADDRESS, encodeXorAddress(peer, port)),
            attr(ATTR_DATA, data)
        ), false);

        transport.send(serverAddress, config.port(), message);
    }

    /**
     * Викликається циклом Client для кожного пакета.
     * Повертає true, якщо пакет прийшов від TURN-сервера і оброблений тут.
     */
    public boolean handle(DatagramPacket packet) {
        if (!packet.getAddress().equals(serverAddress) || packet.getPort() != config.port()) return false;

        Message m = parse(packet.getData(), packet.getOffset(), packet.getLength());
        if (m == null) return true;

        if (m.type() == DATA_INDICATION) {
            byte[] peer = m.attrs().get(ATTR_XOR_PEER_ADDRESS);
            byte[] data = m.attrs().get(ATTR_DATA);

            if (peer != null && data != null) {
                try {
                    handler.onData(decodeXorAddress(peer), data);
                } catch (IOException ignored) {
                }
            }
        } else {
            CompletableFuture<Message> future = pending.get(HexFormat.of().formatHex(m.tid()));
            if (future != null) future.complete(m);
        }

        return true;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();

        if (key == null || relayedAddress == null) return;

        // Звільняємо allocation (LIFETIME = 0), відповіді не чекаємо
        try {
            List<byte[]> attrs = new ArrayList<>();
            attrs.add(attr(ATTR_LIFETIME, ByteBuffer.allocate(4).putInt(0).array()));
            attrs.addAll(authAttrs());

            transport.send(serverAddress, config.port(), build(REFRESH_REQUEST, newTid(), attrs, true));
        } catch (IOException ignored) {
        }
    }

    // ------------------------------------------------------------ internals

    private void maintain() {
        try {
            authed(REFRESH_REQUEST, List.of(
                attr(ATTR_LIFETIME, ByteBuffer.allocate(4).putInt(ALLOCATION_LIFETIME_S).array())
            ));

            for (InetAddress peer : permissions) createPermission(peer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<byte[]> authAttrs() {
        List<byte[]> attrs = new ArrayList<>();
        attrs.add(attr(ATTR_USERNAME, config.username().getBytes(StandardCharsets.UTF_8)));
        attrs.add(attr(ATTR_REALM, realm.getBytes(StandardCharsets.UTF_8)));
        attrs.add(attr(ATTR_NONCE, nonce));
        return attrs;
    }

    private void updateAuth(Message m) throws IOException {
        byte[] realmBytes = m.attrs().get(ATTR_REALM);
        byte[] nonceBytes = m.attrs().get(ATTR_NONCE);

        if (nonceBytes == null) throw new IOException("TURN server did not provide a nonce");

        if (realmBytes != null) realm = new String(realmBytes, StandardCharsets.UTF_8);
        nonce = nonceBytes;

        if (realm == null) throw new IOException("TURN server did not provide a realm");

        try {
            key = MessageDigest.getInstance("MD5").digest(
                (config.username() + ":" + realm + ":" + config.password()).getBytes(StandardCharsets.UTF_8)
            );
        } catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
    }

    /** Запит з long-term авторизацією; один раз повторює при застарілому nonce (438). */
    private Message authed(int type, List<byte[]> extra) throws IOException {
        if (key == null) throw new IOException("TURN is not authenticated yet");

        for (int attempt = 0; attempt < 2; attempt++) {
            List<byte[]> attrs = new ArrayList<>(extra);
            attrs.addAll(authAttrs());

            Message r = transact(type, attrs, true);

            if (r.isError() && r.errorCode() == 438) {
                updateAuth(r);
                continue;
            }

            return r;
        }

        throw new IOException("TURN nonce was rejected");
    }

    private Message transact(int type, List<byte[]> attrs, boolean sign) throws IOException {
        byte[] tid = newTid();
        byte[] message = build(type, tid, attrs, sign);

        String id = HexFormat.of().formatHex(tid);
        CompletableFuture<Message> future = new CompletableFuture<>();
        pending.put(id, future);

        try {
            for (int i = 0; i < REQUEST_ATTEMPTS; i++) {
                transport.send(serverAddress, config.port(), message);

                try {
                    return future.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                    // повторна відправка
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted", e);
                } catch (ExecutionException e) {
                    throw new IOException(e);
                }
            }

            throw new IOException("TURN request timed out");
        } finally {
            pending.remove(id);
        }
    }

    private byte[] newTid() {
        byte[] tid = new byte[12];
        random.nextBytes(tid);
        return tid;
    }

    private byte[] build(int type, byte[] tid, List<byte[]> attrs, boolean sign) throws IOException {
        int attrsLength = attrs.stream().mapToInt(a -> a.length).sum();
        int length = attrsLength + (sign ? 24 : 0);

        ByteBuffer buffer = ByteBuffer.allocate(20 + length);
        buffer.putShort((short) type);
        buffer.putShort((short) length);
        buffer.putInt(MAGIC_COOKIE);
        buffer.put(tid);
        for (byte[] attr : attrs) buffer.put(attr);

        if (sign) {
            try {
                Mac mac = Mac.getInstance("HmacSHA1");
                mac.init(new SecretKeySpec(key, "HmacSHA1"));

                // Довжина в заголовку вже враховує MESSAGE-INTEGRITY (24 байти)
                byte[] hmac = mac.doFinal(java.util.Arrays.copyOf(buffer.array(), 20 + attrsLength));

                buffer.putShort((short) ATTR_MESSAGE_INTEGRITY);
                buffer.putShort((short) 20);
                buffer.put(hmac);
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        }

        return buffer.array();
    }

    private static byte[] attr(int type, byte[] value) {
        int padded = (value.length + 3) & ~3;

        ByteBuffer buffer = ByteBuffer.allocate(4 + padded);
        buffer.putShort((short) type);
        buffer.putShort((short) value.length);
        buffer.put(value);

        return buffer.array();
    }

    private static Message parse(byte[] data, int offset, int length) {
        if (length < 20) return null;

        ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);

        int type = Short.toUnsignedInt(buffer.getShort());
        int bodyLength = Short.toUnsignedInt(buffer.getShort());
        int cookie = buffer.getInt();

        if ((type & 0xC000) != 0 || cookie != MAGIC_COOKIE || bodyLength % 4 != 0 || 20 + bodyLength > length)
            return null;

        byte[] tid = new byte[12];
        buffer.get(tid);

        Map<Integer, byte[]> attrs = new HashMap<>();
        int end = offset + 20 + bodyLength;

        while (buffer.position() + 4 <= end) {
            int attribute = Short.toUnsignedInt(buffer.getShort());
            int size = Short.toUnsignedInt(buffer.getShort());

            if (buffer.position() + size > end) return null;

            byte[] value = new byte[size];
            buffer.get(value);
            attrs.putIfAbsent(attribute, value);

            int padding = ((size + 3) & ~3) - size;
            buffer.position(Math.min(buffer.position() + padding, end));
        }

        return new Message(type, tid, attrs);
    }

    private static byte[] encodeXorAddress(InetAddress address, int port) throws IOException {
        byte[] ip = address.getAddress();
        if (ip.length != 4) throw new IOException("Only IPv4 is supported");

        byte[] cookie = ByteBuffer.allocate(4).putInt(MAGIC_COOKIE).array();

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put((byte) 0);
        buffer.put((byte) 0x01);
        buffer.putShort((short) (port ^ 0x2112));
        for (int i = 0; i < 4; i++) buffer.put((byte) (ip[i] ^ cookie[i]));

        return buffer.array();
    }

    private static InetSocketAddress decodeXorAddress(byte[] value) throws IOException {
        if (value.length < 8 || value[1] != 0x01) throw new IOException("Only IPv4 is supported");

        int port = (((value[2] & 0xFF) << 8) | (value[3] & 0xFF)) ^ 0x2112;

        byte[] cookie = ByteBuffer.allocate(4).putInt(MAGIC_COOKIE).array();
        byte[] ip = new byte[4];
        for (int i = 0; i < 4; i++) ip[i] = (byte) (value[4 + i] ^ cookie[i]);

        return new InetSocketAddress(InetAddress.getByAddress(ip), port);
    }

    private static String errorText(Message m) {
        byte[] v = m.attrs().get(ATTR_ERROR_CODE);

        if (v == null || v.length < 4) return "unexpected response 0x" + Integer.toHexString(m.type());

        String reason = new String(v, 4, v.length - 4, StandardCharsets.UTF_8);
        return m.errorCode() + " " + reason;
    }
}
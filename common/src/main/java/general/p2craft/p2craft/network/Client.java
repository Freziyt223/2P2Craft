package general.p2craft.p2craft.network;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Host - current computer
 * Target - external computer
 * Client uses STUN to get host's true ip and port.
 * To ensure NAT doesn't interfere with connection UDP hole punching and keepalive packets are implemented
 *
 * If even this doesn't work use TURN relay-server.
 * enable with enableTurn() and use by calling connectViaRelay()
 * 
 * Connection doesn't automatically resolve target's address, target needs to share it with you after getting it from STUN
 */
public class Client implements AutoCloseable {
    // Packet format: [MAGIC0][MAGIC1][type][payload...]
    private static final byte MAGIC0 = 0x50; // 'P'
    private static final byte MAGIC1 = 0x32; // '2'
    private static final int HEADER_SIZE = 3;

    private static final byte TYPE_PUNCH = 1;
    private static final byte TYPE_PUNCH_ACK = 2;
    private static final byte TYPE_KEEPALIVE = 3;
    private static final byte TYPE_SYNC = 4;
    private static final byte TYPE_DATA = 5;
    private static final byte TYPE_DISCONNECT = 6;

    private static final int MTU = 1200;
    private static final int PUNCH_INTERVAL_MS = 200;
    private static final int PUNCH_TIMEOUT_MS = 15_000;
    private static final int KEEPALIVE_INTERVAL_MS = 2_000;
    private static final int PEER_TIMEOUT_MS = 10_000;

    public enum DisconnectReason {
        PUNCH_FAILED, 
        TIMEOUT,
        REMOTE_CLOSED, // Peer closed connection
        LOCAL_CLOSED   // Host closed connection
    }

    public interface Listener {
        default void onConnect(Peer peer) {}

        default void onDisconnect(Peer peer, DisconnectReason reason) {}

        default void onData(Peer peer, byte[] data) {}

        /**
         * Single SYNC message sent with sendSync(). Best effort, no delivery guarantee.
         * Called from network thread!
         */
        default void onSync(Peer peer, byte[] data) {}

        /**
         * Fully received SYNC stream sent with sendStream().
         * Called from network thread: move heavy work (disk, applying world) to another thread!
         */
        default void onStream(Peer peer, byte[] data) {}
    }

    private final UdpTransport transport;
    private final Map<String, Peer> peers = new ConcurrentHashMap<>();
    private final Listener listener;
    private final Sync sync;

    private volatile StunResult publicEndpoint;
    private volatile TurnClient turn;
    private volatile boolean running;
    private Thread thread;

    public Client(int localPort, Listener listener) throws IOException {
        this.transport = new UdpTransport(localPort);
        this.listener = listener;
        this.sync = new Sync(
            (peer, payload) -> sendPacket(peer, TYPE_SYNC, payload),
            listener::onSync,
            listener::onStream
        );
    }

    /**
     * Gets public endpoint by using STUN and starts network cycle.   
     * 
     * CALL ONCE!
     * 
     * @return StunResult
     * @throws Exception
     */
    public StunResult start() throws Exception {
        if (running) throw new IllegalStateException("Already started");

        publicEndpoint = Stun.request(transport);

        running = true;
        thread = new Thread(this::loop, "p2craft-client");
        thread.setDaemon(true);
        thread.start();

        return publicEndpoint;
    }

    /**
     * returns true address and port
     * @return StunResult
     */
    public StunResult getPublicEndpoint() {
        return publicEndpoint;
    }

    public int getLocalPort() {
        return transport.getLocalPort();
    }

    public Collection<Peer> getPeers() {
        return peers.values();
    }

    // ----------------------------------------------------------------
    // TURN
    // ----------------------------------------------------------------

    /**
     * Thread is blocked until full resolve. Don't call inside Listener callback!
     * Return is relay address. You need to pass it to target.
     * @return InetSocketAddress
     * @throws IOException
     */
    public InetSocketAddress enableTurn(TurnClient.Config config) throws IOException {
        if (!running) throw new IllegalStateException("Call start() first");
        if (turn != null) throw new IllegalStateException("TURN is already enabled");

        TurnClient client = new TurnClient(transport, config, this::onRelayData);

        turn = client;

        try {
            client.allocate();
        } catch (IOException | RuntimeException e) {
            turn = null;
            client.close();
            throw e;
        }

        return client.getRelayedAddress();
    }

    public void disableTurn() {
        TurnClient client = turn;
        if (client == null) return;

        for (Peer peer : peers.values()) {
            if (peer.getRoute() == Peer.Route.RELAY) disconnect(peer);
        }

        turn = null;
        client.close();
    }

    public boolean isTurnEnabled() {
        return turn != null;
    }

    public InetSocketAddress getRelayedEndpoint() {
        TurnClient client = turn;
        return client == null ? null : client.getRelayedAddress();
    }

    // ------------------------------------------------------------
    // Peers
    // ------------------------------------------------------------

    /**
     * Both host and target have to connect at the same time
     */
    public Peer connect(InetAddress address, int port) {
        return addPeer(address, port, Peer.Route.DIRECT);
    }

    /**
     * Blocks thread (creates permission on TURN-server), don't call inside Listener callback!
     * @return Peer
     * @throws IOException
     */
    public Peer connectViaRelay(InetAddress relayAddress, int relayPort) throws IOException {
        TurnClient client = turn;
        if (client == null) throw new IllegalStateException("TURN is not enabled");

        client.createPermission(relayAddress);

        return addPeer(relayAddress, relayPort, Peer.Route.RELAY);
    }

    private Peer addPeer(InetAddress address, int port, Peer.Route route) {
        Peer peer = peers.computeIfAbsent(
            Peer.key(route, address, port),
            k -> new Peer(address, port, route)
        );

        if (peer.getState() == Peer.State.NEW) {
            peer.markPunchStart();
            peer.setState(Peer.State.PUNCHING);
        }

        return peer;
    }

    public void send(Peer peer, byte[] data) throws IOException {
        if (!peer.isConnected()) throw new IOException("Peer is not connected: " + peer);

        sendPacket(peer, TYPE_DATA, data);
    }
    /// Sends data to all of the peers
    public void broadcast(byte[] data) throws IOException {
        for (Peer peer : peers.values()) {
            if (peer.isConnected()) sendPacket(peer, TYPE_DATA, data);
        }
    }

    // ------------------------------------------------------------
    // SYNC
    // ------------------------------------------------------------

    /**
     * Sends single SYNC message (one packet, no retransmission).
     * Data must fit in one packet (MTU - 1 byte). For anything bigger or anything that
     * must not be lost use sendStream().
     */
    public void sendSync(Peer peer, byte[] data) throws IOException {
        if (!peer.isConnected()) throw new IOException("Peer is not connected: " + peer);

        sync.sendMessage(peer, data);
    }

    /**
     * Reliably sends big block of data (world snapshot, region file, etc.) as SYNC stream.
     * Future completes when target confirmed everything,
     * or fails if target disconnected or transfer stalled.
     */
    public CompletableFuture<Void> sendStream(Peer peer, byte[] data) {
        if (!peer.isConnected())
            return CompletableFuture.failedFuture(new IOException("Peer is not connected: " + peer));

        return sync.sendStream(peer, data);
    }

    public void disconnect(Peer peer) {
        try {
            sendPacket(peer, TYPE_DISCONNECT, new byte[0]);
        } catch (IOException ignored) {
        }

        drop(peer, DisconnectReason.LOCAL_CLOSED);
    }

    // ----------------------------------------------------------------
    // loop
    // ----------------------------------------------------------------

    private void loop() {
        while (running) {
            try {
                DatagramPacket packet = transport.poll(MTU + HEADER_SIZE + 64);

                if (packet != null) handlePacket(packet);

                tick();
            } catch (IOException e) {
                if (running) e.printStackTrace();
            }
        }
    }

    private void handlePacket(DatagramPacket packet) throws IOException {
        // packets from TURN are handled by TurnClient
        TurnClient client = turn;
        if (client != null && client.handle(packet)) return;

        Peer peer = peers.get(Peer.key(Peer.Route.DIRECT, packet.getAddress(), packet.getPort()));
        if (peer == null) return;

        process(peer, packet.getData(), packet.getOffset(), packet.getLength());
    }

    private void onRelayData(InetSocketAddress from, byte[] data) {
        Peer peer = peers.get(Peer.key(Peer.Route.RELAY, from.getAddress(), from.getPort()));
        if (peer == null) return;

        try {
            process(peer, data, 0, data.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void process(Peer peer, byte[] data, int offset, int length) throws IOException {
        if (length < HEADER_SIZE) return;
        if (data[offset] != MAGIC0 || data[offset + 1] != MAGIC1) return;

        byte type = data[offset + 2];

        peer.markReceived();

        // Any valid packet means target endpoint is valid
        if (peer.getState() == Peer.State.PUNCHING || peer.getState() == Peer.State.NEW) {
            peer.setState(Peer.State.CONNECTED);
            listener.onConnect(peer);
        }

        switch (type) {
            case TYPE_PUNCH -> sendPacket(peer, TYPE_PUNCH_ACK, new byte[0]);
            case TYPE_DATA -> listener.onData(peer, Arrays.copyOfRange(data, offset + HEADER_SIZE, offset + length));
            case TYPE_SYNC -> sync.handle(peer, data, offset + HEADER_SIZE, length - HEADER_SIZE);
            case TYPE_DISCONNECT -> drop(peer, DisconnectReason.REMOTE_CLOSED);
            default -> {
                // PUNCH_ACK, KEEPALIVE - markReceived() is alright
            }
        }
    }

    private void tick() throws IOException {
        long now = System.currentTimeMillis();

        sync.tick(now);

        for (Peer peer : peers.values()) {
            switch (peer.getState()) {
                case PUNCHING -> {
                    if (now - peer.getPunchStartMs() > PUNCH_TIMEOUT_MS) {
                        drop(peer, DisconnectReason.PUNCH_FAILED);
                    } else if (now - peer.getLastSentMs() >= PUNCH_INTERVAL_MS) {
                        sendPacket(peer, TYPE_PUNCH, new byte[0]);
                    }
                }
                case CONNECTED -> {
                    if (now - peer.getLastReceivedMs() > PEER_TIMEOUT_MS) {
                        drop(peer, DisconnectReason.TIMEOUT);
                    } else if (now - peer.getLastSentMs() >= KEEPALIVE_INTERVAL_MS) {
                        sendPacket(peer, TYPE_KEEPALIVE, new byte[0]);
                    }
                }
                default -> {
                }
            }
        }
    }

    private void drop(Peer peer, DisconnectReason reason) {
        peers.remove(peer.key());

        synchronized (peer) {
            if (peer.getState() == Peer.State.DISCONNECTED) return;
            peer.setState(Peer.State.DISCONNECTED);
        }

        sync.peerGone(peer);
        listener.onDisconnect(peer, reason);
    }

    private void sendPacket(Peer peer, byte type, byte[] payload) throws IOException {
        if (payload.length > MTU) throw new IOException("Payload too large: " + payload.length + " > " + MTU);

        byte[] packet = new byte[HEADER_SIZE + payload.length];
        packet[0] = MAGIC0;
        packet[1] = MAGIC1;
        packet[2] = type;
        System.arraycopy(payload, 0, packet, HEADER_SIZE, payload.length);

        if (peer.getRoute() == Peer.Route.RELAY) {
            TurnClient client = turn;
            if (client == null) throw new IOException("TURN is not enabled");

            client.sendData(peer.getAddress(), peer.getPort(), packet);
        } else {
            transport.send(peer.getAddress(), peer.getPort(), packet);
        }

        peer.markSent();
    }

    @Override
    public void close() {
        running = false;

        for (Peer peer : peers.values()) {
            try {
                sendPacket(peer, TYPE_DISCONNECT, new byte[0]);
            } catch (IOException ignored) {
            }
        }

        try {
            if (thread != null) thread.join(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (Peer peer : peers.values()) drop(peer, DisconnectReason.LOCAL_CLOSED);

        TurnClient client = turn;
        turn = null;
        if (client != null) client.close();

        transport.close();
    }
}
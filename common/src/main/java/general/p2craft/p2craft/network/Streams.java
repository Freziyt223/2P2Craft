package general.p2craft.p2craft.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Надійна передача великих блоків даних (стріми) поверх ненадійного UDP.
 *
 * Відправник ріже дані на шматки по CHUNK_SIZE, тримає вікно WINDOW шматків "в польоті"
 * і повторно шле ті, що не підтверджені за RTO_MS.
 * Отримувач збирає шматки в довільному порядку, відповідає ACK (cumulative + bitmap наступних 64)
 * і віддає цілий блок, коли отримав усе.
 *
 * Уся робота (handleData/handleAck/tick) іде з мережевого потоку Client.
 * Обмеження: вікно фіксоване, повноцінного congestion control немає.
 */
public class Streams {
    interface Sender {
        void send(Peer peer, byte type, byte[] payload) throws IOException;
    }

    interface Receiver {
        void onStream(Peer peer, byte[] data);
    }

    static final int MAX_STREAM_SIZE = 256 * 1024 * 1024;

    private static final int CHUNK_SIZE = 1000;
    private static final int CHUNK_HEADER = 12; // id, index, count
    private static final int WINDOW = 128;
    private static final int MAX_SENDS_PER_TICK = 32;
    private static final int RTO_MS = 400;
    private static final int STALL_TIMEOUT_MS = 15_000;
    private static final int COMPLETED_KEEP_MS = 30_000;

    private static class Outgoing {
        final Peer peer;
        final int id;
        final byte[] data;
        final int count;
        final long[] sentAt;
        final boolean[] acked;
        final CompletableFuture<Void> future;
        int base;
        long lastProgress = System.currentTimeMillis();

        Outgoing(Peer peer, int id, byte[] data, int count, CompletableFuture<Void> future) {
            this.peer = peer;
            this.id = id;
            this.data = data;
            this.count = count;
            this.sentAt = new long[count];
            this.acked = new boolean[count];
            this.future = future;
        }
    }

    private static class Incoming {
        final int count;
        final byte[][] chunks;
        int received;
        int next; // перший ще не отриманий шматок
        long lastActivity = System.currentTimeMillis();

        Incoming(int count) {
            this.count = count;
            this.chunks = new byte[count][];
        }
    }

    private record Completed(int count, long time) {}

    private final Sender sender;
    private final Receiver receiver;
    private final AtomicInteger nextId = new AtomicInteger();

    private final Map<String, Outgoing> outgoing = new ConcurrentHashMap<>();
    private final Map<String, Incoming> incoming = new ConcurrentHashMap<>();
    private final Map<String, Completed> completed = new ConcurrentHashMap<>();

    Streams(Sender sender, Receiver receiver) {
        this.sender = sender;
        this.receiver = receiver;
    }

    private static String key(Peer peer, int id) {
        return peer.key() + "#" + id;
    }

    // ------------------------------------------------------------ 
    // sending
    // ------------------------------------------------------------ 
    CompletableFuture<Void> send(Peer peer, byte[] data) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (data.length > MAX_STREAM_SIZE) {
            future.completeExceptionally(new IOException("Stream is too large: " + data.length));
            return future;
        }

        int count = Math.max(1, (data.length + CHUNK_SIZE - 1) / CHUNK_SIZE);
        Outgoing o = new Outgoing(peer, nextId.incrementAndGet(), data, count, future);

        outgoing.put(key(peer, o.id), o);

        return future;
    }

    private void sendChunk(Outgoing o, int index) throws IOException {
        int from = index * CHUNK_SIZE;
        int length = Math.min(CHUNK_SIZE, o.data.length - from);

        ByteBuffer buffer = ByteBuffer.allocate(CHUNK_HEADER + length);
        buffer.putInt(o.id).putInt(index).putInt(o.count).put(o.data, from, length);

        sender.send(o.peer, Sync.SUB_STREAM_DATA, buffer.array());
    }

    private void fail(Outgoing o, IOException error) {
        outgoing.remove(key(o.peer, o.id));
        o.future.completeExceptionally(error);
    }

    void handleAck(Peer peer, byte[] data, int offset, int length) {
        if (length < 16) return;

        ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
        int id = buffer.getInt();
        int cumulative = buffer.getInt();
        long bitmap = buffer.getLong();

        Outgoing o = outgoing.get(key(peer, id));
        if (o == null) return;

        int cum = Math.max(0, Math.min(cumulative, o.count));
        boolean progress = cum > o.base;

        for (int i = o.base; i < cum; i++) o.acked[i] = true;

        // bit i = шматок (cum + 1 + i) уже отриманий
        for (int bit = 0; bit < 64; bit++) {
            int index = cum + 1 + bit;
            if (index >= o.count) break;

            if (((bitmap >>> bit) & 1L) != 0 && !o.acked[index]) {
                o.acked[index] = true;
                progress = true;
            }
        }

        while (o.base < o.count && o.acked[o.base]) o.base++;

        if (progress) o.lastProgress = System.currentTimeMillis();

        if (o.base >= o.count) {
            outgoing.remove(key(peer, id));
            o.future.complete(null);
        }
    }

    // ---------------------------------------------------------- 
    // receiving
    // ------------------------------------------------------------ 

    void handleData(Peer peer, byte[] data, int offset, int length) {
        if (length < CHUNK_HEADER) return;

        ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
        int id = buffer.getInt();
        int index = buffer.getInt();
        int count = buffer.getInt();
        int payloadLength = length - CHUNK_HEADER;

        if (count < 1 || count > MAX_STREAM_SIZE / CHUNK_SIZE + 1) return;
        if (index < 0 || index >= count || payloadLength > CHUNK_SIZE) return;

        String key = key(peer, id);
        long now = System.currentTimeMillis();

        // Стрім вже зібраний, а відправник повторює шматок (наш ACK загубився)
        Completed done = completed.get(key);
        if (done != null) {
            sendAck(peer, id, done.count(), 0L);
            return;
        }

        Incoming in = incoming.computeIfAbsent(key, k -> new Incoming(count));
        if (in.count != count) return;

        in.lastActivity = now;

        if (in.chunks[index] == null) {
            byte[] chunk = new byte[payloadLength];
            buffer.get(chunk);
            in.chunks[index] = chunk;
            in.received++;
        }

        while (in.next < in.count && in.chunks[in.next] != null) in.next++;

        long bitmap = 0L;
        for (int bit = 0; bit < 64; bit++) {
            int i = in.next + 1 + bit;
            if (i >= in.count) break;
            if (in.chunks[i] != null) bitmap |= 1L << bit;
        }

        sendAck(peer, id, in.next, bitmap);

        if (in.received == in.count) {
            incoming.remove(key);
            completed.put(key, new Completed(count, now));

            int total = 0;
            for (byte[] chunk : in.chunks) total += chunk.length;

            byte[] result = new byte[total];
            int position = 0;
            for (byte[] chunk : in.chunks) {
                System.arraycopy(chunk, 0, result, position, chunk.length);
                position += chunk.length;
            }

            receiver.onStream(peer, result);
        }
    }

    private void sendAck(Peer peer, int id, int cumulative, long bitmap) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(id).putInt(cumulative).putLong(bitmap);

        try {
            sender.send(peer, Sync.SUB_STREAM_ACK, buffer.array());
        } catch (IOException ignored) {
            // втрачений ACK відправник компенсує повторною відправкою шматка
        }
    }

    // --------------------------------------------------------------- 
    // tick
    // ------------------------------------------------------------ 

    void tick(long now) {
        for (Outgoing o : outgoing.values()) {
            if (now - o.lastProgress > STALL_TIMEOUT_MS) {
                fail(o, new IOException("Stream stalled"));
                continue;
            }

            int sends = 0;
            int limit = Math.min(o.count, o.base + WINDOW);

            for (int i = o.base; i < limit && sends < MAX_SENDS_PER_TICK; i++) {
                if (o.acked[i]) continue;
                if (o.sentAt[i] != 0 && now - o.sentAt[i] < RTO_MS) continue;

                try {
                    sendChunk(o, i);
                } catch (IOException e) {
                    fail(o, e);
                    break;
                }

                o.sentAt[i] = now;
                sends++;
            }
        }

        incoming.values().removeIf(in -> now - in.lastActivity > STALL_TIMEOUT_MS);
        completed.values().removeIf(c -> now - c.time() > COMPLETED_KEEP_MS);
    }

    /** Пір відключився: скасовуємо його стріми. */
    void peerGone(Peer peer) {
        for (Outgoing o : outgoing.values()) {
            if (o.peer == peer) fail(o, new IOException("Peer disconnected"));
        }

        String prefix = peer.key() + "#";
        incoming.keySet().removeIf(k -> k.startsWith(prefix));
        completed.keySet().removeIf(k -> k.startsWith(prefix));
    }
}
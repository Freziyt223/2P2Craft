package general.p2craft.p2craft.network;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * SYNC-пакети: усе, що Client пересилає з типом TYPE_SYNC.
 *
 * Формат payload: [subtype][body...]
 *
 *   SUB_MESSAGE      - одиночне повідомлення (без гарантій доставки), іде в Listener.onSync
 *   SUB_STREAM_DATA  - шматок стріму (надійна передача великих даних)
 *   SUB_STREAM_ACK   - підтвердження шматків стріму
 *
 * Новий вид SYNC-пакета: додай константу SUB_* і гілку в handle().
 * Уся обробка йде з мережевого потоку Client.
 */
public class Sync {
    interface RawSender {
        /** Відправляє payload пакетом TYPE_SYNC. */
        void send(Peer peer, byte[] payload) throws IOException;
    }

    interface MessageReceiver {
        void onSync(Peer peer, byte[] data);
    }

    static final byte SUB_MESSAGE = 0;
    static final byte SUB_STREAM_DATA = 1;
    static final byte SUB_STREAM_ACK = 2;

    private final RawSender raw;
    private final MessageReceiver messages;
    private final Streams streams;

    Sync(RawSender raw, MessageReceiver messages, Streams.Receiver streamReceiver) {
        this.raw = raw;
        this.messages = messages;
        this.streams = new Streams(this::sendSub, streamReceiver);
    }

    /** Одиночне SYNC-повідомлення: один пакет, без повторів. Розмір обмежений MTU. */
    void sendMessage(Peer peer, byte[] data) throws IOException {
        sendSub(peer, SUB_MESSAGE, data);
    }

    /** Надійна передача великого блоку. */
    CompletableFuture<Void> sendStream(Peer peer, byte[] data) {
        return streams.send(peer, data);
    }

    private void sendSub(Peer peer, byte subtype, byte[] body) throws IOException {
        byte[] payload = new byte[1 + body.length];
        payload[0] = subtype;
        System.arraycopy(body, 0, payload, 1, body.length);

        raw.send(peer, payload);
    }

    void handle(Peer peer, byte[] data, int offset, int length) {
        if (length < 1) return;

        byte subtype = data[offset];
        int bodyOffset = offset + 1;
        int bodyLength = length - 1;

        switch (subtype) {
            case SUB_MESSAGE -> messages.onSync(peer, Arrays.copyOfRange(data, bodyOffset, bodyOffset + bodyLength));
            case SUB_STREAM_DATA -> streams.handleData(peer, data, bodyOffset, bodyLength);
            case SUB_STREAM_ACK -> streams.handleAck(peer, data, bodyOffset, bodyLength);
            default -> {
                // невідомий підтип - ігноруємо
            }
        }
    }

    void tick(long now) {
        streams.tick(now);
    }

    void peerGone(Peer peer) {
        streams.peerGone(peer);
    }
}
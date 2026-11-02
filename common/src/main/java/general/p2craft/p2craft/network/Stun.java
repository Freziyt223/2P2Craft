package general.p2craft.p2craft.network;

import general.p2craft.p2craft.Constants;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Arrays;

public class Stun {
    private static final int MAGIC_COOKIE = 0x2112A442;
    private static final int TIMEOUT = 5000;

    public static StunResult request(UdpTransport transport) throws Exception {
        InetAddress stunAddress = InetAddress.getByName(Constants.STUN_HOST);

        byte[] transactionId = new byte[12];
        new SecureRandom().nextBytes(transactionId);

        ByteBuffer request = ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN);

        // STUN Binding Request
        request.putShort((short) 0x0001);
        request.putShort((short) 0);
        request.putInt(MAGIC_COOKIE);
        request.put(transactionId);

        transport.send(
            stunAddress,
            Constants.STUN_PORT,
            request.array()
        );

        long deadline = System.nanoTime() + TIMEOUT * 1_000_000L;

        while (System.nanoTime() < deadline) {
            DatagramPacket packet = transport.poll(512);

            if (packet == null) continue;

            // Ignore packets from other endpoints
            if (!packet.getAddress().equals(stunAddress) || packet.getPort() != Constants.STUN_PORT) continue;

            ByteBuffer response = ByteBuffer.wrap(
                packet.getData(),
                packet.getOffset(),
                packet.getLength()
            ).order(ByteOrder.BIG_ENDIAN);

            if (response.remaining() < 20) continue;

            int type = Short.toUnsignedInt(response.getShort());
            int length = Short.toUnsignedInt(response.getShort());
            int cookie = response.getInt();

            if (type != 0x0101 || cookie != MAGIC_COOKIE) continue;

            if (length % 4 != 0 || length != response.remaining() - 12) continue;

            byte[] receivedId = new byte[12];
            response.get(receivedId);

            if (!Arrays.equals(transactionId, receivedId)) continue;

            int end = 20 + length;

            while (response.position() + 4 <= end) {
                int attribute = Short.toUnsignedInt(response.getShort());
                int size = Short.toUnsignedInt(response.getShort());

                int paddedSize = (size + 3) & ~3;

                if (response.position() + paddedSize > end)
                    throw new Exception("Invalid STUN attribute");

                if (attribute == 0x0020) {
                    if (size != 8)
                        throw new Exception("Invalid XOR-MAPPED-ADDRESS");

                    response.get(); // Reserved

                    int family = Byte.toUnsignedInt(response.get());

                    if (family != 0x01) throw new Exception("Only IPv4 is supported");

                    int port = Short.toUnsignedInt(response.getShort()) ^ 0x2112;

                    byte[] ip = new byte[4];
                    response.get(ip);

                    byte[] mask = ByteBuffer.allocate(4)
                        .order(ByteOrder.BIG_ENDIAN)
                        .putInt(MAGIC_COOKIE)
                        .array();

                    for (int i = 0; i < 4; i++)
                        ip[i] ^= mask[i];

                    return new StunResult(
                        InetAddress.getByAddress(ip),
                        port
                    );
                }

                response.position(response.position() + paddedSize);
            }
        }

        throw new Exception("STUN request timed out");
    }
}
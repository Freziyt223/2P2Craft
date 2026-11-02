package general.p2craft.p2craft.network;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.SocketException;
import java.io.IOException;

public class UdpTransport implements AutoCloseable {
    private final DatagramSocket socket;

    public UdpTransport(int localPort) throws SocketException {
        socket = new DatagramSocket(localPort);
    }

    public int getLocalPort() {
        return socket.getLocalPort();
    }

    public DatagramPacket poll(int bufferSize) throws IOException {
        socket.setSoTimeout(1);

        byte[] data = new byte[bufferSize];
        DatagramPacket packet = new DatagramPacket(data, data.length);

        try {
            socket.receive(packet);
            return packet;
        } catch (SocketTimeoutException e) {
            return null;
        }
    }

    public DatagramPacket request(
        InetAddress address,
        int port,
        byte[] data,
        int responseSize,
        int timeout
    ) throws IOException {
        socket.setSoTimeout(timeout);

        socket.send(new DatagramPacket(data, data.length, address, port));

        byte[] responseData = new byte[responseSize];
        DatagramPacket response = new DatagramPacket(
            responseData,
            responseData.length
        );

        socket.receive(response);

        return response;
    }

    public void send(
        InetAddress address,
        int port,
        byte[] data
    ) throws IOException {
        socket.send(new DatagramPacket(
            data,
            data.length,
            address,
            port
        ));
    }

    @Override
    public void close() {
        socket.close();
    }
}
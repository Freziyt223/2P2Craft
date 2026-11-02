package general.p2craft.p2craft.network;

import java.net.InetAddress;

public record StunResult(
    InetAddress address,
    int port
) {
    public String getEndpoint() {
        return address.getHostAddress() + ":" + port;
    }
}
package general.p2craft.p2craft.network;

import java.net.InetAddress;

/**
 * Віддалений учасник мережі: його адреса/порт, маршрут (напряму або через TURN) і стан з'єднання.
 */
public class Peer {
    public enum State {
        NEW,           // створений, ще не пробивали
        PUNCHING,      // шлемо PUNCH пакети, чекаємо відповідь
        CONNECTED,     // отримали пакет від піра
        DISCONNECTED   // з'єднання завершене
    }

    public enum Route {
        DIRECT,  // напряму (UDP hole punching), адреса - публічна адреса піра зі STUN
        RELAY    // через TURN, адреса - relayed-адреса піра
    }

    private final InetAddress address;
    private final int port;
    private final Route route;

    private volatile State state = State.NEW;
    private volatile long lastReceivedMs;
    private volatile long lastSentMs;
    private volatile long punchStartMs;

    public Peer(InetAddress address, int port, Route route) {
        this.address = address;
        this.port = port;
        this.route = route;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public Route getRoute() {
        return route;
    }

    public State getState() {
        return state;
    }

    public boolean isConnected() {
        return state == State.CONNECTED;
    }

    public long getLastReceivedMs() {
        return lastReceivedMs;
    }

    public long getLastSentMs() {
        return lastSentMs;
    }

    public long getPunchStartMs() {
        return punchStartMs;
    }

    void setState(State state) {
        this.state = state;
    }

    void markReceived() {
        lastReceivedMs = System.currentTimeMillis();
    }

    void markSent() {
        lastSentMs = System.currentTimeMillis();
    }

    void markPunchStart() {
        punchStartMs = System.currentTimeMillis();
    }

    static String key(Route route, InetAddress address, int port) {
        return route + ":" + address.getHostAddress() + ":" + port;
    }

    String key() {
        return key(route, address, port);
    }

    @Override
    public String toString() {
        return address.getHostAddress() + ":" + port + " [" + route + ", " + state + "]";
    }
}
package general.p2craft.p2craft.gui;

import general.p2craft.p2craft.network.Client;
import general.p2craft.p2craft.network.Peer;
import general.p2craft.p2craft.network.StunResult;
import general.p2craft.p2craft.network.TurnClient;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.components.Button;
import net.minecraft.client.gui.components.EditBox;
import net.minecraft.client.gui.components.StringWidget;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.network.chat.Component;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;

public class P2CraftScreen extends Screen {
    private static volatile Client client;

    private final Screen parent;
    private EditBox localPort;
    private EditBox peerEndpoint;
    private EditBox turnServer;
    private EditBox turnUsername;
    private EditBox turnPassword;
    private StringWidget status;
    private Button turnButton;

    public P2CraftScreen(Screen parent) {
        super(Component.literal("2P2Craft"));
        this.parent = parent;
    }

    @Override
    protected void init() {
        int fieldWidth = Math.min(260, Math.max(120, width - 32));
        int x = (width - fieldWidth) / 2;
        int y = Math.max(28, height / 2 - 88);

        addRenderableWidget(new StringWidget(x, y - 22, fieldWidth, 18,
            Component.literal("2P2Craft peer connection"), font));

        localPort = new EditBox(font, x, y, fieldWidth, 20, Component.literal("Local UDP port"));
        localPort.setValue("0");
        localPort.setHint(Component.literal("Local UDP port (0 = automatic)"));
        addRenderableWidget(localPort);

        peerEndpoint = new EditBox(font, x, y + 22, fieldWidth, 20, Component.literal("Peer endpoint"));
        peerEndpoint.setHint(Component.literal("Peer IP:port"));
        addRenderableWidget(peerEndpoint);

        turnServer = new EditBox(font, x, y + 44, fieldWidth, 20, Component.literal("TURN server"));
        turnServer.setHint(Component.literal("TURN server:port"));
        addRenderableWidget(turnServer);

        turnUsername = new EditBox(font, x, y + 66, fieldWidth, 20, Component.literal("TURN username"));
        turnUsername.setHint(Component.literal("TURN username"));
        addRenderableWidget(turnUsername);

        turnPassword = new EditBox(font, x, y + 88, fieldWidth, 20, Component.literal("TURN password"));
        turnPassword.setHint(Component.literal("TURN password"));
        addRenderableWidget(turnPassword);

        int buttonWidth = (fieldWidth - 4) / 2;
        addRenderableWidget(Button.builder(Component.literal("Start P2P"), button -> startClient())
            .bounds(x, y + 110, buttonWidth, 20).build());
        addRenderableWidget(Button.builder(Component.literal("Connect"), button -> connectToPeer())
            .bounds(x + buttonWidth + 4, y + 110, buttonWidth, 20).build());

        turnButton = Button.builder(Component.literal(turnEnabled() ? "Disable TURN relay" : "Enable TURN relay"),
            button -> toggleTurn()).bounds(x, y + 133, fieldWidth, 20).build();
        addRenderableWidget(turnButton);

        String current = client == null ? "P2P: stopped" : "P2P: " + endpointText();
        status = new StringWidget(x, y + 156, fieldWidth, 24, Component.literal(current), font);
        addRenderableWidget(status);

        addRenderableWidget(Button.builder(Component.literal("Back"), button -> onClose())
            .bounds(x, height - 28, fieldWidth, 20).build());
    }

    private void startClient() {
        if (client != null) {
            setStatus("P2P already running: " + endpointText());
            return;
        }

        final int port;
        try {
            port = Integer.parseInt(localPort.getValue().trim());
            if (port < 0 || port > 65535) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            setStatus("Enter a UDP port from 0 to 65535");
            return;
        }

        setStatus("Starting P2P and discovering public address...");
        CompletableFuture.runAsync(() -> {
            Client created = null;
            try {
                created = new Client(port, new Client.Listener() {
                    @Override public void onConnect(Peer peer) {
                        updateStatus("Connected: " + peer);
                    }

                    @Override public void onDisconnect(Peer peer, Client.DisconnectReason reason) {
                        updateStatus("Disconnected: " + reason);
                    }
                });
                StunResult result = created.start();
                client = created;
                updateStatus("Share this address: " + result.getEndpoint());
            } catch (Exception e) {
                if (created != null) created.close();
                updateStatus("P2P start failed: " + errorMessage(e));
            }
        });
    }

    private void connectToPeer() {
        Client activeClient = client;
        if (activeClient == null) {
            setStatus("Start P2P first");
            return;
        }

        String endpoint = peerEndpoint.getValue().trim();
        int separator = endpoint.lastIndexOf(':');
        if (separator <= 0 || separator == endpoint.length() - 1) {
            setStatus("Enter peer as IP:port");
            return;
        }

        final int port;
        try {
            port = Integer.parseInt(endpoint.substring(separator + 1));
            if (port < 1 || port > 65535) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            setStatus("Peer port must be from 1 to 65535");
            return;
        }

        String host = endpoint.substring(0, separator);
        setStatus("Connecting to " + endpoint + "...");
        CompletableFuture.runAsync(() -> {
            try {
                InetAddress address = InetAddress.getByName(host);
                Peer peer = activeClient.isTurnEnabled()
                    ? activeClient.connectViaRelay(address, port)
                    : activeClient.connect(address, port);
                updateStatus("Connecting: " + peer);
            } catch (Exception e) {
                updateStatus("Connect failed: " + errorMessage(e));
            }
        });
    }

    private void toggleTurn() {
        Client activeClient = client;
        if (activeClient == null) {
            setStatus("Start P2P before enabling TURN relay");
            return;
        }

        if (activeClient.isTurnEnabled()) {
            activeClient.disableTurn();
            turnButton.setMessage(Component.literal("Enable TURN relay"));
            setStatus("TURN relay disabled");
            return;
        }

        String endpoint = turnServer.getValue().trim();
        int separator = endpoint.lastIndexOf(':');
        if (separator <= 0 || separator == endpoint.length() - 1) {
            setStatus("Enter TURN server as host:port");
            return;
        }

        final int port;
        try {
            port = Integer.parseInt(endpoint.substring(separator + 1));
            if (port < 1 || port > 65535) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            setStatus("TURN port must be from 1 to 65535");
            return;
        }

        String username = turnUsername.getValue().trim();
        String password = turnPassword.getValue();
        if (username.isEmpty() || password.isEmpty()) {
            setStatus("Enter TURN username and password");
            return;
        }

        String host = endpoint.substring(0, separator);
        turnButton.active = false;
        setStatus("Enabling TURN relay...");
        CompletableFuture.runAsync(() -> {
            try {
                var relayed = activeClient.enableTurn(new TurnClient.Config(host, port, username, password));
                updateStatus("TURN relay enabled: " + relayed.getAddress().getHostAddress() + ":" + relayed.getPort());
                Minecraft.getInstance().execute(() -> {
                    turnButton.active = true;
                    turnButton.setMessage(Component.literal("Disable TURN relay"));
                });
            } catch (Exception e) {
                updateStatus("TURN enable failed: " + errorMessage(e));
                Minecraft.getInstance().execute(() -> turnButton.active = true);
            }
        });
    }

    private boolean turnEnabled() {
        Client activeClient = client;
        return activeClient != null && activeClient.isTurnEnabled();
    }

    private String endpointText() {
        Client activeClient = client;
        StunResult endpoint = activeClient == null ? null : activeClient.getPublicEndpoint();
        return endpoint == null ? "starting" : endpoint.getEndpoint();
    }

    private String errorMessage(Exception e) {
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }

    private void setStatus(String message) {
        if (status != null) status.setMessage(Component.literal(message));
    }

    private void updateStatus(String message) {
        Minecraft.getInstance().execute(() -> setStatus(message));
    }

    @Override
    public void onClose() {
        Minecraft.getInstance().setScreen(parent);
    }

}

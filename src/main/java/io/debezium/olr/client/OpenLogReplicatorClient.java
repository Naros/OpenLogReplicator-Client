package io.debezium.olr.client;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

/**
 * @author Chris Cranford
 */
@ApplicationScoped
public class OpenLogReplicatorClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLogReplicatorClient.class);

    @ConfigProperty(name = "database")
    private String databaseName;
    @ConfigProperty(name = "host")
    private String networkHostName;
    @ConfigProperty(name = "port")
    private Integer port;

    /**
     * Various protocol states
     */
    enum State {
        DISCONNECTED,
        CONNECTED,
        INFO,
        START,
        REDO,
        STREAMING
    }

    private NetClient client;
    private NetSocket socket;
    private State state;
    private Buffer recvBuffer;

    public OpenLogReplicatorClient() {
        this.client = Vertx.vertx().createNetClient(new NetClientOptions().setConnectTimeout(10000));
        this.state = State.DISCONNECTED;
        this.recvBuffer = Buffer.buffer();

    }

    public void onQuarkusStart(@Observes StartupEvent event) {
        connect();
    }

    public void onQuarkusStop(@Observes ShutdownEvent event) {
        disconnect();
    }

    private void connect() {
        LOGGER.info("Connecting to OpenLogReplicator at {}:{}", networkHostName, port);
        client.connect(port, networkHostName).onComplete(result -> {
            if (result.succeeded()) {
                // Connected
                this.state = State.CONNECTED;
                this.socket = result.result();
                this.socket.handler(this::handleDataReceived);
                this.socket.closeHandler(this::handleClosed);

                // Now that socket is set-up start protocol by sending first request.
                sendInfoRequest();
            }
            else {
                LOGGER.error("Failed to connect: {}", result.cause().getMessage());
            }
        });
    }

    private void disconnect() {
        if (socket != null) {
            client.close().onComplete(result -> {
                this.state = State.DISCONNECTED;
                LOGGER.info("Disconnected from OpenLogReplicator service");
            });
            this.socket = null;
        }
    }

    private void send(State state, GeneratedMessageV3 message) {
        if (this.state != state) {
            LOGGER.info("[PROTOCOL STATE CHANGED] {} => {}", this.state, state);
            this.state = state;
        }

        final Buffer buffer = Buffer.buffer();
        buffer.appendIntLE(message.getSerializedSize());
        buffer.appendBytes(message.toByteArray());
        this.socket.write(buffer, (event) -> {
            LOGGER.info("[SENT]: {}", message.toString().trim());
        });
    }

    private void sendInfoRequest() {
        send(State.INFO, makeRequest(RequestCode.INFO).build());
    }

    private Buffer readMessageFromBuffer(Buffer buffer, int messageSize, int sizeSize) {
        if (buffer.length() == (messageSize + sizeSize)) {
            recvBuffer = Buffer.buffer();
            return buffer.getBuffer(4, buffer.length());
        }
        else {
            final Buffer message = buffer.getBuffer(sizeSize, messageSize + sizeSize);

            final Buffer newBuffer = Buffer.buffer();
            newBuffer.appendBuffer(buffer.slice(sizeSize + messageSize, recvBuffer.length()));
            recvBuffer = newBuffer;

            return message;
        }
    }

    private String dumpMessageAsHex(Buffer message) {
        final byte[] data = message.getBytes();

        StringBuilder sb = new StringBuilder();
        for (int i = 0, j = 1; i < message.length(); ++i, ++j) {
            sb.append(String.format("%02x ", data[i]));
            if (j == 10) {
                sb.append("\n");
                j = 0;
            }
        }
        return sb.toString();
    }

    private void handleClosed(Void d) {
        LOGGER.info("Socket closed.");
    }

    private void handleDataReceived(Buffer buffer) {
        // Received data, append it to the recvBuffer
        LOGGER.debug("Received {} bytes (existing buffer {} bytes)", buffer.length(), recvBuffer.length());
        recvBuffer.appendBuffer(buffer);

        // Each packet received has a minimum of 4 bytes for the length encoded in the stream
        // Only need to process the for-loop if the buffer is at least 4 bytes
        while (recvBuffer.length() >= 4) {
            // Read the message size
            final int messageSize = recvBuffer.getIntLE(0);

            // If the message size exceeds the current buffer; then we don't have enough data
            // to parse the message; it's safe to exit and wait for another callback.
            if (messageSize + 4 > recvBuffer.length()) {
                return;
            }

            final Buffer message = readMessageFromBuffer(recvBuffer, messageSize, 4);

            try {
                final RedoResponse response = RedoResponse.parseFrom(message.getBytes());
                LOGGER.info("Received {} message (size {})\n{}", response.getCode(), messageSize, dumpMessageAsHex(message));

                switch (state) {
                    case INFO -> handleInfoResponse(response);
                    case START -> handleStartResponse(response);
                    case REDO -> handleRedoResponse(response);
                    case STREAMING -> handleStreamingResponse(response);
                    default -> LOGGER.error("Received an unexpected code {}", response.getCode());
                }
            }
            catch (InvalidProtocolBufferException | UnexpectedResponseException e) {
                LOGGER.error("Failed to parse message (state {} size {})\n{}\n{}", state, messageSize, e.getMessage(), dumpMessageAsHex(message));
                disconnect();
            }
        }
    }

    private void handleInfoResponse(RedoResponse response) throws UnexpectedResponseException {
        if (response.getCode() == ResponseCode.READY) {
            send(State.START, makeRequest(RequestCode.START).setScn(-1L).build());
        }
        else if (response.getCode() == ResponseCode.STARTED) {
            send(State.REDO, makeRequest(RequestCode.REDO).build());
        }
        else {
            throw new UnexpectedResponseException(response.getCode());
        }
    }

    private void handleStartResponse(RedoResponse response) throws UnexpectedResponseException {
        if (response.getCode() == ResponseCode.STARTED || response.getCode() == ResponseCode.ALREADY_STARTED) {
            send(State.REDO, makeRequest(RequestCode.REDO).build());
        }
        else {
            throw new UnexpectedResponseException(response.getCode());
        }
    }

    private void handleRedoResponse(RedoResponse response) throws UnexpectedResponseException {
        if (response.getCode() == ResponseCode.STREAMING) {
            this.state = State.STREAMING;
            // No need to acknowledge here
        }
        else {
            throw new UnexpectedResponseException(response.getCode());
        }
    }

    private void handleStreamingResponse(RedoResponse response) throws UnexpectedResponseException {
        // no-op right now
    }

    private RedoRequest.Builder makeRequest(RequestCode code) {
        // Explicitly set "ORACLE" here as it is how it's configured in the OpenLogReplicator.json file
        return RedoRequest.newBuilder().setCode(code).setDatabaseName(databaseName);
    }

    private class UnexpectedResponseException extends Exception {
        public UnexpectedResponseException(ResponseCode code) {
            super("Received an unexpected response: " + code);
        }
    }

}

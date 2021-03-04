package br.com.diego.silva.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.streaming.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

public class NatsStreamConnectionWrapper {

    private static Logger logger = LoggerFactory.getLogger(NatsStreamConnectionWrapper.class);

    private StreamingConnection streamingConnection;
    private Connection natsConnection;
    private final String clusterId;
    private final String clientId;
    private final Map<String, NatsEventHandler> handlers = new HashMap<>();
    private final Map<String, Subscription> subscriptions = new HashMap<>();
    private Timer streammingConnectionTimer = new Timer();

    public NatsStreamConnectionWrapper(String natsUrl, String clusterId, String clientId) {
        this.clientId = clientId;
        this.clusterId = clusterId;
        this.natsConnection = createNatsConnection(natsUrl);
        createStreamingConnection(this.natsConnection);
    }

    private Connection createNatsConnection(String natsUrl) {
        try {
            io.nats.client.Options options = new io.nats.client.Options.Builder().server(natsUrl)
                    .maxReconnects(-1)
                    .reconnectBufferSize(-1)
                    .maxControlLine(1024)
                    .connectionListener((newConnection, type) -> {
                        switch (type) {
                            case RECONNECTED:
                                break;
                            case RESUBSCRIBED:
                                this.reconnect(newConnection);
                                break;
                        }
                    }).build();
            return Nats.connect(options);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    private void createStreamingConnection(Connection connection) {
        try {
            try {
                streammingConnectionTimer.cancel();
                streammingConnectionTimer.purge();
            } catch (Exception e) {
                logger.warn("Exception canceling timer", e);
            }
            Options streamingOpt = new Options.Builder()
                    .natsConn(connection)
                    .clusterId(clusterId)
                    .clientId(clientId)
                    .build();
            this.streamingConnection = new StreamingConnectionFactory(streamingOpt).createConnection();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            streammingConnectionTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    createStreamingConnection(connection);
                }
            }, 2000);
            throw new RuntimeException(e);
        }
    }

    public Subscription subscribe(String topic, NatsEventHandler handler, Optional<String> durableName) {
        try {
            if (subscriptions.containsKey(topic)) {
                try {
                    subscriptions.get(topic).unsubscribe();
                } catch (Exception e) {
                }
            }
            Subscription subscription = streamingConnection.subscribe(topic, msg -> {
                        try {
                            handler.run(msg);
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    },
                    new SubscriptionOptions.Builder().durableName(durableName.orElse("durable_" + topic + "_" + this.clientId))
                            .manualAcks().build());
            handlers.put(topic, handler);
            subscriptions.put(topic, subscription);

            return subscription;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void reconnect(Connection connection) {
        this.natsConnection = connection;
        createStreamingConnection(connection);
        Set<Map.Entry<String, NatsEventHandler>> entries = handlers.entrySet();
        entries.forEach(entry -> subscribe(entry.getKey(), entry.getValue(), Optional.of(subscriptions.get(entry.getKey()).getOptions().getDurableName())));
    }

    public void publish(String subject, byte[] data) throws InterruptedException, TimeoutException, IOException {
        if (streamingConnection == null || streamingConnection.getNatsConnection() == null) {
            throw new RuntimeException("Nats connection is not ok to publish");
        }
        streamingConnection.publish(subject, data);
    }

    public CompletionStage<String> publishAsync(String subject, byte[] data) {
        CompletableFuture<String> future = new CompletableFuture<>();

        if (streamingConnection == null || streamingConnection.getNatsConnection() == null) {
            future.completeExceptionally(new RuntimeException("Nats connection cannot publish because is not connected"));
        }
        try {
            streamingConnection.publish(subject, data, (nuid, ex) -> {
                future.complete(nuid);
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}

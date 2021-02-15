package br.com.diego.silva.nats;

import io.nats.client.Connection;
import io.nats.streaming.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

public class NatsStreamConnectionWrapper {

    private StreamingConnection streamingConnection;
    private final String clusterId;
    private final String clientId;
    private final Map<String, NatsEventHandler> handlers = new HashMap<>();
    private final Map<String, Subscription> subscriptions = new HashMap<>();

    public NatsStreamConnectionWrapper(Connection nastsConnection, String clusterId, String clientId) {
        this.clientId = clientId;
        this.clusterId = clusterId;
        createStreamingConnection(nastsConnection);
    }

    private void createStreamingConnection(Connection connection) {
        try {
            Options streamingOpt = new Options.Builder()
                    .natsConn(connection)
                    .clusterId(clusterId)
                    .clientId(clientId).build();
            this.streamingConnection = new StreamingConnectionFactory(streamingOpt).createConnection();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void subscribe(String topic, NatsEventHandler handler) {
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
                    new SubscriptionOptions.Builder().durableName("durable_" + topic + "_" + this.clientId)
                            .manualAcks().build());

            handlers.put(topic, handler);
            subscriptions.put(topic, subscription);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void reconnect(Connection connection) {
        createStreamingConnection(connection);
        Set<Map.Entry<String, NatsEventHandler>> entries = handlers.entrySet();
        entries.forEach(entri -> subscribe(entri.getKey(), entri.getValue()));
    }

    public void publish(String subject, byte[] data) throws InterruptedException, TimeoutException, IOException {
        if(streamingConnection == null || streamingConnection.getNatsConnection() == null){
            throw new RuntimeException("Nats connection is not ok to publish");
        }
        streamingConnection.publish(subject, data);
    }

    public CompletionStage<String> publishAsync(String subject, byte[] data) {
        CompletableFuture<String> future = new CompletableFuture<>();

        if(streamingConnection == null || streamingConnection.getNatsConnection() == null){
            future.completeExceptionally(new RuntimeException("Nats connection is not ok to publish"));
        }
        try {
            streamingConnection.publish(subject, data, (nuid, ex) -> {
                if (ex != null)
                    future.complete(nuid);
                else
                    future.completeExceptionally(ex);
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}

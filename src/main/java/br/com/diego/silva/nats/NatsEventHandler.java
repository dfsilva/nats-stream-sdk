package br.com.diego.silva.nats;

import io.nats.streaming.Message;

@FunctionalInterface
public interface NatsEventHandler {
    void run(Message message);
}

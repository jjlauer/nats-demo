package com.fizzed.nats.demo;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class NatsStreamPublisher {

    static public void main(String[] args) throws Exception{
        Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .connectionName(NatsStreamPublisher.class.getCanonicalName())
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            JetStream js = nc.jetStream();

            PublishAck pa = js.publish(NatsMessage.builder()
                .subject("REQUESTS.priority")
                    .headers(new Headers()
                        .add("Content-Type", "application/json")
                        .add("Sequence-Id", System.currentTimeMillis()+"")
                    )
                .data("Hello World " + Instant.now())
                .build());

            System.out.printf("Published message: " + pa);
        }
    }

}
package com.fizzed.nats.demo;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class NatsStreamPublisher {

    static public void main(String[] args) throws Exception{
        Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            jsm.addStream(StreamConfiguration.builder()
                .name("request-stream")
                .storageType(StorageType.File)
                .subjects("request.*")
                .build());

            PublishAck pa = js.publish(NatsMessage.builder()
                .subject("request.priority")
                .data("Hello World " + Instant.now())
                .build());

            System.out.printf("Published message on stream %s, seqno %d.\n",
                pa.getStream(), pa.getSeqno());
        }
    }

}
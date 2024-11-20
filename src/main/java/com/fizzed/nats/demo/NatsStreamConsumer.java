package com.fizzed.nats.demo;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;

import java.time.Duration;
import java.time.Instant;

public class NatsStreamConsumer {

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

            //JetStreamSubscription jss = js.subscribe("request.priority", PullSubscribeOptions.builder()
                //.configuration(ConsumerConfiguration.builder()
                    //.maxBatch(1)
                //    .build())
            //    .build());

            JetStreamSubscription jss = js.subscribe("request.priority");

            Message message = jss.nextMessage(Duration.ofSeconds(10));

            System.out.printf("Consumed message on subject %s, data %s.\n",
                message.getSubject(), new String(message.getData()));

            message.ack();
            Thread.sleep(5000);
        }
    }

}
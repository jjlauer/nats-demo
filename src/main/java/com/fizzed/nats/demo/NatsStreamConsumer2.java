package com.fizzed.nats.demo;

import io.nats.client.*;

import java.time.Duration;

public class NatsStreamConsumer2 {

    static public void main(String[] args) throws Exception{
        final Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            final JetStream js = nc.jetStream();

            final JetStreamSubscription jss = js.subscribe("REQUESTS.priority", PullSubscribeOptions.builder()
                //.stream("REQUESTS")
                //.name("REQUESTS-consumer")
                .durable("fuck-you")
                //.bind(true)
                .build());

            while (true) {
                System.out.println("Waiting for next message on consumer: " + jss.getConsumerName());

                Message message = jss.nextMessage(Duration.ofSeconds(10));

                if (message != null) {
                    System.out.printf("Consumed message on subject %s, data %s.\n",
                        message.getSubject(), new String(message.getData()));
                    message.ack();
                } else {
                    System.out.println("No message received");
                }
            }
        }
    }

}
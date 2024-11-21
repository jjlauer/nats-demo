package com.fizzed.nats.demo;

import io.nats.client.*;

import java.time.Duration;
import java.util.List;

abstract public class NatsStreamConsumer {

    public void execute() throws Exception {
        final Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .connectionName(this.getClass().getCanonicalName())
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            final JetStream js = nc.jetStream();

            final JetStreamSubscription jss = js.subscribe("REQUESTS.priority", PullSubscribeOptions.builder()
                //.stream("REQUESTS")
                //.name("REQUESTS-consumer")
                // important: this allows multiple subscribers to overlapping subjects
                .durable("REQUESTS-priority-processor")
                //.bind(true)
                .build());

            //jss.pull(1);

            while (true) {
                System.out.println("Waiting for next message...");

                //Message message = jss.nextMessage(Duration.ofSeconds(100000));

                final List<Message> messages = jss.fetch(1, Duration.ofSeconds(10000));
                final Message message = messages != null && !messages.isEmpty() ? messages.get(0) : null;

                if (message != null) {
                    System.out.printf("Consumed message: sid=%s, headers=%s, subject=%s, data=%s\n",
                        message.getSID(),
                        message.getHeaders(), message.getSubject(), new String(message.getData()));

                    // if you don't ack the message, problems arise and it'll eventually be redelivered and retried
                    message.ack();
                } else {
                    System.out.println("No message received");
                }
            }
        }
    }

}
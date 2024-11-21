package com.fizzed.nats.demo;

import io.nats.client.*;
import io.nats.client.impl.NatsJetStreamMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

abstract public class NatsStreamConsumer {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    public void execute() throws Exception {
        final Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .verbose()
            .connectionName(this.getClass().getCanonicalName())
            .build();

        try (Connection nc = Nats.connect(o)) {
            log.info("Connected to nats server!");

            final JetStream js = nc.jetStream();

            final JetStreamSubscription jss = js.subscribe("REQUESTS.priority", PullSubscribeOptions.builder()
                //.stream("REQUESTS")
                //.name("REQUESTS-consumer")
                // important: this allows multiple subscribers to overlapping subjects
                .durable("REQUESTS-priority-processor")
                //.bind(true)
                .build());

            //jss.pull(1);

            final Thread subscriberThread = new Thread(() -> {
               while (true) {
                   try {
                       log.info("Waiting for next message...");


                       final List<Message> messages = jss.fetch(1, Duration.ofSeconds(1000));
                       final Message message = messages != null && !messages.isEmpty() ? messages.get(0) : null;

                       if (message != null) {
                           NatsJetStreamMetaData md = message.metaData();
                           final long seqNo = md.streamSequence();
                           final ZonedDateTime ts = md.timestamp();

                           log.info("Consumed message: seqNo={}, ts={}, headers={}, subject={}, data={}",
                               seqNo, ts, message.getHeaders(), message.getSubject(), new String(message.getData()));

                           // if you don't ack the message, problems arise and it'll eventually be redelivered and retried
                           message.ack();
                       } else {
                           log.info("No message received");
                       }
                   } catch (Exception e) {
                       log.error("Error consuming message", e);
                       try {
                           Thread.sleep(5000L);
                       } catch (InterruptedException ex) {
                           log.error("Error sleeping", ex);
                       }
                   }
                }
            });

            nc.addConnectionListener(new ConnectionListener() {
                @Override
                public void connectionEvent(Connection conn, Events type) {
                    System.out.println("Connection event type=" + type);
                    if (type == Events.DISCONNECTED || type == Events.CLOSED) {
                        subscriberThread.interrupt();
                        /*try {
                            nc.close();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }*/
                    }
                }
            });

            subscriberThread.start();
            subscriberThread.join();

        }
    }

}
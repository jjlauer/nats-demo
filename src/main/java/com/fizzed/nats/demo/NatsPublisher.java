package com.fizzed.nats.demo;

import io.nats.client.*;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class NatsPublisher {

    static public void main(String[] args) throws Exception{
        Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .reportNoResponders()
            .pedantic()
            .errorListener(new ErrorListener() {
                @Override
                public void messageDiscarded(Connection conn, Message msg) {
                    System.out.println("Message discarded");
                }

                @Override
                public void errorOccurred(Connection conn, String error) {
                    System.out.println("Error occurred: " + error);
                }

                @Override
                public void exceptionOccurred(Connection conn, Exception exp) {
                    System.out.println("Exception occurred: " + exp);
                }
            })
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            var si = nc.getServerInfo();
            System.out.println("Server info: " + si);

            nc.addConnectionListener(new ConnectionListener() {
                @Override
                public void connectionEvent(Connection connection, Events events) {
                    System.out.println("Connection event: " + events);
                }
            });

            nc.publish(NatsMessage.builder()
                .subject("mysubject")
                .data("Hello World!")
                .build());

            System.out.println("Published message");

            nc.flush(Duration.ofSeconds(5));
        }
    }

}
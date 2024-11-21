package com.fizzed.nats.demo;

import io.nats.client.*;

import java.nio.charset.StandardCharsets;

public class NatConsumer {

    static public void main(String[] args) throws Exception{
        Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            var si = nc.getServerInfo();
            System.out.println("Server info: " + si);

//            Dispatcher dispatcher = nc.createDispatcher();
//            dispatcher.

            Subscription sub = nc.createDispatcher()
                .subscribe("mysubject", new MessageHandler() {
                    @Override
                    public void onMessage(Message message) throws InterruptedException {
                        System.out.println("Received subject=" + message.getSubject() + ", message=" + new String(message.getData(), StandardCharsets.UTF_8));
                    }
                });

            Thread.sleep(100000000L);
        }
    }

}
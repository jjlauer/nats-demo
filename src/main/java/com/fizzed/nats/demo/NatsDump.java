package com.fizzed.nats.demo;

import io.nats.client.*;

import java.util.List;

public class NatsDump {

    static public void main(String[] args) throws Exception{
        Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            JetStreamManagement jsm = nc.jetStreamManagement();

            List<String> streamNames = jsm.getStreamNames();
            System.out.println("Found " + streamNames.size() + " streams:");
            streamNames.forEach(System.out::println);
        }
    }

}
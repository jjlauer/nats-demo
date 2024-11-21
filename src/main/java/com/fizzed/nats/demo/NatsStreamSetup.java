package com.fizzed.nats.demo;

import io.nats.client.*;
import io.nats.client.api.*;

public class NatsStreamSetup {

    static public void main(String[] args) throws Exception{
        Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            JetStreamManagement jsm = nc.jetStreamManagement();

            // GET OR CREATE STREAM
            final String streamName1 = "REQUESTS";

            StreamInfo si1 = null;
            try {
                si1 = jsm.getStreamInfo(streamName1);
            } catch (JetStreamApiException e) {
                if (!e.getMessage().contains("not found")) {
                    throw e;
                }
            }

            if (si1 != null) {
                jsm.deleteStream(streamName1);
            }

            si1 = jsm.addStream(StreamConfiguration.builder()
                .name(streamName1)
                .storageType(StorageType.File)
                .subjects("REQUESTS.priority")
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.Old)
                .build());

            System.out.println("Created jet stream: " + si1);

            /*jsm.createConsumer(streamName1, ConsumerConfiguration.builder()
                .name(streamName1 + "-consumer")
                .ackPolicy(AckPolicy.Explicit)
                .build());*/
        }
    }

}
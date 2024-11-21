package com.fizzed.nats.demo;

import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.impl.NatsMessage;

import java.time.Instant;

public class NatsKeyValuePublisher {

    static public void main(String[] args) throws Exception{
        Options o = new Options.Builder()
            .server("nats://localhost:14222")
            .maxReconnects(-1)
            .connectionName(NatsKeyValuePublisher.class.getCanonicalName())
            .build();

        try (Connection nc = Nats.connect(o)) {
            System.out.println("Connected to nats server!");

            KeyValueManagement kvm = nc.keyValueManagement();
            kvm.create(KeyValueConfiguration.builder()
                .name("TESTKV")
                .storageType(StorageType.File)
                .build());

            KeyValue kv = nc.keyValue("TESTKV");

            /*kv.put("a", 1);

            System.out.printf("Put key/value");*/

            KeyValueEntry entry = kv.get("a");
            System.out.println(entry);
        }
    }

}
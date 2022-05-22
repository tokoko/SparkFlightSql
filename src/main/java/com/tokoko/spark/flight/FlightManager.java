package com.tokoko.spark.flight;

import com.google.protobuf.ByteString;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class FlightManager {

    private final HashMap<ByteString, String> flights;

    private final BufferAllocator allocator;

    private final List<FlightClient> clients;

    public FlightManager(List<Location> locations, BufferAllocator allocator) {
        this.allocator = allocator;
        flights = new HashMap<>();

        clients = locations.stream()
                .map(loc -> Location.forGrpcInsecure(loc.getUri().getHost(), loc.getUri().getPort()))
                .map(location -> FlightClient.builder(allocator, location).build())
                .collect(Collectors.toList());
    }

    public void addFlight(ByteString handle) {
        if (flights.containsKey(handle)) {
            throw CallStatus.ALREADY_EXISTS.toRuntimeException();
        }
        flights.put(handle, "RUNNING");
    }

    public void setCompleted(ByteString handle) {
        if (!flights.containsKey(handle)) {
            throw new RuntimeException("Flight doesn't exist");
        }
        flights.put(handle, "COMPLETED");
    }

    public String getStatus(ByteString handle) {
        if (!flights.containsKey(handle)) {
            throw new RuntimeException("Flight doesn't exist");
        }
        return flights.get(handle);
    }

    public void broadcast(ByteString handle) {
        if (!flights.containsKey(handle)) {
            throw new RuntimeException("Flight doesn't exist");
        }

        Action action = new Action(flights.get(handle), handle.toByteArray());

        clients.forEach(client -> {
                Iterator<Result> it = client.doAction(action);

                while (it.hasNext()) {
                    System.out.println(it.next().getBody().length);
                }
        } );
    }

}

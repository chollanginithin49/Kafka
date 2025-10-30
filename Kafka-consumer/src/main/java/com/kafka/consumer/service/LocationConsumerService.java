package com.kafka.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.model.Location;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class LocationConsumerService {

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "${kafka.topic.driver-location}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            String key = record.key();
            String value = record.value();

            Location location = objectMapper.readValue(value, Location.class);


            System.out.println("📡 Location update received for driver " + location.getDriverId());
            System.out.println("Name of the driver "+ location.getName());


            notifyRider(location);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void notifyRider(Location location) {
        System.out.println("📨 Rider notified: Driver " + location.getDriverId() + " location updated.");
    }
}

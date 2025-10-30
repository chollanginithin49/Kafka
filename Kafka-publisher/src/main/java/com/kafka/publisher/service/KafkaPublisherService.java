package com.kafka.publisher.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.publisher.model.Location;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaPublisherService {
    @Value("${kafka.topic.driver-location}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper mapper = new ObjectMapper();

    public void publishLocation(Location location) {
        try {
            String key = location.getDriverId();
            String value = mapper.writeValueAsString(location);
            kafkaTemplate.send(topic, key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

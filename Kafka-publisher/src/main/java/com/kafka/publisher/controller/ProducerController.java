package com.kafka.publisher.controller;

import com.kafka.publisher.model.Location;
import com.kafka.publisher.service.KafkaPublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    @Autowired
    private KafkaPublisherService locationPublisherService;

    @PostMapping("/updateLocation")
    public String updateLocation(@RequestBody Location location) {
        locationPublisherService.publishLocation(location);
        return "Location update published successfully.";
    }
}

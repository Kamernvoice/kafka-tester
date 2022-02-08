package ru.vironit.kafkatester.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.vironit.kafkatester.dto.StarshipDto;

import java.time.LocalTime;

@Service
@Slf4j
public class StarshipServiceConsumer {

    @KafkaListener(topics = "1", containerFactory = "singleFactory", groupId = "topic")
    public void consumeFirstTopicOne(String message) {
        log.info(String.format("=> consumed last first {topic:1, message:%s}", message));
    }

    @KafkaListener(topics = "1", containerFactory = "singleFactory", groupId = "topic")
    public void consumeSecondTopicOne(String message) {
        log.info(String.format("=> consumed last second {topic:1, message:%s}", message));
    }

    @KafkaListener(topics = "2", containerFactory = "singleFactory", groupId = "first")
    public void consumeFirstTopicTwo(String message) {
        log.info(String.format("=> consumed first {topic:2, message:%s}", message));
    }

    @KafkaListener(topics = "2", containerFactory = "singleFactory", groupId = "second")
    public void consumeSecondTopicTwo(String message) {
        log.info(String.format("=> consumed second {topic:2, message:%s}", message));
    }

    @KafkaListener(topics = "3", containerFactory = "batchFactory", groupId = "batch")
    public void consumeTopicThree(String message) {
        log.info(String.format("=> consumed {topic:3, message:%s}", message));
    }
}

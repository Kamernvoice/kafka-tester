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
public class StarshipServiceImpl implements StarshipService {

    private final KafkaTemplate<Long, StarshipDto> kafkaStarshipTemplate;
    private final ObjectMapper objectMapper;
    private StarshipDto dto;

    @Autowired
    public StarshipServiceImpl(KafkaTemplate<Long, StarshipDto> kafkaStarshipTemplate, ObjectMapper objectMapper) {
        this.kafkaStarshipTemplate = kafkaStarshipTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(initialDelay = 10000, fixedDelay = 5000)
    @Override
    public void produce() {
        StarshipDto dto = createDto();
        log.info("<= sending {}", writeValueAsString(dto));
        kafkaStarshipTemplate.send("server.starship", dto);
    }

    private StarshipDto createDto() {
        StarshipDto dto = new StarshipDto();
        dto.setName("Starship " + (LocalTime.now().toNanoOfDay() / 1000000));
        return dto;
    }

    private String writeValueAsString(StarshipDto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException("Writing value to JSON failed: " + dto.toString());
        }
    }

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

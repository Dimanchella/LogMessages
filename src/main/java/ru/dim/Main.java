package ru.dim;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import ru.dim.loadtester.LogMessagesLoadTester;
import ru.dim.provider.LogMessagesProvider;
import ru.dim.service.LogMessagesService;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Slf4j
public class Main {
    static private final String DEFAULT_INPUT_PATH = "inputData\\2025-07-20.log";
    static private final String DEFAULT_OUTPUT_PATH = "outputData";
    static private final long DEFAULT_BUNCH_SIZE = 200000;

    static private final long START_BUNCH_SIZE_LOAD = 10000;
    static private final long END_BUNCH_SIZE_LOAD = 500000;
    static private final long STEP_SIZE_LOAD = 10000;


    public static void main(String[] args) {
        try {
            standardLaunch();
        } catch (Exception e) {
            log.error("unexpected error: ", e);
        }
    }

    private static void standardLaunch() {
        LocalDateTime startDateTime = LocalDateTime.now();

        LogMessagesProvider logMessagesProvider = new LogMessagesProvider(true);
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        LogMessagesService logMessagesService = new LogMessagesService(
                logMessagesProvider,
                objectMapper
        );
        logMessagesService.processLogMessages(DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH, DEFAULT_BUNCH_SIZE);

        LocalDateTime endDateTime = LocalDateTime.now();

        log.info("Work time: {}", ChronoUnit.MILLIS.between(startDateTime, endDateTime));
    }

    private static void loadTestLaunch() {
        LogMessagesLoadTester logMessagesLoadTester = new LogMessagesLoadTester();
        logMessagesLoadTester.startTesting(
                DEFAULT_INPUT_PATH,
                DEFAULT_OUTPUT_PATH,
                START_BUNCH_SIZE_LOAD,
                END_BUNCH_SIZE_LOAD,
                STEP_SIZE_LOAD
        );
    }
}
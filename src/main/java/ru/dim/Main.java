package ru.dim;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import ru.dim.provider.LogMessagesProvider;
import ru.dim.service.LogMessagesService;

@Slf4j
public class Main {
    static private final String DEFAULT_INPUT_PATH = "inputData\\2025-07-11.log";
    static private final String DEFAULT_OUTPUT_PATH = "outputData";

    public static void main(String[] args) {
        try {
            LogMessagesProvider logMessagesProvider = new LogMessagesProvider();
            ObjectMapper objectMapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            LogMessagesService logMessagesService = new LogMessagesService(
                    logMessagesProvider,
                    objectMapper
            );

            logMessagesService.processLogMessages(DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH);
        } catch (Exception e) {
            log.error("unexpected error: ", e);
        }
    }
}
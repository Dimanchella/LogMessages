package ru.dim.loadtester;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.dim.provider.LogMessagesProvider;
import ru.dim.service.LogMessagesService;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Slf4j
@NoArgsConstructor
public class LogMessagesLoadTester {
    private static final String STATISTIC_FILE_NAME = "statistic.txt";

    public void startTesting(
            String inputLogFilePath,
            String outputLogfilePath,
            long startBunchSize,
            long endBunchSize,
            long sizeStep
    ) {
        LogMessagesProvider logMessagesProvider = new LogMessagesProvider(true);
        for (long size = startBunchSize; size <= endBunchSize; size += sizeStep) {
            logMessagesProvider.saveTextInFile(
                    STATISTIC_FILE_NAME,
                    String.format(
                            "Bunch size = %d : %d ms",
                            size,
                            testProcess(inputLogFilePath, outputLogfilePath, size)
                    )
            );
        }
    }

    private long testProcess(String inputLogFilePath, String outputLogfilePath, long bunchSize) {
        LocalDateTime startDateTime = LocalDateTime.now();

        LogMessagesProvider logMessagesProvider = new LogMessagesProvider(false);
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        LogMessagesService logMessagesService = new LogMessagesService(
                logMessagesProvider,
                objectMapper
        );
        logMessagesService.processLogMessages(inputLogFilePath, outputLogfilePath, bunchSize);

        LocalDateTime endDateTime = LocalDateTime.now();

        log.info("Work time: {}", ChronoUnit.MILLIS.between(startDateTime, endDateTime));
        return ChronoUnit.MILLIS.between(startDateTime, endDateTime);
    }
}

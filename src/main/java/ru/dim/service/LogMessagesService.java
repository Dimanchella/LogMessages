package ru.dim.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.dim.dto.DataBunch;
import ru.dim.dto.LogMessage;
import ru.dim.provider.LogMessagesProvider;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@RequiredArgsConstructor
public class LogMessagesService {
    private static final String STRING_PATTERN = "([\\w\\-]{30,}|-?[0-9]+(\\.[0-9]+)?)";
    private static final String REPORT_FILE_NAME = "report.txt";

    private final LogMessagesProvider logMessagesProvider;
    private final ObjectMapper objectMapper;

    public void processLogMessages(String inputFilePath, String outputDirectoryPath) {
        Map<String, Map<String, Integer>> report = new ConcurrentHashMap<>();
        logMessagesProvider.executeTaskOnLogFile(
                inputFilePath,
                stream -> stream
                        .parallel()
                        .forEach(subStream -> processLogStrings(subStream, report, outputDirectoryPath))
        );
        saveReport(report, outputDirectoryPath);
    }

    private void processLogStrings(
            DataBunch data,
            Map<String, Map<String, Integer>> report,
            String outputDirectoryPath
    ) {
        log.info("Start processing bunch № {}.", data.getBunchNumber());
        data
                .getDataLines()
                .map(this::deserializeLogMessage)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(logMessage -> {
                    countLogMessageToReport(logMessage, report);
                    saveLog(logMessage, outputDirectoryPath);
                });
        log.info("End bunch № {}.", data.getBunchNumber());
    }

    private Optional<LogMessage> deserializeLogMessage(String logString) {
        try {
            return Optional.ofNullable(objectMapper.readValue(logString, LogMessage.class));
        } catch (JsonProcessingException e) {
            log.error("Json input parsing error: ", e);
            return Optional.empty();
        }
    }

    private void countLogMessageToReport(LogMessage logMessage, Map<String, Map<String, Integer>> report) {
        String clearMessageText = logMessage.getMessage().replaceAll(STRING_PATTERN, "");
        Map<String, Integer> newClearMessageMap = new HashMap<>();
        newClearMessageMap.put(clearMessageText, 1);

        report.merge(
                logMessage.getApp(),
                newClearMessageMap,
                (clearMessages, newClearMessage) -> {
                    newClearMessage
                            .keySet()
                            .forEach(msg -> clearMessages.merge(msg, newClearMessage.get(msg), Integer::sum));
                    return clearMessages;
                }
        );
        // log.info("<COUNT LOG> App: {}\t| Log: {}", logMessage.getApp(), logMessage.getMessage());
    }

    private void saveLog(LogMessage logMessage, String outputDirectoryPath) {
        try {
            logMessagesProvider.saveFile(
                    logMessagesProvider.getSaveLogFilePath(
                            outputDirectoryPath,
                            logMessage.getApp(),
                            logMessage.getMessage().replaceAll(STRING_PATTERN, "")
                    ),
                    objectMapper.writeValueAsString(logMessage)
            );
            // log.info("<SAVE LOG> App: {}\t| Clear log: {}", logMessage.getApp(), logMessage.getMessage());
        } catch (JsonProcessingException e) {
            log.error("Json output parsing error: ", e);
        }
    }

    private void saveReport(Map<String, Map<String, Integer>> report, String outputDirectoryPath) {
        StringBuilder reportText = new StringBuilder();
        for (String app : report.keySet()) {
            reportText.append(app).append("\n");
            for (String message : report.get(app).keySet()) {
                reportText.append(message).append(" - ").append(report.get(app).get(message)).append("\n");
            }
            reportText.append("\n");
        }
        logMessagesProvider.saveFile(
                outputDirectoryPath + File.separator + REPORT_FILE_NAME,
                reportText.toString()
        );
        log.info("<SAVE REPORT>");
    }
}

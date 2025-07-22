package ru.dim.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.dim.dto.DataBunch;
import ru.dim.dto.LogMessage;
import ru.dim.provider.LogMessagesProvider;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    public void processLogMessages(String inputFilePath, String outputDirectoryPath, long bunchSize) {
        Map<String, Map<String, Integer>> report = new ConcurrentHashMap<>();
        logMessagesProvider.executeTaskOnLogFile(
                inputFilePath,
                stream -> stream
                        .parallel()
                        .forEach(subStream -> processLogStrings(
                                subStream,
                                report,
                                outputDirectoryPath)
                        ),
                bunchSize
        );
        saveReport(report, outputDirectoryPath);
    }

    private void processLogStrings(
            DataBunch data,
            Map<String, Map<String, Integer>> report,
            String outputDirectoryPath
    ) {
        log.info("Start processing bunch № {}.", data.getBunchNumber());
        Map<String, Map<String, List<String>>> localReportList = new HashMap<>();

        data
                .getDataLines()
                .map(this::deserializeLogMessage)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(logMessage -> {
                    countLogMessageToReport(logMessage, report);
                    addLogMessageToReportList(logMessage, localReportList);
                });
        log.info("Save bunch № {}.", data.getBunchNumber());
        saveReportList(localReportList, outputDirectoryPath);
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
                            .forEach(message -> clearMessages.merge(
                                    message,
                                    newClearMessage.get(message),
                                    Integer::sum
                            ));
                    return clearMessages;
                }
        );
        // log.info("<COUNT LOG> App: {}\t| Log: {}", logMessage.getApp(), logMessage.getMessage());
    }

    private void addLogMessageToReportList(LogMessage logMessage, Map<String, Map<String, List<String>>> reportList) {
        List<String> newLogList = new ArrayList<>();
        try {
            newLogList.add(objectMapper.writeValueAsString(logMessage));
        } catch (JsonProcessingException e) {
            log.error("Json output parsing error: ", e);
        }

        String clearMessageText = logMessage.getMessage().replaceAll(STRING_PATTERN, "");
        Map<String, List<String>> newClearMessageMap = new HashMap<>();
        newClearMessageMap.put(clearMessageText, newLogList);

        reportList.merge(
                logMessage.getApp(),
                newClearMessageMap,
                (clearMessages, newClearMessage) -> {
                    newClearMessage
                            .keySet()
                            .forEach(message -> clearMessages.merge(
                                    message,
                                    newClearMessage.get(message),
                                    (logMessages, newLogMessage) -> {
                                        logMessages.addAll(newLogMessage);
                                        return logMessages;
                                    }
                            ));
                    return clearMessages;
                }
        );
    }

    private void saveReportList(Map<String, Map<String, List<String>>> reportList, String outputDirectoryPath) {
        for (String app : reportList.keySet()) {
            for (String clearMessage : reportList.get(app).keySet()) {
                logMessagesProvider.saveTextListInFile(
                        logMessagesProvider.getSaveLogFilePath(outputDirectoryPath, app, clearMessage),
                        reportList.get(app).get(clearMessage)
                );
            }
        }
    }

    private void saveReport(Map<String, Map<String, Integer>> report, String outputDirectoryPath) {
        log.info("Save report");
        StringBuilder reportText = new StringBuilder();
        for (String app : report.keySet()) {
            reportText.append(app).append("\n");
            for (String message : report.get(app).keySet()) {
                reportText.append(message).append(" - ").append(report.get(app).get(message)).append("\n");
            }
            reportText.append("\n");
        }
        logMessagesProvider.saveTextInFile(
                outputDirectoryPath + File.separator + REPORT_FILE_NAME,
                reportText.toString()
        );
    }
}

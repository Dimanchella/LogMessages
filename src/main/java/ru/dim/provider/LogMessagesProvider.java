package ru.dim.provider;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.dim.dto.DataBunch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class LogMessagesProvider {
    static private final int MAX_LOG_FILE_NAME_LENGTH = 64;
    static private final String END_SUBSTRING = "...";
    static private final String UNRESOLVED_CHARACTERS_PATTERN = "[\\\\/:*?\"<>|]";
    static private final String LOG_FILE_TYPE = ".json";

    private final boolean appendFiles;

    private long bunchNumber = 0;


    public void executeTaskOnLogFile(String inputFilePath, Consumer<Stream<DataBunch>> executor, long bunchSize) {
        try (BufferedReader bufferedReader = new BufferedReader(
                new FileReader(inputFilePath, StandardCharsets.UTF_8)
        )) {
            executor.accept(
                    Stream
                            .generate(() -> getDataBunch(++bunchNumber, bufferedReader, bunchSize))
                            .takeWhile(Optional::isPresent)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
            );
        } catch (IOException e) {
            log.error("Reading file error: ", e);
        }
    }

    public Optional<DataBunch> getDataBunch(long bunchNumber, BufferedReader bufferedReader, long bunchSize) {
        try {
            List<String> bunch = new ArrayList<>();
            String line = bufferedReader.readLine();
            if (line == null) {
                log.info("End of file!");
                return Optional.empty();
            }

            log.info("Read new bunch № {}.", bunchNumber);
            //for (int i = 0; i < BUNCH_SIZE && line != null; i++) {
            for (int i = 0; i < bunchSize && line != null; i++) {
                if (!line.isBlank()) {
                    bunch.add(line);
                }
                line = bufferedReader.readLine();
            }
            return Optional.of(new DataBunch(bunchNumber, bunch.stream()));
        } catch (IOException e) {
            log.error("Getting data bunch from file error: ", e);
            return Optional.empty();
        }
    }


    public String getSaveLogFilePath(String outputDirectoryPath, String appName, String clearLogMessage) {
        StringBuilder fileName = new StringBuilder(outputDirectoryPath);
        fileName.append(File.separator).append(appName).append(File.separator);

        String newLogName = clearLogMessage.replaceAll(UNRESOLVED_CHARACTERS_PATTERN, "");
        if (newLogName.length() > MAX_LOG_FILE_NAME_LENGTH) {
            newLogName = newLogName.substring(0, MAX_LOG_FILE_NAME_LENGTH - 1 - END_SUBSTRING.length()) + END_SUBSTRING;
        }

        fileName.append(newLogName).append(LOG_FILE_TYPE);
        return fileName.toString();
    }

    public void saveTextInFile(String filePath, String text) {
        File file = new File(filePath);
        File path = file.getParentFile();
        if (path == null || path.exists() || makeDirs(path)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, appendFiles))) {
                bufferedWriter.write(text);
                bufferedWriter.newLine();
            } catch (IOException e) {
                log.error("Saving file error: ", e);
            }
        } else {
            log.error("The creation of the directory \"{}\" failed.", path);
        }
    }

    public void saveTextListInFile(String filePath, List<String> textList) {
        File file = new File(filePath);
        File path = file.getParentFile();
        if (path == null || path.exists() || makeDirs(path)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, appendFiles))) {
                for (String log : textList) {
                    bufferedWriter.write(log);
                    bufferedWriter.newLine();
                }
            } catch (IOException e) {
                log.error("Saving file error: ", e);
            }
        } else {
            log.error("The creation of the directory \"{}\" failed.", path);
        }
    }

    /* Повторение проверки на существование директории нужно для избежания повторного вызова метода mkdirs,
     * в случае, если другой поток уже вызвал метод makeDirs и создал директорию.
     * Первая проверка на существование директории нужна для исключения вызова синхронного метода makeDirs
     * и остановки других потоков.
     */
    public synchronized boolean makeDirs(File path) {
        boolean result = path.mkdirs();
        if (result) {
            log.info("Directory {} is created.", path);
        }
        return path.exists() || result;
    }
}

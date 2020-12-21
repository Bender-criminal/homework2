package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();
    private final Exchanger<List<Pair<String, Integer>>> exchanger = new Exchanger<>();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        Thread writerThread = startWriterThread(new File(resultFileName));

        ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {

           List<Future<Pair<String, Integer>>> futures = new ArrayList<>();
           boolean eof = false;

            while (scanner.hasNext()) {
                String line = scanner.nextLine();

                if (!bufferIsFull(futures.size()) && !eof) {
                    futures.add(executorService.submit(() -> new LineCounterProcessor().process(line)));
                    eof = !scanner.hasNextLine();
                }
                if (bufferIsFull(futures.size()) || eof) {

                    exchanger.exchange(getResults(futures));
                    futures.clear();
                }
            }



        } catch (IOException | InterruptedException exception) {
            logger.error("", exception);
        }

        writerThread.interrupt();
        executorService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private Thread startWriterThread(File resultFile) {
         Thread thread = new Thread(new FileWriter(resultFile, exchanger));
         thread.start();
         return thread;
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }

    private boolean bufferIsFull(int count){
        return count >= CHUNK_SIZE;
    }

    private List<Pair<String, Integer>> getResults(List<Future<Pair<String, Integer>>> futures){
        return futures.stream().map(pairFuture -> {
            try {
                return pairFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Не удалось получить результаты вычислений", e);
            }
            return null;
        }).collect(Collectors.toList());
    }
}

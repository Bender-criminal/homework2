package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        // TODO: NotImplemented: запускаем FileWriter в отдельном потоке
        Thread writerThread = startWriterThread();

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

                    // TODO: NotImplemented: добавить обработанные данные в результирующий файл
                    for (Future<Pair<String, Integer>> future : futures) {
                        Pair<String, Integer> pair = future.get();
                        System.out.println(pair.getKey() + ' ' + pair.getValue());
                    }
                    futures.clear();
                }
            }



        } catch (IOException exception) {
            logger.error("", exception);
        } catch (InterruptedException e) {
            logger.error("", e);
        } catch (ExecutionException e) {
            logger.error("", e);

        }

        writerThread.interrupt();
        executorService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private Thread startWriterThread() {
         Thread thread = new Thread(new FileWriter());
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
}

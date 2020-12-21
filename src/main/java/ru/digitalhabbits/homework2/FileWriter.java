package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private File outFile;
    private final Exchanger<List<Pair<String, Integer>>> exchanger;

    public FileWriter(File outFile, Exchanger<List<Pair<String, Integer>>> exchanger) {
        this.outFile = outFile;
        this.exchanger = exchanger;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

       try(java.io.FileWriter fileWriter = new java.io.FileWriter(outFile, true)){
           while (!Thread.currentThread().isInterrupted()) {

               List<Pair<String, Integer>> pairs = exchanger.exchange(null);
               if (pairs.size() < 1) continue;

               for (Pair<String, Integer> pair : pairs) {
                   fileWriter.write(pair.getKey() + ' ' + pair.getValue() + "\n");
               }
           }
       } catch (IOException e) {
           logger.error("Не удалось открыть выходной файл", e);
       } catch (InterruptedException e) {
           logger.error("Поток записи завершен", e);
       }

        logger.info("Finish writer thread {}", currentThread().getName());
    }
}

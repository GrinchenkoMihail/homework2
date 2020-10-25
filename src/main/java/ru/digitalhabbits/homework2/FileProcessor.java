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

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {

        checkFileExists(processingFileName);
        final File file = new File(processingFileName);

        // TODO: NotImplemented: запускаем FileWriter в отдельном потоке
        Exchanger<List<Pair<String, Integer>>> exchanger = new Exchanger<>();
        ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

        Thread writerThread = new Thread(new FileWriter(exchanger, resultFileName));
        writerThread.start();

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                // TODO: NotImplemented: вычитываем CHUNK_SIZE строк для параллельной обработки
                List<String> lineList = prepareOfLinesForProcessing(scanner);

                // TODO: NotImplemented: обрабатывать строку с помощью LineProcessor. Каждый поток обрабатывает свою строку.
                List<Pair<String, Integer>> resultList = processLines(lineList, executorService);

                // TODO: NotImplemented: добавить обработанные данные в результирующий файл
                exchanger.exchange(resultList);
            }
        } catch (IOException exception) {
            logger.error("", exception);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        // TODO: NotImplemented: остановить поток writerThread
        writerThread.interrupt();
        executorService.shutdown();
        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
    private List<String> prepareOfLinesForProcessing(Scanner scanner){
        List<String> lineList = new ArrayList<>();
        while (lineList.size() < CHUNK_SIZE && scanner.hasNextLine()) {
            String line = scanner.nextLine();
            lineList.add(line);
        }
        return lineList;
    }

    private List<Pair<String, Integer>> processLines(List<String> lineList, ExecutorService executorService) throws InterruptedException, ExecutionException {
        List<Callable<Pair<String, Integer>>> listPairCal = new ArrayList<>();
        List<Pair<String, Integer>> resultList = new ArrayList<>();
        for (String line : lineList) {
            listPairCal.add(() -> new LineCounterProcessor().process(line));
        }
        List<Future<Pair<String, Integer>>> futureList = executorService.invokeAll(listPairCal);
        for (Future<Pair<String, Integer>> future : futureList) {
            resultList.add(future.get());
        }
        return resultList;
    }
}

package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private Exchanger<List<Pair<String, Integer>>> exchanger;
    private  String resultFileName;

    public FileWriter(Exchanger<List<Pair<String, Integer>>> exchanger, String resultFileName) {
        this.exchanger = exchanger;
        this.resultFileName = resultFileName;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        File file = new File(resultFileName);
        try(BufferedWriter outPutWriter = new BufferedWriter(new java.io.FileWriter(file))){
            List<Pair<String,Integer>> resultPairList = new ArrayList<>();
            while(!currentThread().isInterrupted()){
                try {
                    resultPairList = exchanger.exchange(resultPairList);
                }
                catch (InterruptedException ex){
                    break;
                }
                for(Pair<String,Integer> pair: resultPairList){
                    outPutWriter.write(String.format("%s %s",pair.getLeft(), pair.getRight().toString()));
                    outPutWriter.newLine();
                }
            }
        }
        catch (Exception ex){
            ex.getStackTrace();
        }
        logger.info("Finish writer thread {}", currentThread().getName());
    }
}

package org.apache.bigtop.load;

import com.github.rnowling.bps.datagenerator.datamodels.Transaction;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by jayunit100 on 2/28/15.
 */
public class FileLoadGen extends LoadGen{

    Path path;

    public FileLoadGen(int nStores, int nCustomers, double simulationLength, long seed, Path outputDir) throws Throwable {
        super(nStores, nCustomers, simulationLength, seed);
        path=outputDir;
    }

    public LinkedBlockingQueue<Transaction> startWriteQueue(final int milliseconds){
        if(! path.toFile().isDirectory()) {
            throw new RuntimeException("Input for the queue Should be a directory! Files will be transactions0.txt, transactions1.txt, and so on.");
        }

        /**
         * Write queue.   Every 5 seconds, write
         */
        final LinkedBlockingQueue<Transaction> transactionQueue = new LinkedBlockingQueue<Transaction>(1000000);
        new Thread(){
            @Override
            public void run() {
                int fileNumber=0;
                while(true){
                    waitFor(milliseconds, transactionQueue);
                    System.out.println("Clearing " + transactionQueue.size() + " elements");
                    Stack<Transaction> transactionsToWrite = new Stack<Transaction>();
                    transactionQueue.drainTo(transactionsToWrite);
                    StringBuffer lines = new StringBuffer();
                    while(!transactionsToWrite.isEmpty()){
                        lines.append(Utils.printable(transactionsToWrite.pop())+"\n");
                        total++;
                    }
                    try{
                        Path outputFile = Paths.get(path.toFile().getAbsolutePath(), "/transactions" + fileNumber++ + ".txt");
                        Files.write(outputFile, lines.toString().getBytes());
                        System.out.println("WRITE FILE to " + outputFile.toFile().length() + "bytes -> " + outputFile.toFile().getAbsolutePath());
                    }
                    catch(Throwable t){

                    }
                    System.out.println(
                            "TRANSACTIONS SO FAR " + total++ +
                                    " RATE " + total/((System.currentTimeMillis()-startTime)/1000));
                }
            }
        }.start();
        return transactionQueue;
    }

}

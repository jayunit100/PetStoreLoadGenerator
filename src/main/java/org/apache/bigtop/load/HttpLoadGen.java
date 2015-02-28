package org.apache.bigtop.load;

import com.github.rnowling.bps.datagenerator.datamodels.Transaction;
import org.apache.bigtop.load.LoadGen;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by jayunit100 on 2/28/15.
 */
public class HttpLoadGen extends LoadGen {

    URL path = null;
    public HttpLoadGen(int nStores, int nCustomers, double simulationLength, long seed, URL u) throws Throwable{
        super(nStores, nCustomers, simulationLength, seed);
        path=u;
    }

    /**
     * Appends via REST calls.
     */
    public LinkedBlockingQueue<Transaction> startWriteQueue(final int milliseconds) {
        /**
         * Write queue.   Every 5 seconds, write
         */
        final LinkedBlockingQueue<Transaction> transactionQueue = new LinkedBlockingQueue<Transaction>(1000000);
        new Thread() {
            @Override
            public void run() {
                int fileNumber = 0;
                while (true) {
                    waitFor(milliseconds, transactionQueue);
                    System.out.println("Clearing " + transactionQueue.size() + " elements");
                    Stack<Transaction> transactionsToWrite = new Stack<Transaction>();
                    transactionQueue.drainTo(transactionsToWrite);
                    while (!transactionsToWrite.isEmpty()) {
                        try {
                            Utils.get(path, Utils.printable(transactionsToWrite.pop()));
                            total++;
                        } catch (Throwable t) {
                            System.err.println("transaction failed.");
                        }
                        System.out.println(
                                "TRANSACTIONS SO FAR " + total++ +
                                        " RATE " + total / ((System.currentTimeMillis() - startTime) / 1000));
                    }
                }


            }
        }.start();

        return transactionQueue;
    }
}
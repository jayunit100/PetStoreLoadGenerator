package org.apache.bigtop.load;

import com.github.rnowling.bps.datagenerator.datamodels.Transaction;
import com.google.common.collect.Lists;
import org.apache.bigtop.load.LoadGen;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by jayunit100 on 2/28/15.
 */
public class HttpLoadGen extends LoadGen {

    String path = "http://localhost:3000";

    public HttpLoadGen(int nStores, int nCustomers, double simulationLength, long seed, URL u) throws Throwable{
        super(nStores, nCustomers, simulationLength, seed);
        //path hardcoded for now.
        path=u.toString();
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

                    transactionQueue.drainTo(transactionsToWrite); //write all to an in memory store.
                    /**
                     * This while loop does HTTP requests, and might take a while.
                     * The longer it takes, the longer time till the next "drain", which
                     * can ultimately result in blocking of the producer (in another thread).
                     */
                    while (!transactionsToWrite.isEmpty()) {
                        try {
                            Transaction tr =  transactionsToWrite.pop();

                            //For now, we dont have any params on the REST requests.
                            //params.add(new BasicNameValuePair("transaction", Utils.printable(transactionsToWrite.pop())));
                            String customer= URLEncoder.encode(Utils.json(tr));
                            //path should be something like : "localhost:3000/rpush/guestbook"
                            HttpResponse resp=Utils.get(path + "/" + customer);
                            if(total%10==0)
                                System.out.println("wrote customer " + customer);

                            total++;
                        }
                        catch (Throwable t) {
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
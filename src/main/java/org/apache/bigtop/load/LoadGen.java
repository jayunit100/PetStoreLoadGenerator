package org.apache.bigtop.load;

/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


import com.github.rnowling.bps.datagenerator.datamodels.inputs.InputData;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.ProductCategory;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.ZipcodeRecord;
import com.github.rnowling.bps.datagenerator.datamodels.*;
//import com.github.rnowling.bps.datagenerator.*{DataLoader,StoreGenerator,CustomerGenerator => CustGen, PurchasingProfileGenerator,TransactionGenerator}
import com.github.rnowling.bps.datagenerator.*;
import com.github.rnowling.bps.datagenerator.framework.SeedFactory;
import com.google.common.collect.Lists;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This driver uses the data generator API to generate
 * an arbitrarily large data set of petstore transactions.
 * <p/>
 * Each "transaction" consists of many "products", each of which
 * is stringified into what is often called a "line item".
 * <p/>
 * Then, spark writes those line items out as a distributed hadoop file glob.
 */
public class LoadGen {

    public static boolean TESTING=false;

    int nStores = 1000;
    int nCustomers = 1000;
    double simulationLength = -1;
    long seed = System.currentTimeMillis();
    String outputDir = "/shared";

    static final String[] DEFAULT = new String[]{"/tmp","1000","100000","1500","13241234"};
    static final String DEFAULT_EXPECTED = "10 to 25K transactions per second on i7 chip w/ SSD";
    public LoadGen(int nStores, int nCustomers, double simulationLength, long seed, String outputDir) {
        this.nStores = nStores;
        this.nCustomers = nCustomers;
        this.simulationLength = simulationLength;
        this.seed = seed;
        this.outputDir = outputDir;
    }

    public static void printUsage() {
        String usage =
                "BigPetStore Data Generator.\n" +
                        "Usage: outputDir nStores nCustomers simulationLength [seed]\n" +
                        "outputDir - (string) directory to write files\n" +
                        "nStores - (int) number of stores to generate\n" +
                        "nCustomers - (int) number of customers to generate\n" +
                        "simulationLength - (float) number of days to simulate\n" +
                        "seed - (long) seed for RNG. If not given, one is reandomly generated.\n";
        System.err.println(usage);
    }

    public static LoadGen parseArgs(String[] args) {
        if(args.length==0){
            System.out.println("Running default simulation, which should result in " + DEFAULT_EXPECTED);
            return parseArgs(DEFAULT);
        }
        int nStores = 1000;
        int nCustomers = 1000;
        double simulationLength = -1;
        long seed = System.currentTimeMillis();
        String outputDir = "/shared";

        int PARAMS=5;
        if (args.length != PARAMS && args.length != (PARAMS - 1)) {
            printUsage();
            System.exit(1);
        }
        outputDir = args[0];
        try {
            nStores = Integer.parseInt(args[1]);
        } catch (Throwable t) {
            System.err.println("Unable to parse '" + args[1] + "' as an integer for nStores.\n");
            printUsage();
            System.exit(1);
        }
        try {
            nCustomers = Integer.parseInt(args[2]);
        } catch (Throwable t) {
            System.err.println("Unable to parse '" + args[2] + "' as an integer for nCustomers.\n");
            printUsage();
            System.exit(1);
        }
        try {
            simulationLength = Double.parseDouble(args[3]);
        } catch (Throwable t) {
            System.err.println("Unable to parse '" + args[3] + "' as a float for simulationLength.\n");
            printUsage();
            System.exit(1);
        }

        //If seed isnt present, then no is used seed.
        if (args.length == PARAMS) {
            try {
                seed = Long.parseLong(args[4]);
            } catch (Throwable t) {
                System.err.println("Unable to parse '" + args[4] + "' as a long for seed.\n");
                printUsage();
                System.exit(1);
            }
        } else {
            seed = new Random().nextLong();
        }

        return new LoadGen(nStores,nCustomers,simulationLength,seed,outputDir);
    }

    public static String printable(Transaction t){
        return t.getStore().getId() + "," +
                        t.getStore().getLocation().getZipcode() + "," +
                        t.getStore().getLocation().getCity() + "," +
                        t.getStore().getLocation().getState() + "," +
                        t.getCustomer().getId() + "," +
                        t.getCustomer().getName().getFirst()+ "," +
                        t.getCustomer().getName().getSecond() + "," +
                        t.getCustomer().getLocation().getZipcode() + "," +
                        t.getCustomer().getLocation().getCity() + "," +
                        t.getCustomer().getLocation().getState() + "," +
                        t.getId() + "," +
                        t.getDateTime() +","+
                        join(t.getProducts(), ",");
        }

    //borrowed from apache.
    public static String join(Collection var0, Object var1) {
        StringBuffer var2 = new StringBuffer();

        for(Iterator var3 = var0.iterator(); var3.hasNext(); var2.append(var3.next())) {
            if(var2.length() != 0) {
                var2.append(var1);
            }
        }

        return var2.toString();
    }

    /**
     * Helper function.  Makes sure we sleep for a while
     * when queue is empty and also when we startup.
     */
    private static void waitFor(long milliseconds, LinkedBlockingQueue<Transaction> q){
        try{
            Thread.sleep(milliseconds);
        }
        catch(Throwable t){
        }
        //now, sleep for 2 seconds at a time until queue is full.
            while(q.size()<100) {
            try{
                Thread.sleep(2000L);
            }
            catch(Throwable t){

            }
        }
    }


    static final long startTime = System.currentTimeMillis();
    static double total = 0;
    /**
     * Appends to files.
     * @param path
     * @return
     */
    public static LinkedBlockingQueue<Transaction> startWriteQueue(final Path path, final int milliseconds){
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
                        lines.append(printable(transactionsToWrite.pop())+"\n");
                        total++;
                    }
                   try{
                       Path outputFile = Paths.get(path.toFile().getAbsolutePath(),"/transactions"+fileNumber++ +".txt");
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


    public static void main(String[] args){
        try {
            LoadGen lg = parseArgs(args);
            float count = 0.0f;
            long start=System.currentTimeMillis();

            //write everything to /tmp, every 20 seconds.
            LinkedBlockingQueue<Transaction> q = startWriteQueue(Paths.get(lg.outputDir),10000);
            while(true){
                lg.iterateData(q, System.currentTimeMillis());
                //if testing , dont run forever.  TODO, make runtime configurable.
                if(TESTING){
                    System.out.println("DONE...");
                    return;
                }
            }
        }
        catch(Throwable t){
            t.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Thread-friendly data iterator, writes to a blocking queue.
     */
    public void iterateData(LinkedBlockingQueue<Transaction> queue,long rseed) throws Throwable {
        long start = System.currentTimeMillis();
        final InputData inputData = new DataLoader().loadData();
        final SeedFactory seedFactory = new SeedFactory(rseed);

        System.out.println("Generating stores...");
        final ArrayList<Store> stores = new ArrayList<Store>();
        final StoreGenerator storeGenerator = new StoreGenerator(inputData, seedFactory);
        for (int i = 0; i < nStores; i++) {
            Store store = storeGenerator.generate();
            stores.add(store);
        }

        System.out.println("Generating customers...");

        final List<Customer> customers = Lists.newArrayList();
        final CustomerGenerator custGen = new CustomerGenerator(inputData, stores, seedFactory);
        for (int i = 0; i < nCustomers; i++) {
            Customer customer = custGen.generate();
            customers.add(customer);
        }

        System.out.println("...Generated " + customers.size());

        Long nextSeed = seedFactory.getNextSeed();

        long index = 0;

        index++;
        Collection<ProductCategory> products = inputData.getProductCategories();
        Iterator<Customer> custIter = customers.iterator();

        if(! custIter.hasNext())
            throw new RuntimeException("No customer data ");
        //Create a new purchasing profile.
        PurchasingProfileGenerator profileGen = new PurchasingProfileGenerator(products, seedFactory);
        PurchasingProfile profile = profileGen.generate();

        while(queue.remainingCapacity()>0 && custIter.hasNext()){
            Customer cust = custIter.next();
            int transactionsForThisCustomer = 0;
            TransactionGenerator transGen = new TransactionGenerator(cust, profile, stores, products, seedFactory);
            Transaction trC = transGen.generate();
            while(trC.getDateTime()<simulationLength) {
                queue.put(trC);
                index++;
                trC=transGen.generate();
            }
        }

    }
}
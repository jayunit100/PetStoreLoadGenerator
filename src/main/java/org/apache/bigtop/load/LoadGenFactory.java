package org.apache.bigtop.load;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Created by jayunit100 on 2/28/15.
 */
public class LoadGenFactory {

    static final String[] DEFAULT = new String[]{"/tmp","1000","100000","1500","13241234"};
    static final String DEFAULT_EXPECTED = "10 to 25K transactions per second on i7 chip w/ SSD";

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

    public static LoadGen parseArgs(String[] args) throws Throwable {
        if(args.length==0){
            System.out.println("Running default simulation, which should result in " + DEFAULT_EXPECTED);
            return parseArgs(DEFAULT);
        }
        int nStores = 1000;
        int nCustomers = 1000;
        double simulationLength = -1;
        long seed = System.currentTimeMillis();
        String outputARG = "/shared";

        int PARAMS=5;
        if (args.length != PARAMS && args.length != (PARAMS - 1)) {
            printUsage();
            System.exit(1);
        }
        //Was Dir, now ARG, since we can support http or file paths.
        outputARG = args[0];
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
        if(outputARG.startsWith("http://")){
            return new HttpLoadGen(nStores,nCustomers,simulationLength,seed, new URL(outputARG));
        }
        else{
            return new FileLoadGen(nStores,nCustomers,simulationLength,seed, Paths.get(outputARG));
        }
    }
}

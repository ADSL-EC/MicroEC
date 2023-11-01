package org.apache.crail.tools;

import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiClientBenchmark {

    public void callShellByExec(String shellString) {
        BufferedReader reader = null;
        try {
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                System.out.println("call shell failed. error code is :" + exitValue);
            }
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                System.out.println("mac@wxw %  " + line);
            }
        } catch (Throwable e) {
            System.out.println("call shell failed. " + e);
        }
    }

    void getClientsLatency(List<MultiClientReplayer> clients) {
        int clientNum = clients.size();
        int times = clients.get(0).loopLatency.length;

        for (int i = 0; i < times; i++) {
            StringBuilder latencyRecord = new StringBuilder();
            latencyRecord.append(i);
            for (int j = 0; j < clientNum; j++) {
                latencyRecord.append("," + (clients.get(j).loopLatency[i] / 1000.0));
            }
            System.out.println(latencyRecord.toString());
        }

        // get avgLatency
        double allAvg = 0.0, sumThgpt = 0.0;
        StringBuilder avgLatencyRecord = new StringBuilder();
        avgLatencyRecord.append("avgLatency(us)");
        for (int i = 0; i < clientNum; i++) {
            allAvg += clients.get(i).getAvgLatency();
            avgLatencyRecord.append("," + clients.get(i).getAvgLatency());
        }
        System.out.println(avgLatencyRecord.toString());
        allAvg /= clientNum;

        // get thgpt
        StringBuilder thgptRecord = new StringBuilder();
        thgptRecord.append("thgpt(MB/s)");
        for (int i = 0; i < clientNum; i++) {
            sumThgpt += clients.get(i).getThoughput();
            thgptRecord.append("," + clients.get(i).getThoughput());
        }
        System.out.println(thgptRecord.toString());

        System.out.println("allAvgLatency(us),"+allAvg);
        System.out.println("sumThgpt(MB/s),"+sumThgpt);
    }

    public static void main(String[] args) throws Exception {
        String scheme = "replicas";
        String operation = "write";
        String prefix = "node";
        int size = 1024 * 1024;
        int times = 1000;
        int clientNum = 2;

        String schemeTypes = "replicas|nativeec|microec|hydra";
        String operationTypes = "write|degraderead";
        Option schemeOption = Option.builder("s").desc("type of scheme [" + schemeTypes + "]").hasArg().build();
        Option operationOption = Option.builder("o").desc("type of operation [" + operationTypes + "]").hasArg().build();
        Option prefixOption = Option.builder("p").desc("prefix of each computer node").hasArg().build();
        Option sizeOption = Option.builder("z").desc("size of each operation").hasArg().build();
        Option timesOption = Option.builder("k").desc("times of operation").hasArg().build();
        Option clientNumOption = Option.builder("c").desc("number of client on one computer node").hasArg().build();

        Options options = new Options();
        options.addOption(schemeOption);
        options.addOption(operationOption);
        options.addOption(prefixOption);
        options.addOption(sizeOption);
        options.addOption(timesOption);
        options.addOption(clientNumOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
        if (line.hasOption(schemeOption.getOpt())) {
            scheme = line.getOptionValue(schemeOption.getOpt());
        }
        if (line.hasOption(operationOption.getOpt())) {
            operation = line.getOptionValue(operationOption.getOpt());
        }
        if (line.hasOption(prefixOption.getOpt())) {
            prefix = line.getOptionValue(prefixOption.getOpt());
        }
        if (line.hasOption(sizeOption.getOpt())) {
            size = Integer.parseInt(line.getOptionValue(sizeOption.getOpt()));
        }
        if (line.hasOption(timesOption.getOpt())) {
            times = Integer.parseInt(line.getOptionValue(timesOption.getOpt()));
        }
        if (line.hasOption(clientNumOption.getOpt())) {
            clientNum = Integer.parseInt(line.getOptionValue(clientNumOption.getOpt()));
        }

        // TODO verify of input parameters

        MultiClientBenchmark benchmark = new MultiClientBenchmark();
        ExecutorService threadpool = Executors.newFixedThreadPool(clientNum);
        List<MultiClientReplayer> clients = new ArrayList<>();
        for (int i = 0; i < clientNum; i++) {
            clients.add(new MultiClientReplayer(i, scheme, operation, prefix, size, times));
        }

        benchmark.callShellByExec("/home/hadoop/testCrail/clear.sh");
        benchmark.callShellByExec("/home/hadoop/testCrail/usage.sh");
        for (MultiClientReplayer client : clients) {
            threadpool.submit(client);
        }

        threadpool.shutdown();
        while (!threadpool.awaitTermination(10, TimeUnit.SECONDS)) {
            // check whether threads in threadpool are finished every 10s
            // and blocking until all threads are finished
        }
        benchmark.callShellByExec("/home/hadoop/testCrail/usage.sh");

        benchmark.getClientsLatency(clients);
    }
}

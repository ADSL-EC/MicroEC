package org.apache.crail.tools;

import org.apache.commons.cli.*;
import java.util.*;

public class TraceBenchmark {

    public static void main(String[] args) throws Exception {

        String trace = "ibm";
        String scheme = "replicas";
        String warmPath = null;
        String testPath = null;
        String prefix = null;

        String traceTypes = "ibm|ycsb";
        String schemeTypes = "replicas|nativeec|microec|hydra";
        Option traceOption = Option.builder("t").desc("type of trace [" + traceTypes + "]").hasArg().build();
        Option schemeOption = Option.builder("s").desc("type of scheme [" + schemeTypes + "]").hasArg().build();
        Option warmPathOption = Option.builder("w").desc("path of warm trace").hasArg().build();
        Option testPathOption = Option.builder("p").desc("path of test trace").hasArg().build();
        Option prefixOption = Option.builder("f").desc("prefix of each ycsb compute node").hasArg().build();

        Options options = new Options();
        options.addOption(traceOption);
        options.addOption(schemeOption);
        options.addOption(warmPathOption);
        options.addOption(testPathOption);
        options.addOption(prefixOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
        if (line.hasOption(traceOption.getOpt())) {
            trace = line.getOptionValue(traceOption.getOpt());
        }
        if (line.hasOption(schemeOption.getOpt())) {
            scheme = line.getOptionValue(schemeOption.getOpt());
        }
        if (line.hasOption(warmPathOption.getOpt())) {
            warmPath = line.getOptionValue(warmPathOption.getOpt());
        }
        if (line.hasOption(testPathOption.getOpt())) {
            testPath = line.getOptionValue(testPathOption.getOpt());
        }
        if (line.hasOption(prefixOption.getOpt())) {
            prefix = line.getOptionValue(prefixOption.getOpt());
        }

        TraceReplayer benchmark = new TraceReplayer(scheme);
        // get trace info
        benchmark.open();
        if (trace.equals("ibm")) {
            benchmark.initIBMTrace(warmPath, testPath);
        } else if (trace.equals("ycsb")) {
            benchmark.initYCSBTrace(warmPath, testPath, prefix, 4 * 1024);
        } else {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("crail tracebench", options);
            System.exit(-1);
        }

        benchmark.traceTest();
        benchmark.close();
    }
}

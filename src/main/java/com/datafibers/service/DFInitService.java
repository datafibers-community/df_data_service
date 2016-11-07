package com.datafibers.service;

import com.datafibers.test_tool.AvroProducerTest;
import com.datafibers.util.CLIParser;
import com.datafibers.util.Runner;
import com.datafibers.test_tool.UnitTestSuiteFlink;
import org.apache.commons.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DFInitService {

    private static final Logger LOG = LoggerFactory.getLogger(DFInitService.class);
    private static String runningMode;

    public static void main(String[] args) {

        welcome();
        LOG.info("*********Starting DataFibers Services ...");

        CLIParser cli = new CLIParser(args);
        cli.parse();
        runningMode = cli.getRunMode();

        if (runningMode == null) {
            Runner.runExample(DFDataProcessor.class);
            Runner.runExample(DFWebUI.class);
        } else {
            if (runningMode.contains("TEST")) runTestCases();
            if (runningMode.contains("Cluster")) Runner.runClusteredExample(DFDataProcessor.class);
            if (runningMode.contains("Standalone")) Runner.runExample(DFDataProcessor.class);
            if (runningMode.contains("WebUI")) Runner.runExample(DFWebUI.class);
        }

        LOG.info("*********Start DataFibers Services Completed :)");
    }

    public static void welcome() {
        System.out.println(" __    __     _                            _             ___      _          ___ _ _                   \n" +
                "/ / /\\ \\ \\___| | ___ ___  _ __ ___   ___  | |_ ___      /   \\__ _| |_ __ _  / __(_) |__   ___ _ __ ___ \n" +
                "\\ \\/  \\/ / _ \\ |/ __/ _ \\| '_ ` _ \\ / _ \\ | __/ _ \\    / /\\ / _` | __/ _` |/ _\\ | | '_ \\ / _ \\ '__/ __|\n" +
                " \\  /\\  /  __/ | (_| (_) | | | | | |  __/ | || (_) |  / /_// (_| | || (_| / /   | | |_) |  __/ |  \\__ \\\n" +
                "  \\/  \\/ \\___|_|\\___\\___/|_| |_| |_|\\___|  \\__\\___/  /___,' \\__,_|\\__\\__,_\\/    |_|_.__/ \\___|_|  |___/\n" +
                "                                                                                                       ");
    }

    public static void runTestCases() {
        try {
            String testcaseNumber = runningMode.replaceAll("[^0-9]", "");
            switch (testcaseNumber) {
                case "1":
                    AvroProducerTest.main(new String[10]);
                    break;
                case "2":
                    UnitTestSuiteFlink.testFlinkAvroSQL();
                    break;
                case "3":
                    UnitTestSuiteFlink.testFlinkAvroSerDe("http://localhost:8081");
                    break;
                case "4":
                    UnitTestSuiteFlink.testFlinkAvroSQLJson();
                default:
                    break;
            }

        } catch (IOException|DecoderException ioe) {
            ioe.printStackTrace();
        }

    }
}

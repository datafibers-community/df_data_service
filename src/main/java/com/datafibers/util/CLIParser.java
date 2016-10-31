package com.datafibers.util;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.*;

public class CLIParser {
    private static final Logger LOG = Logger.getLogger(CLIParser.class.getName());
    private String[] args = null;
    private Options options = new Options();
    public static String run_mode;
    public static String service_mode;
    public static String test_mode;

    public CLIParser(String[] args) {
        this.args = args;
        options.addOption("h", "help", false, "show help.");
        options.addOption("t", "test", false, "run test cases");
        options.addOption("u", "webui", true, "enable web <arg>=ui|noui");
        options.addOption("m", "mode", true, "running vertx mode, <arg>=cluster|standalone");
    }

    public CommandLine parse() {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h"))
                help();

            if (cmd.hasOption("m")) {
                if(cmd.getOptionValue("m").equalsIgnoreCase("cluster")) {
                    this.run_mode = "Cluster"; // Cluster
                } else {
                    this.run_mode = "Standalone"; // Standalone
                }
                // Whatever you want to do with the setting goes here
            }

            if (cmd.hasOption("u")) {
                if(cmd.getOptionValue("u").equalsIgnoreCase("ui")) {
                    this.service_mode = "WebUI"; // UI only
                } else {
                    this.service_mode = "Processor"; // Processor Only
                }
            }

            if (cmd.hasOption("t")) {
                this.test_mode = "TEST";
            } else {
                this.test_mode = "NO_TEST";
            }


        } catch (ParseException e) {
            LOG.log(Level.SEVERE, "Failed to parse command line properties", e);
            help();
        }
        return null;
    }

    public String getRunMode () {
        return this.run_mode + this.service_mode + this.test_mode;
    }

    public void help() {
        // This prints out some help
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
        System.exit(0);
    }
}

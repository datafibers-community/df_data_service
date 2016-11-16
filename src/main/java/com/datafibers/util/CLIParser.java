package com.datafibers.util;

import org.apache.commons.cli.*;
import org.slf4j.LoggerFactory;

public class CLIParser {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CLIParser.class);
    private String[] args = null;
    private Options options = new Options();
    public static String run_mode;
    public static String service_mode;
    public static String test_mode;
    public static String admin_tool;


    public CLIParser(String[] args) {
        this.args = args;
        options.addOption("h", "help", false, "show help.");
        options.addOption("t", "test", true, "run configured test cases, <arg>=test_case_number");
        options.addOption("u", "webui", true, "enable web, <arg>=ui|noui");
        options.addOption("m", "mode", true, "running vertx mode, <arg>=cluster|standalone");
        options.addOption("a", "admin", true, "run admin tools, <arg>=admin_tool_name");
    }

    public CommandLine parse() {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        if (args == null || args.length == 0) return null;
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
                if(cmd.getOptionValue("t").matches("[-+]?\\d*\\.?\\d+")) {
                    this.test_mode = "TEST_CASE_" + cmd.getOptionValue("t");
                } else {
                    this.test_mode = "TEST_CASE_1";
                }
            } else {
                this.test_mode = "NO_TEST";
            }

            if (cmd.hasOption("a")) {
                if(cmd.getOptionValue("a") != null)
                    this.admin_tool = "ADMIN_TOOL_" + cmd.getOptionValue("a");
            } else {
                this.admin_tool = "NO_ADMIN_TOOL";
            }


        } catch (ParseException e) {
            LOG.warn("Failed to parse command line properties", e);
            help();
        }
        return null;
    }

    public String getRunMode () {
        if (args == null || args.length == 0) {
            LOG.info("Starting both DataFibers Service and Web UI ...");
            return null;
        }
        LOG.info("Starting DataFibers in customized options.");
        LOG.info("run_mode = " + this.run_mode);
        LOG.info("service_mode = " + this.service_mode);
        LOG.info("test_mode = " + this.test_mode);
        return this.run_mode + this.service_mode + this.test_mode;
    }

    public void help() {
        // This prints out some help
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
        System.exit(0);
    }
}

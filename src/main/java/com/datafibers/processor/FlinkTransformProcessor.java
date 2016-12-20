package com.datafibers.processor;

import com.datafibers.flinknext.DFRemoteStreamEnvironment;
import com.datafibers.flinknext.Kafka09AvroTableSource;
import com.datafibers.flinknext.Kafka09JsonTableSink;
import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.log4j.Logger;
import java.util.Properties;

public class FlinkTransformProcessor {
    private static final Logger LOG = Logger.getLogger(FlinkTransformProcessor.class);
    /**
     * This method first submit a flink job against Kafka streaming in other thread. Then, it captures job_id from console.
     * After that of 8000 milliseconds, it restores the system.out and put newly captured job_id to job config property
     * flink.submit.job.id. At the end, update the record at repo - mongo as response.
     *
     * @param mongoCOLLECTION The mongodb collection name
     * @param mongoClient The client used to insert final data to repository - mongodb
     * @param transSql The SQL string defined for data transformation
     * @param outputTopic The kafka topic to keep transformed data
     * @param inputTopic The kafka topic to keep source data
     * @param colSchemaList The list of data type for the select columns
     * @param colNameList The list of name for the select columns
     * @param groupid The Kafka consumer id
     * @param kafkaHostPort The hostname and port for kafka
     * @param zookeeperHostPort The hostname and port for zookeeper
     * @param flinkEnv The Flink runtime enter point
     * @param maxRunTime The max run time for thread of submitting flink job
     * @param vertx The vertx enter point
     * @param dfJob The job config object
     */
    public static void submitFlinkSQL(DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                      DFRemoteStreamEnvironment flinkEnv, String zookeeperHostPort,
                                      String kafkaHostPort, String groupid, String colNameList,
                                      String colSchemaList, String inputTopic, String outputTopic,
                                      String transSql, MongoClient mongoClient, String mongoCOLLECTION) {

        String inputTopic_stage = "df_trans_stage_" + inputTopic;
        String outputTopic_stage = "df_trans_stage_" + outputTopic;

        String uuid = dfJob.hashCode() + "_";

        // Submit Flink through client in vertx worker thread and terminate it once the job is launched.
        WorkerExecutor exec_flink = vertx.createSharedWorkerExecutor(dfJob.getName() + dfJob.hashCode(),ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
        // Submit Flink job in separate thread
        exec_flink.executeBlocking(future -> {

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(flinkEnv);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", kafkaHostPort); //9092 for kafka server
            properties.setProperty("group.id", groupid);

            String[] fieldNames = colNameList.split(",");

            String[] fields = colSchemaList.split(",");
            Class<?>[] fieldTypes = new Class[fields.length];
            String temp;
            for (int i = 0; i < fields.length; i++) {
                try {
                    switch (fields[i].trim().toLowerCase()) {
                        case "boolean":
                        case "string":
                        case "long":
                        case "float":
                        case "integer":
                        case "bytes":
                            temp = StringUtils.capitalize(fields[i].trim().toLowerCase());
                            break;
                        case "date":
                            temp = "java.util.Date";
                            break;
                        default: temp = fields[i].trim();
                    }
                    fieldTypes[i] = Class.forName(temp);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            Table result;

            if (dfJob.getConnectorConfig().get("data.format.input").trim().toLowerCase().
                    equalsIgnoreCase("json_string")){
                // Internal covert Json String to Json - Begin
                DataStream<String> stream = flinkEnv
                        .addSource(new FlinkKafkaConsumer09<>(inputTopic, new SimpleStringSchema(), properties));

                stream.map(new MapFunction<String, String>() {
                    @Override
                    public String map(String jsonString) throws Exception {
                        return jsonString.replaceAll("\\\\", "").replace("\"{", "{").replace("}\"","}");
                    }
                }).addSink(new FlinkKafkaProducer09<String>(kafkaHostPort, inputTopic_stage, new SimpleStringSchema()));
                // Internal covert Json String to Json - End

                KafkaJsonTableSource kafkaTableSource =
                        new Kafka09JsonTableSource(inputTopic_stage, properties, fieldNames, fieldTypes);

                tableEnv.registerTableSource(inputTopic_stage, kafkaTableSource);

                // run a SQL query on the Table and retrieve the result as a new Table
                result = tableEnv.sql(transSql.replace(inputTopic, inputTopic_stage));
            } else { // Topic has json data, no transformation needed
                KafkaJsonTableSource kafkaTableSource =
                        new Kafka09JsonTableSource(inputTopic, properties, fieldNames, fieldTypes);

                tableEnv.registerTableSource(inputTopic, kafkaTableSource);

                // run a SQL query on the Table and retrieve the result as a new Table
                result = tableEnv.sql(transSql);
            }

            FixedPartitioner partition =  new FixedPartitioner();

            if (dfJob.getConnectorConfig().get("data.format.output").trim().toLowerCase().
                    equalsIgnoreCase("json_string")) {
                Kafka09JsonTableSink sinkJsonString = new Kafka09JsonTableSink (outputTopic_stage, properties, partition);
                result.writeToSink(sinkJsonString); // Flink will create the output result topic automatically

                // Internal covert Json to Json String - Begin
                DataStream<String> stream = flinkEnv
                        .addSource(new FlinkKafkaConsumer09<>(outputTopic_stage, new SimpleStringSchema(), properties));

                stream.map(new MapFunction<String, String>() {
                    @Override
                    public String map(String jsonString) throws Exception {
                        return "\"" +  jsonString.replace("\"", "\\\"") + "\"";
                    }
                }).addSink(new FlinkKafkaProducer09<String>(kafkaHostPort, outputTopic, new SimpleStringSchema()));
                // Internal covert Json to Json String - End
            } else {
                Kafka09JsonTableSink sink = new Kafka09JsonTableSink (outputTopic, properties, partition);
                result.writeToSink(sink); // Flink will create the output result topic automatically
            }

            // create a TableSink
            try {
                JobExecutionResult jres = flinkEnv.executeWithDFObj("DF_FLINK_TRANS_" + uuid, dfJob);
                future.complete(jres);
            } catch (Exception e) {
                LOG.error("Flink Submit Exception");
            }

        }, res -> {
            LOG.debug("BLOCKING CODE IS TERMINATE?FINISHED");

        });

        long timerID = vertx.setTimer(8000, id -> {
            LOG.info("Job - DF_FLINK_TRANS_" + uuid +  "'s JobID is - " + dfJob.getFlinkIDFromJobConfig());
            mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", dfJob.getId()),
                    new JsonObject().put("$set", dfJob.toJson()), v -> {
                        if (v.failed()) {
                            LOG.error("update Flink JOb_ID Failed.", v.cause());
                        }
                    }
            );
        });
    }

    /**
     * This is to read JSON data and output JSON format of data
     * This method first submit a flink job against Kafka streaming in other thread. Then, it captures job_id from console.
     * After that of 8000 milliseconds, it restores the system.out and put newly captured job_id to job config property
     * flink.submit.job.id. At the end, update the record at repo - mongo as response.
     * @param dfJob
     * @param vertx
     * @param maxRunTime
     * @param flinkEnv
     * @param zookeeperHostPort
     * @param kafkaHostPort
     * @param groupid
     * @param colNameList
     * @param colSchemaList
     * @param inputTopic
     * @param outputTopic
     * @param transSql
     * @param mongoClient
     * @param mongoCOLLECTION The mongodb collection name
     */
    public static void submitFlinkSQLJ2J(DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                      DFRemoteStreamEnvironment flinkEnv, String zookeeperHostPort,
                                      String kafkaHostPort, String groupid, String colNameList,
                                      String colSchemaList, String inputTopic, String outputTopic,
                                      String transSql, MongoClient mongoClient, String mongoCOLLECTION) {

        String uuid = dfJob.hashCode() + "_";

        WorkerExecutor exec_flink = vertx.createSharedWorkerExecutor(dfJob.getName() + dfJob.hashCode(),ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);

        exec_flink.executeBlocking(future -> {

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(flinkEnv);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", kafkaHostPort);
            properties.setProperty("zookeeper.connect", zookeeperHostPort);
            properties.setProperty("group.id", groupid);

            String[] fieldNames = colNameList.split(",");
            String[] fields = colSchemaList.split(",");
            Class<?>[] fieldTypes = new Class[fields.length];
            String temp;
            for (int i = 0; i < fields.length; i++) {
                try {
                    switch (fields[i].trim().toLowerCase()) {
                        case "boolean":
                        case "string":
                        case "long":
                        case "float":
                        case "integer":
                        case "bytes":
                            temp = StringUtils.capitalize(fields[i].trim().toLowerCase());
                            break;
                        case "date":
                            temp = "java.util.Date";
                            break;
                        default: temp = fields[i].trim();
                    }
                    fieldTypes[i] = Class.forName(temp);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            Table result;
            KafkaJsonTableSource kafkaTableSource =
                        new Kafka09JsonTableSource(inputTopic, properties, fieldNames, fieldTypes);
            tableEnv.registerTableSource(inputTopic, kafkaTableSource);
            // run a SQL query on the Table and retrieve the result as a new Table
            result = tableEnv.sql(transSql);
            Kafka09JsonTableSink sink = new Kafka09JsonTableSink (outputTopic, properties, new FixedPartitioner());
            result.writeToSink(sink); // Flink will create the output result topic automatically

            try {
                JobExecutionResult jres = flinkEnv.executeWithDFObj("DF_FLINK_TRANS_" + uuid, dfJob);
                future.complete(jres);
            } catch (Exception e) {
                LOG.error("Flink Submit Exception" + e.getCause());
            }

        }, res -> {
            LOG.debug("BLOCKING CODE IS TERMINATE?FINISHED");
        });

        long timerID = vertx.setTimer(8000, id -> {
            LOG.info("Job - DF_FLINK_TRANS_" + uuid +  "'s JobID is - " + dfJob.getFlinkIDFromJobConfig());
            mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", dfJob.getId()),
                    new JsonObject().put("$set", dfJob.toJson()), v -> {
                        if (v.failed()) {
                            LOG.error("update Flink JOb_ID Failed.", v.cause());
                        }
                    }
            );
        });
    }
    /**
     * This is to read AVRO data and output JSON format of data
     * This method first submit a flink job against Kafka streaming in other thread.
     * At the end, update the record at repo - mongo as response.
     * @param dfJob
     * @param vertx
     * @param maxRunTime
     * @param flinkEnv
     * @param zookeeperHostPort
     * @param kafkaHostPort
     * @param groupid
     * @param inputTopic
     * @param outputTopic
     * @param transSql
     * @param mongoClient
     * @param mongoCOLLECTION
     */
    public static void submitFlinkSQLA2J(DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                         DFRemoteStreamEnvironment flinkEnv, String zookeeperHostPort,
                                         String kafkaHostPort, String SchemaRegistryHostPort, String groupid,
                                         String inputTopic, String outputTopic,
                                         String transSql, String schemSubject, String staticSchemaString,
                                         MongoClient mongoClient, String mongoCOLLECTION) {

        String uuid = dfJob.hashCode() + "";
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(flinkEnv);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaHostPort);
        properties.setProperty("group.id", groupid);
        properties.setProperty("schema.subject", schemSubject);
        properties.setProperty("schema.registry", SchemaRegistryHostPort);
        properties.setProperty("static.avro.schema", staticSchemaString);

        LOG.info(HelpFunc.getPropertyAsString(properties));

        WorkerExecutor exec_flink = vertx.createSharedWorkerExecutor(dfJob.getName() + dfJob.hashCode(),ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);

        exec_flink.executeBlocking(future -> {

            try {
                Kafka09AvroTableSource kafkaAvroTableSource =  new Kafka09AvroTableSource(inputTopic, properties);
                tableEnv.registerTableSource(inputTopic, kafkaAvroTableSource);
                Table result = tableEnv.sql(transSql);
                Kafka09JsonTableSink json_sink =
                        new Kafka09JsonTableSink (outputTopic, properties, new FixedPartitioner());
                result.writeToSink(json_sink);
                JobExecutionResult jres = flinkEnv.executeWithDFObj("DF_FLINK_TRANS_SQL_AVRO_TO_JSON_" + uuid, dfJob);
            } catch (Exception e) {
                LOG.error("Flink Submit Exception" + e.getCause());
            }

        }, res -> {
            LOG.debug("BLOCKING CODE IS TERMINATE?FINISHED" + res.cause());
        });

        long timerID = vertx.setTimer(8000, id -> {
            // exec_flink.close();
            LOG.info("Job - DF_FLINK_TRANS_" + uuid +  "'s JobID is - " + dfJob.getFlinkIDFromJobConfig());
            mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", dfJob.getId()),
                    new JsonObject().put("$set", dfJob.toJson()), v -> {
                        if (v.failed()) {
                            LOG.error("update Flink JOb_ID Failed.", v.cause());
                        }
                    }
            );
        });
    }

    /**
     * This method lunch a local flink CLI and connect specified job manager in order to cancel the job.
     * Job may not exist. In this case, just delete it for now.
     * @param jobManagerHostPort The job manager address and port where to send cancel
     * @param jobID The job ID to cancel for flink job
     * @param mongoClient repo handler
     * @param mongoCOLLECTION collection to keep data
     * @param routingContext response for rest client
     */
    public static void cancelFlinkSQL(String jobManagerHostPort, String jobID,
                                      MongoClient mongoClient, String mongoCOLLECTION, RoutingContext routingContext,
                                      Boolean cancelRepoAndSendResp) {
        String id = routingContext.request().getParam("id");

        try {
            String cancelCMD = "cancel;-m;" + jobManagerHostPort + ";" + jobID;
            CliFrontend cli = new CliFrontend("conf/flink-conf.yaml");
            int retCode = cli.parseParameters(cancelCMD.split(";"));
            LOG.info("Flink job " + jobID + " is canceled " + ((retCode == 0)? "successful.":"failed."));

            String respMsg = (retCode == 0)? " is deleted from repository.":
                    " is deleted from repository, but Job_ID is not found.";
            if(cancelRepoAndSendResp) {
                mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
                        remove -> routingContext.response().end(id + respMsg));
            }

        } catch (IllegalArgumentException ire) {
            LOG.warn("No Flink job found with ID for cancellation");
        } catch (Throwable t) {
            LOG.error("Fatal error while running command line interface.", t.getCause());
        }
    }

    public static void cancelFlinkSQL(String jobManagerHostPort, String jobID,
                                      MongoClient mongoClient, String mongoCOLLECTION, RoutingContext routingContext) {
        cancelFlinkSQL(jobManagerHostPort, jobID, mongoClient, mongoCOLLECTION, routingContext, Boolean.TRUE);
    }

    public static void updateFlinkSQL (DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                       DFRemoteStreamEnvironment flinkEnv, String zookeeperHostPort,
                                       String kafkaHostPort, String groupid, String colNameList,
                                       String colSchemaList, String inputTopic, String outputTopic,
                                       String transSql, MongoClient mongoClient, String mongoCOLLECTION,
                                       String jobManagerHostPort, RoutingContext routingContext,
                                       String SchemaRegistryHostPort, String schemSubject, String staticSchemaString) {

        final String id = routingContext.request().getParam("id");

        cancelFlinkSQL(jobManagerHostPort, dfJob.getJobConfig().get("flink.submit.job.id"),
                mongoClient, mongoCOLLECTION, routingContext, Boolean.FALSE);

        // Submit generic Flink SQL Json|Avro
        if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_FLINK_SQL_GENE.name()) {
            submitFlinkSQL(dfJob, vertx, maxRunTime, flinkEnv, zookeeperHostPort, kafkaHostPort, groupid, colNameList,
                    colSchemaList, inputTopic, outputTopic, transSql, mongoClient, mongoCOLLECTION);
        }

        // Submit Flink UDF
        if(dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_FLINK_UDF.name()) {
            FlinkTransformProcessor.runFlinkJar(dfJob.getUdfUpload(), jobManagerHostPort);
        }

        // Submit Flink SQL Avro to Json
        if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_FLINK_SQL_A2J.name()) {
            submitFlinkSQLA2J(dfJob, vertx, maxRunTime, flinkEnv, zookeeperHostPort, kafkaHostPort,
                    SchemaRegistryHostPort, groupid, inputTopic, outputTopic, transSql, schemSubject, staticSchemaString,
                    mongoClient, mongoCOLLECTION);
        }

        // Submit Flink SQL Json to Json
        if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_FLINK_SQL_J2J.name()) {
            submitFlinkSQLJ2J(dfJob, vertx, maxRunTime, flinkEnv, zookeeperHostPort, kafkaHostPort, groupid, colNameList,
                    colSchemaList, inputTopic, outputTopic, transSql, mongoClient, mongoCOLLECTION);
        }

        mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id), // Select a unique document
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", dfJob.toJson()), v -> {
                    if (v.failed()) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(HelpFunc.errorMsg(133, "updateOne to repository is failed."));
                    } else {
                        routingContext.response().putHeader(ConstantApp.CONTENT_TYPE,
                                ConstantApp.APPLICATION_JSON_CHARSET_UTF_8).end();
                    }
                }
        );



    }

    public static void updateFlinkSQL (DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                       DFRemoteStreamEnvironment flinkEnv, String zookeeperHostPort,
                                       String kafkaHostPort, String groupid, String colNameList,
                                       String colSchemaList, String inputTopic, String outputTopic,
                                       String transSql, MongoClient mongoClient, String mongoCOLLECTION,
                                       String jobManagerHostPort, RoutingContext routingContext) {

        updateFlinkSQL (dfJob, vertx, maxRunTime, flinkEnv, zookeeperHostPort,
                kafkaHostPort, groupid, colNameList,
                colSchemaList, inputTopic, outputTopic,
                transSql, mongoClient, mongoCOLLECTION,
                jobManagerHostPort, routingContext, null, null, null);

    }

    /**
     * This method lunch a local flink CLI and connect specified job manager in order to cancel the job.
     * Job may not exist. In this case, just delete it for now.
     * @param jobManagerHostPort The job manager address and port where to send cancel
     * @param jarFile The Jar file name uploaded
     */
    public static void runFlinkJar (String jarFile, String jobManagerHostPort) {

        try {
            String runCMD = "run;-m;" + jobManagerHostPort + ";" + jarFile;
            CliFrontend cli = new CliFrontend("conf/flink-conf.yaml");
            int retCode = cli.parseParameters(runCMD.split(";"));
            String respMsg = (retCode == 0)? "Flink job is submitted for Jar UDF at " :
                    "Flink job is failed to submit for Jar UDF at " + jarFile;
            LOG.info(respMsg);

        } catch (IllegalArgumentException ire) {
            LOG.error("No Flink job found with ID for cancellation");
        } catch (ProgramInvocationException t) {
            LOG.error("Fatal error while running the jar file.", t.getCause());
        } catch (Exception e) {
            LOG.error("Flink submit UDF Jar run-time exception.");
        }


    }
}

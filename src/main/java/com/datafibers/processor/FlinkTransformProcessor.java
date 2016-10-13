package com.datafibers.processor;

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
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.Properties;

/* This is sample transform config
    {
        "group.id":"consumer3",
        "data.format.input":"json_string",
        "data.format.output":"json_string",
        "avro.schema.enabled":"false",
        "column.name.list":"symbol,name",
        "column.schema.list":"string,string",
        "topic.for.query":"finance",
        "topic.for.result":"stock",
        "trans.sql":"SELECT STREAM symbol, name FROM finance"
    }
*/

/* This is sample udf config
    {
        "group.id":"consumer3",
        "data.format.input":"json_string",
        "data.format.output":"json_string",
        "avro.schema.enabled":"false",
        "topic.for.query":"finance",
        "topic.for.result":"stock",
        "trans.jar":"flinkUDFDemo.jar"
    }
*/

public class FlinkTransformProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkTransformProcessor.class);

    /**
     * This method first submit a flink job against Kafka streaming in other thread. Then, it captures job_id from console.
     * After that of 8000 milliseconds, it restores the system.out and put newly captured job_id to job config property
     * flink.submit.job.id. At the end, update the record at repo - mongo as response.
     *
     * @param flinkToKafkaTopicReplicationNum The number of replications for the Kafka topic carrying Flink result
     * @param flinkToKafkaTopicPartitionNum he number of partitions for the Kafka topic carrying Flink result
     * @param routingContext The response for REST API
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
                                      StreamExecutionEnvironment flinkEnv, String zookeeperHostPort,
                                      String kafkaHostPort, String groupid, String colNameList,
                                      String colSchemaList, String inputTopic, String outputTopic,
                                      String transSql, MongoClient mongoClient, String mongoCOLLECTION) {

        String inputTopic_stage = "df_trans_stage_" + inputTopic;
        String outputTopic_stage = "df_trans_stage_" + outputTopic;

        String uuid = dfJob.hashCode() + "_" +
                dfJob.getName() + "_" + dfJob.getConnector() + "_" + dfJob.getTaskId();

        // Create a stream to hold the output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        // IMPORTANT: Save the old System.out!
        PrintStream old = System.out;
        // Submit Flink through client in vertx worker thread and terminate it once the job is launched.
        WorkerExecutor exec_flink = vertx.createSharedWorkerExecutor(dfJob.getName() + dfJob.hashCode(),5, maxRunTime);
        // Submit Flink job in separate thread
        exec_flink.executeBlocking(future -> {
            // Tell Java to use your special stream
            System.setOut(ps);

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(flinkEnv);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", kafkaHostPort); //9092 for kafka server
            // only required for Kafka 0.9
            properties.setProperty("zookeeper.connect", zookeeperHostPort);
            properties.setProperty("group.id", groupid);

            String[] fieldNames = colNameList.split(",");

            String[] fields = colSchemaList.split(",");
            Class<?>[] fieldTypes = new Class[fields.length];
            String temp;
            for (int i = 0; i < fields.length; i++) {
                try {
                    switch (fields[i].trim().toLowerCase()) {
                        case "string":
                            temp = "java.lang.String";
                            break;
                        case "date":
                            temp = "java.util.Date";
                            break;
                        case "integer":
                            temp = "java.lang.Integer";
                            break;
                        case "long":
                            temp = "java.lang.Long";
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
                JobExecutionResult jres = flinkEnv.execute("DF_FLINK_TRANS_" + uuid);
                future.complete(jres);

            } catch (JobExecutionException je) {
                LOG.info("Flink Client is terminated normally once the job is submitted.");
            } catch (InterruptedException ie) {
                LOG.info("Flink Client is terminated normally once the job is submitted.");
            } catch (Exception e) {
                LOG.error("Flink Submit Exception");
            }

        }, res -> {
            LOG.debug("BLOCKING CODE IS TERMINATE?FINISHED");

        });

        long timerID = vertx.setTimer(8000, id -> {
            // Put things back
            System.out.flush();
            System.setOut(old);
            // Show what happened
            String jobID = StringUtils.substringBetween(baos.toString(),
                    "Submitting job with JobID:", "Waiting for job completion.").trim().replace(".", "");
            /*
            Close previous flink exec thread. This will lead Flink client to throw InterruptedException.
            We capture it in above job submission code to ignore this as normal
             */
            exec_flink.close();
            LOG.info("Job - DF_FLINK_TRANS_" + uuid +  "'s JobID is captured - " + jobID);
            dfJob.setFlinkIDToJobConfig(jobID);

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
                                       StreamExecutionEnvironment flinkEnv, String zookeeperHostPort,
                                       String kafkaHostPort, String groupid, String colNameList,
                                       String colSchemaList, String inputTopic, String outputTopic,
                                       String transSql, MongoClient mongoClient, String mongoCOLLECTION,
                                       String jobManagerHostPort, RoutingContext routingContext) {

        final String id = routingContext.request().getParam("id");

        cancelFlinkSQL(jobManagerHostPort, dfJob.getJobConfig().get("flink.submit.job.id"),
                mongoClient, mongoCOLLECTION, routingContext, Boolean.FALSE);

        submitFlinkSQL(dfJob, vertx, maxRunTime, flinkEnv, zookeeperHostPort, kafkaHostPort, groupid, colNameList,
                colSchemaList, inputTopic, outputTopic, transSql, mongoClient, mongoCOLLECTION);

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
            LOG.warn("No Flink job found with ID for cancellation");
        } catch (Throwable t) {
            LOG.error("Fatal error while running command line interface.", t.getCause());
        }


    }
}

package com.datafibers.processor;

import com.datafibers.exception.DFPropertyValidationException;
import com.datafibers.util.SchemaRegistryClient;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import java.util.Properties;
import net.openhft.compiler.CompilerUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.log4j.Logger;
import com.datafibers.flinknext.DFRemoteStreamEnvironment;
import com.datafibers.flinknext.Kafka09AvroTableSink;
import com.datafibers.flinknext.Kafka09AvroTableSource;
import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.DynamicRunner;
import com.datafibers.util.HelpFunc;

public class FlinkTransformProcessor {
    private static final Logger LOG = Logger.getLogger(FlinkTransformProcessor.class);

    /**
     * This is to read AVRO data and output JSON format of data
     * This method first submit a flink job against Kafka streaming in other thread.
     * At the end, update the record at repo - mongo as response.
     * It supports both SQL and table API
     * @param dfJob
     * @param vertx
     * @param maxRunTime
     * @param flinkEnv
     * @param kafkaHostPort
     * @param groupid
     * @param topicIn
     * @param topicOut
     * @param sinkKeys
     * @param transScript - this either SQL or table API
     * @param mongoClient
     * @param mongoCOLLECTION
     * @param engine - SQL_API or TABLE_API
     */
    public static void submitFlinkJobA2A(DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                         DFRemoteStreamEnvironment flinkEnv, String kafkaHostPort,
                                         String SchemaRegistryHostPort, String groupid,
                                         String topicIn, String topicOut, String sinkKeys,
                                         String transScript, String schemaSubjectIn, String schemaSubjectOut,
                                         MongoClient mongoClient, String mongoCOLLECTION, String engine) {

        String uuid = dfJob.hashCode() + "";
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(flinkEnv);

        // Set all properties. Note, all inputTopic related properties are set later in loop
        Properties properties = new Properties();
        properties.setProperty(ConstantApp.PK_KAFKA_HOST_PORT, kafkaHostPort);
        properties.setProperty(ConstantApp.PK_KAFKA_CONSUMER_GROURP, groupid);
        properties.setProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT, schemaSubjectOut);
        properties.setProperty(ConstantApp.PK_KAFKA_SCHEMA_REGISTRY_HOST_PORT, SchemaRegistryHostPort);
        properties.setProperty(ConstantApp.PK_FLINK_TABLE_SINK_KEYS, sinkKeys);

        // delivered properties
        properties.setProperty(ConstantApp.PK_SCHEMA_ID_OUTPUT,
                SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT) + "");
        properties.setProperty(ConstantApp.PK_SCHEMA_STR_OUTPUT,
                SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT).toString());

        // Check and validate properties
        String[] topicInList = topicIn.split(",");
        String[] schemaSubjectInList = schemaSubjectIn.split(",");

        if(topicInList.length != schemaSubjectInList.length)
            throw new DFPropertyValidationException("Number of inputTopic and inputTopicSchema Mismatch.");
        if(engine.equalsIgnoreCase("tABLE_API") && (topicInList.length > 1 || schemaSubjectInList.length > 1))
            throw new DFPropertyValidationException("Script/Table API only supports single inputTopic/inputTopicSchema.");

        WorkerExecutor exec_flink = vertx.createSharedWorkerExecutor(dfJob.getName() + dfJob.hashCode(),
                ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);

        exec_flink.executeBlocking(future -> {

            try {
                // Support multiple inputTopics by reusing the same property for each topic in the list
                // TODO - this is an alternative until we redesign the table source to support list of topics
                for (int i = 0; i < schemaSubjectInList.length; i++) {
                    // need to override all input meta, such as below
                    properties.setProperty(ConstantApp.PK_SCHEMA_SUB_INPUT, schemaSubjectInList[i]);
                    properties.setProperty(ConstantApp.PK_SCHEMA_ID_INPUT,
                            SchemaRegistryClient.getLatestSchemaIDFromProperty(properties,
                                    ConstantApp.PK_SCHEMA_SUB_INPUT) + "");
                    properties.setProperty(ConstantApp.PK_SCHEMA_STR_INPUT,
                            SchemaRegistryClient.getLatestSchemaFromProperty(properties,
                                    ConstantApp.PK_SCHEMA_SUB_INPUT).toString());
                    LOG.debug(HelpFunc.getPropertyAsString(properties));

                    tableEnv.registerTableSource(topicInList[i], new Kafka09AvroTableSource(topicInList[i], properties));
                }

                Table result = null;
                switch (engine.toUpperCase()) {
                    case "SQL_API": {
                        result = tableEnv.sql(transScript);
                        break;
                    }

                    case "TABLE_API": { // TODO - does not support multiple input topics
                        Table ingest = tableEnv.scan(topicIn);
                        String className = "dynamic.FlinkScript";
                        String header = "package dynamic;\n" +
                                "import org.apache.flink.table.api.Table;\n" +
                                "import com.datafibers.util.*;\n";
                        String javaCode = header +
                                "public class FlinkScript implements DynamicRunner {\n" +
                                "@Override \n" +
                                "    public Table transTableObj(Table tbl) {\n" +
                                "try {" +
                                "return tbl." + transScript + ";\n" +
                                "} catch (Exception e) {" +
                                "};" +
                                "return null;}}";
                        // Dynamic code generation
                        Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode);
                        DynamicRunner runner = (DynamicRunner) aClass.newInstance();
                        result = runner.transTableObj(ingest);
                        break;
                    }
                    default: break;
                }

                Kafka09AvroTableSink avro_sink =
                        new Kafka09AvroTableSink (topicOut, properties, new FlinkFixedPartitioner());
                result.writeToSink(avro_sink);

                JobExecutionResult jres = flinkEnv.executeWithDFObj("DF_FLINK_A2A_" + engine.toUpperCase() + uuid, dfJob);
            } catch (Exception e) {
                LOG.error("Flink Submit Exception:" + e.getCause());
                e.printStackTrace();
            }

        }, res -> {
            LOG.debug("BLOCKING CODE IS TERMINATE?FINISHED" + res.cause());
        });

        long timerID = vertx.setTimer(maxRunTime, id -> {
            // exec_flink.close();
            LOG.info("Job - DF_FLINK_TRANS_" + uuid +  "'s JobID is - " + dfJob.getFlinkIDFromJobConfig());
            mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", dfJob.getId()),
                    new JsonObject().put("$set", dfJob.toJson()), v -> {
                        if (v.failed()) {
                            LOG.error("update Flink JOB_ID Failed.", v.cause());
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
    		if (jobID == null || jobID.trim().equalsIgnoreCase("")) {
    			LOG.warn("Flink job ID is empty or null in cancelFlinkSQL() ");
    		} 
    		//else {
    			String cancelCMD = "cancel;-m;" + jobManagerHostPort + ";" + jobID;
    			LOG.info("Flink job " + jobID + " cancel CMD: " + cancelCMD);
    			//CliFrontend cli = new CliFrontend("conf/flink-conf.yaml");
    			CliFrontend cli = new CliFrontend("conf");
    			int retCode = cli.parseParameters(cancelCMD.split(";"));
    			LOG.info("Flink job " + jobID + " is canceled " + ((retCode == 0)? "successful.":"failed."));

    			String respMsg = (retCode == 0)? " is deleted from repository.":
    				" is deleted from repository, but Job_ID is not found.";
    			if(cancelRepoAndSendResp) {
    				mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
    						remove -> routingContext.response().end(id + respMsg));
    			}
    		//}
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
                                       DFRemoteStreamEnvironment flinkEnv,
                                       String kafkaHostPort, String groupid,
                                       String inputTopic, String outputTopic,
                                       String transSql, MongoClient mongoClient, String mongoCOLLECTION,
                                       String jobManagerHostPort, RoutingContext routingContext,
                                       String SchemaRegistryHostPort, String schemSubject, String schemaSubjectOut,
                                       String sinkKeys) {

        final String id = routingContext.request().getParam("id");

        cancelFlinkSQL(jobManagerHostPort, dfJob.getJobConfig().get("flink.submit.job.id"),
                mongoClient, mongoCOLLECTION, routingContext, Boolean.FALSE);

        // Submit Flink UDF
        if(dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_FLINK_UDF.name()) {
            FlinkTransformProcessor.runFlinkJar(dfJob.getUdfUpload(), jobManagerHostPort);
        }

        // Submit Flink SQL Avro to Avro
        if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_FLINK_SQL_A2A.name()) {
            submitFlinkJobA2A(dfJob, vertx, maxRunTime, flinkEnv, kafkaHostPort,
                    SchemaRegistryHostPort, groupid, inputTopic, outputTopic, sinkKeys, transSql, schemSubject, schemaSubjectOut,
                    mongoClient, mongoCOLLECTION, "SQL_API");
        }

        // Submit Flink SQL Json to Json
        if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_FLINK_SCRIPT.name()) {
            submitFlinkJobA2A(dfJob, vertx, maxRunTime, flinkEnv, kafkaHostPort,
                    SchemaRegistryHostPort, groupid, inputTopic, outputTopic, sinkKeys, transSql, schemSubject, schemaSubjectOut,
                    mongoClient, mongoCOLLECTION, "TABLE_API");
        }

        mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id), // Select a unique document
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", dfJob.toJson()), v -> {
                    if (v.failed()) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(HelpFunc.errorMsg(133, "updateOne to repository is failed."));
                    } else {
                        routingContext.response()
                                .putHeader("Access-Control-Allow-Origin", "*")
                                .putHeader(ConstantApp.CONTENT_TYPE,
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
            //CliFrontend cli = new CliFrontend("conf/flink-conf.yaml");
            CliFrontend cli = new CliFrontend("conf");
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

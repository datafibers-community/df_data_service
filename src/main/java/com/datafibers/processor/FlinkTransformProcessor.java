package com.datafibers.processor;

import com.datafibers.exception.DFPropertyValidationException;
import com.datafibers.flinknext.Kafka010AvroTableSource;
import com.datafibers.util.*;
import com.hubrick.vertx.rest.MediaType;
import com.hubrick.vertx.rest.RestClient;
import com.hubrick.vertx.rest.RestClientRequest;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import java.util.Arrays;
import java.util.Properties;

import io.vertx.ext.web.client.WebClient;
import net.openhft.compiler.CompilerUtils;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.log4j.Logger;
import com.datafibers.flinknext.DFRemoteStreamEnvironment;
import com.datafibers.flinknext.Kafka09AvroTableSink;
import com.datafibers.model.DFJobPOPJ;
import org.json.JSONObject;

public class FlinkTransformProcessor {
    private static final Logger LOG = Logger.getLogger(FlinkTransformProcessor.class);

    /**
     * submitFlinkJar is a generic function to submit specific jar file with proper configurations, such as jar para,
     * to the Flink Rest API. This is used for Flink SQL, UDF, and Table API submission with different client class.
     * @param dfJob jd job object
     * @param programArgs parameters used by the jar files separated by " "
     * @param mongo mongo admin client
     * @param COLLECTION mongo collection name
     */
    public static void submitFlinkJar(WebClient client, DFJobPOPJ dfJob, MongoClient mongo,
                                      String jarVersionCollection, String taskCollection,
                                      String flinkRestHost, int flinkRestPort,
                                      String allowNonRestoredState, String savepointPath, String entryClass,
                                      String parallelism, String programArgs) {
        LOG.debug("SUBMIT JOB START");
        String taskId = dfJob.getId();
        // Search mongo to get the flink_jar_id
        mongo.findOne(jarVersionCollection, new JsonObject().put("_id", ConstantApp.FLINK_JAR_ID_IN_MONGO), null, res -> {
            if (res.succeeded()) {
                String df_jar_id = res.result().getString(ConstantApp.FLINK_JAR_VALUE_IN_MONGO);
                // Submit jar to Flink Rest API
                client.post(flinkRestPort, flinkRestHost, ConstantApp.FLINK_REST_URL_JARS + "/" + df_jar_id + "/run")
                        .addQueryParam("allowNonRestoredState", allowNonRestoredState)
                        .addQueryParam("savepointPath", savepointPath)
                        .addQueryParam("entry-class", entryClass)
                        .addQueryParam("parallelism", parallelism)
                        .addQueryParam("allowNonRestoredState", allowNonRestoredState)
                        .addQueryParam("program-args", programArgs)
                        .send(ar -> {
                            if (ar.succeeded()) {
                                String flinkJobId = ar.result().bodyAsJsonObject()
                                        .getString(ConstantApp.FLINK_JOB_SUBMIT_RESPONSE_KEY);

                                dfJob.setFlinkIDToJobConfig(flinkJobId)
                                        .setStatus(ConstantApp.DF_STATUS.RUNNING.name());
                                LOG.debug("dfJob to Json = " + dfJob.toJson());

                                mongo.updateCollection(taskCollection, new JsonObject().put("_id", taskId),
                                        new JsonObject().put("$set", dfJob.toJson()), v -> {
                                            if (v.failed()) {
                                                LOG.error(DFAPIMessage.logResponseMessage(1001, taskId));
                                            } else {
                                                LOG.info(DFAPIMessage.logResponseMessage(1005,
                                                        taskId + " flinkJobId = " + flinkJobId));
                                            }
                                        }
                                );
                            } else {
                                LOG.error(DFAPIMessage.logResponseMessage(9010, taskId +
                                        " details - " + ar.cause()));
                            }
                        });
            } else {
                LOG.error(DFAPIMessage.
                        logResponseMessage(9035, taskId + " details - " + res.cause()));
            }
        });
        LOG.debug("SUBMIT JOB END");
    }

    /**
     * This is to read AVRO data and output JSON format of data
     * This method first submit a flink job against Kafka streaming in other thread.
     * At the end, update the record at repo - mongo as response.
     * It supports both SQL and table API
     */
    @Deprecated
    public static void submitFlinkJobA2A(DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                         DFRemoteStreamEnvironment flinkEnv, String kafkaHostPort,
                                         String SchemaRegistryHostPort, String groupid,
                                         String topicIn, String topicOut, String sinkKeys,
                                         String transScript, String schemaSubjectIn, String schemaSubjectOut,
                                         MongoClient mongoClient, String mongoCOLLECTION, String engine) {

        String uuid = dfJob.hashCode() + "";
        LOG.debug("submitFlinkJobA2A is called with uuid = " + uuid);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(flinkEnv);

        // Set all properties. Note, all inputTopic related properties are set later in loop
        Properties properties = new Properties();
        properties.setProperty(ConstantApp.PK_KAFKA_HOST_PORT.replace("_", "."), kafkaHostPort); //has to use bootstrap.servers
        properties.setProperty(ConstantApp.PK_KAFKA_CONSUMER_GROURP, groupid);
        properties.setProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT, schemaSubjectOut);
        properties.setProperty(ConstantApp.PK_KAFKA_SCHEMA_REGISTRY_HOST_PORT.replace("_", "."), SchemaRegistryHostPort);
        properties.setProperty(ConstantApp.PK_FLINK_TABLE_SINK_KEYS, sinkKeys);

        // Check and validate properties
        String[] topicInList = topicIn.split(",");
        String[] schemaSubjectInList = schemaSubjectIn.split(",");

        if (topicInList.length != schemaSubjectInList.length) {
            LOG.error("Number of inputTopic and inputTopicSchema Mismatch. topicInList.length=" + topicInList.length +
                    " schemaSubjectInList.length=" + schemaSubjectInList.length);
            throw new DFPropertyValidationException("Number of inputTopic and inputTopicSchema Mismatch.");
        }

        if (engine.equalsIgnoreCase("TABLE_API") && (topicInList.length > 1 || schemaSubjectInList.length > 1)) {
            LOG.error("Script/Table API only supports single inputTopic/inputTopicSchema. topicInList.length="
                    + topicInList.length + " schemaSubjectInList.length=" + schemaSubjectInList.length);
            throw new DFPropertyValidationException("Script/Table API only supports single inputTopic/inputTopicSchema.");
        }

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

                    tableEnv.registerTableSource(topicInList[i], new Kafka010AvroTableSource(topicInList[i], properties));
                    for (String key : properties.stringPropertyNames()) {
                        LOG.debug("FLINK_PROPERTIES KEY:" + key + " VALUE:" + properties.getProperty(key));
                    }
                    LOG.debug("FLINK_TOPIC_IN = " + topicInList[i]);
                    LOG.debug("FLINK_TRANSCRIPT = " + transScript);
                }

                LOG.debug("FLINK_TOPIC_OUT = " + topicOut);

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
                    default:
                        break;
                }

                SchemaRegistryClient.addSchemaFromTableResult(SchemaRegistryHostPort, schemaSubjectOut, result);

                // delivered properties
                properties.setProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT, schemaSubjectOut);
                properties.setProperty(ConstantApp.PK_SCHEMA_ID_OUTPUT, SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT) + "");
                properties.setProperty(ConstantApp.PK_SCHEMA_STR_OUTPUT, SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT).toString());


                Kafka09AvroTableSink avro_sink =
                        new Kafka09AvroTableSink(topicOut, properties, new FlinkFixedPartitioner());
                result.writeToSink(avro_sink);

                flinkEnv.executeWithDFObj("DF_FLINK_A2A_" + engine.toUpperCase() + uuid, dfJob);

            } catch(Exception e) {
                e.printStackTrace();
                /*
                TODO
                Sometimes this is normal since we cancel from rest api through UI but the job submit in code
                We need distinguish this by checking if the task is removed from repo. If removed, it is not exception
                Or else, it is true exception.
                 */
                LOG.error(DFAPIMessage.logResponseMessage(9010, "jobId = " + dfJob.getId() +
                " exception - " + e.getCause()));
                // e.printStackTrace();
            }

        }, res -> {
            LOG.debug("BLOCKING CODE IS TERMINATE?FINISHED" + res.cause());
        });

        long timerID = vertx.setTimer(maxRunTime, id -> {
            // exec_flink.close();
            LOG.info(DFAPIMessage.logResponseMessage(1005, "id-" + dfJob.getId() + " flink_job_id-" + dfJob.getFlinkIDFromJobConfig()));
            mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", dfJob.getId()),
                    new JsonObject().put("$set", dfJob.toJson()), v -> {
                        if (v.failed()) {
                            LOG.error(DFAPIMessage.logResponseMessage(1001, dfJob.getId()));
                        }
                    }
            );
        });
    }

    /**
     * This method cancel a flink job by jobId through Flink rest API
     * Job may not exist or got exception. In this case, just delete it for now.
     *
     * @param jobID           The job ID to cancel for flink job
     * @param mongoClient     repo handler
     * @param mongoCOLLECTION collection to keep data
     * @param routingContext  response for rest client
     * @param restClient client to response
     */
    public static void cancelFlinkJob(String jobID, MongoClient mongoClient, String mongoCOLLECTION,
                                      RoutingContext routingContext, RestClient restClient) {
        String id = routingContext.request().getParam("id");
        System.out.println("jobId to cancel - " + jobID);

        if (jobID == null || jobID.trim().equalsIgnoreCase("")) {
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            final RestClientRequest postRestClientRequest = restClient.delete(ConstantApp.FLINK_REST_URL + "/" +
                            jobID + "/cancel", String.class,
                    portRestResponse -> {
                        LOG.debug("FLINK_SERVER_ACK: " + portRestResponse.statusMessage() + " "
                                + portRestResponse.statusCode());
                        if (portRestResponse.statusCode() == ConstantApp.STATUS_CODE_OK) {
                            // Once REST API forward is successful, delete the record to the local repository
                            mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
                                    ar -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .end(DFAPIMessage.getResponseMessage(1002, id)));
                            LOG.info(DFAPIMessage.logResponseMessage(1002, "CANCEL_FLINK_JOB " + id));
                        } else {
                            LOG.error(DFAPIMessage.logResponseMessage(9026, id));
                        }

                        portRestResponse.exceptionHandler(exception -> {
                            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                    .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                    .end(DFAPIMessage.getResponseMessage(9029));
                            LOG.error(DFAPIMessage.logResponseMessage(9029, id));
                        });
                    });

            postRestClientRequest.exceptionHandler(exception -> {
                // Still delete once exception happens
                mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
                        ar -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                .end(DFAPIMessage.getResponseMessage(9007)));
                LOG.info(DFAPIMessage.logResponseMessage(9012, id));
            });

            restClient.exceptionHandler(exception -> {
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                        .end(DFAPIMessage.getResponseMessage(9028));
                LOG.error(DFAPIMessage.logResponseMessage(9028, exception.getMessage()));
            });

            postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
            postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
            postRestClientRequest.end(DFAPIMessage.getResponseMessage(1002));
        }
    }

    public static void updateFlinkSQL (DFJobPOPJ dfJob, Vertx vertx, Integer maxRunTime,
                                       DFRemoteStreamEnvironment flinkEnv,
                                       String kafkaHostPort, String groupid,
                                       String inputTopic, String outputTopic,
                                       String transSql, MongoClient mongoClient, String mongoCOLLECTION,
                                       String jobManagerHostPort, RoutingContext routingContext,
                                       String SchemaRegistryHostPort, String schemSubject, String schemaSubjectOut,
                                       String sinkKeys, RestClient restClient) {

        final String id = routingContext.request().getParam("id");
        if(dfJob.getStatus().equalsIgnoreCase("RUNNING"))
        cancelFlinkJob(dfJob.getJobConfig().get(ConstantApp.PK_FLINK_SUBMIT_JOB_ID), mongoClient, mongoCOLLECTION,
                routingContext, restClient);

        // Submit Flink UDF
        if(dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_UDF.name()) {
            FlinkTransformProcessor.runFlinkJar(dfJob.getUdfUpload(), jobManagerHostPort);
        }

        // Submit Flink SQL A2A - Avro to Avro
        if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_SQLA2A.name()) {
            submitFlinkJobA2A(dfJob, vertx, maxRunTime, flinkEnv, kafkaHostPort,
                    SchemaRegistryHostPort, groupid, inputTopic, outputTopic, sinkKeys, transSql, schemSubject, schemaSubjectOut,
                    mongoClient, mongoCOLLECTION, "SQL_API");
        }

        // Submit Flink Table API Job
        if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_Script.name()) {
            submitFlinkJobA2A(dfJob, vertx, maxRunTime, flinkEnv, kafkaHostPort,
                    SchemaRegistryHostPort, groupid, inputTopic, outputTopic, sinkKeys, transSql, schemSubject, schemaSubjectOut,
                    mongoClient, mongoCOLLECTION, "TABLE_API");
        }

        mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id), // Select a unique document
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", dfJob.toJson()), v -> {
                    if (v.failed()) {
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9003));
                        LOG.error(DFAPIMessage.logResponseMessage(9003, id));
                    } else {
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .end(DFAPIMessage.getResponseMessage(1001));
                        LOG.info(DFAPIMessage.logResponseMessage(1001, id));
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
    @Deprecated
    public static void runFlinkJar (String jarFile, String jobManagerHostPort) {

        try {
            String runCMD = "run;-m;" + jobManagerHostPort + ";" + jarFile;
            //CliFrontend cli = new CliFrontend("conf/flink-conf.yaml");
            CliFrontend cli = new CliFrontend("conf");
            int retCode = cli.parseParameters(runCMD.split(";"));
            String respMsg = (retCode == 0)? "Flink job is submitted for Jar UDF at " :
                    "Flink job is failed to submit for Jar UDF at " + jarFile;
            LOG.info(DFAPIMessage.logResponseMessage(9013, "respMsg-" + respMsg));
        } catch (IllegalArgumentException ire) {
            LOG.error(DFAPIMessage.logResponseMessage(9013, "jarFile-" + jarFile));
        } catch (ProgramInvocationException t) {
            LOG.error(DFAPIMessage.logResponseMessage(9013, "jarFile-" + jarFile));
        } catch (Exception e) {
            LOG.error(DFAPIMessage.logResponseMessage(9013, "jarFile-" + jarFile));
        }
    }

    /**
     * This method first decode the REST GET request to DFJobPOPJ object. Then, it updates its job status and repack
     * for Kafka REST GET. After that, it forward the new GET to Flink API.
     * Once REST API forward is successful, response.
     *
     * @param routingContext This is the contect from REST API
     * @param restClient This is vertx non-blocking rest client used for forwarding
     * @param taskId This is the id used to look up status
     */
    public static void forwardGetAsGetOne(RoutingContext routingContext, RestClient restClient, String taskId, String jobId) {
        if(!jobId.isEmpty() || jobId != null) {
            // Create REST Client for Kafka Connect REST Forward
            final RestClientRequest postRestClientRequest =
                    restClient.get(ConstantApp.FLINK_REST_URL + "/" + jobId, String.class,
                            portRestResponse -> {
                                JsonObject jo = new JsonObject(portRestResponse.getBody());
                                JsonArray subTaskArray = jo.getJsonArray("vertices");
                                for (int i = 0; i < subTaskArray.size(); i++) {
                                    subTaskArray.getJsonObject(i)
                                            .put("subTaskId", subTaskArray.getJsonObject(i).getString("id"))
                                            .put("id", taskId + "_" + subTaskArray.getJsonObject(i).getString("id"))
                                            .put("jobId", jo.getString("jid"))
                                            .put("dfTaskState", HelpFunc.getTaskStatusFlink(new JSONObject(jo.toString())))
                                            .put("taskState", jo.getString("state"));
                                }
                                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                        .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                        .putHeader("X-Total-Count", subTaskArray.size() + "" )
                                        .end(Json.encodePrettily(subTaskArray.getList()));
                                LOG.info(DFAPIMessage.logResponseMessage(1024, taskId));

                                portRestResponse.exceptionHandler(exception -> {
                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .end(DFAPIMessage.getResponseMessage(9029));
                                    LOG.error(DFAPIMessage.logResponseMessage(9029, taskId));
                                });
                            });

            // Return a lost status when there is exception
            postRestClientRequest.exceptionHandler(exception -> {
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_OK)
                        //.setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                        .end(Json.encodePrettily(new JsonObject()
                                .put("id", taskId)
                                .put("jobId", jobId)
                                .put("state", ConstantApp.DF_STATUS.LOST.name())
                                .put("jobState", ConstantApp.DF_STATUS.LOST.name())
                                .put("subTask", new JsonArray().add("NULL"))));
                LOG.error(DFAPIMessage.logResponseMessage(9006, taskId));
            });

            restClient.exceptionHandler(exception -> {
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_OK)
                        //.setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                        .end(Json.encodePrettily(new JsonObject()
                                .put("id", taskId)
                                .put("jobId", jobId)
                                .put("state", ConstantApp.DF_STATUS.LOST.name())
                                .put("jobState", ConstantApp.DF_STATUS.LOST.name())
                                .put("subTask", new JsonArray().add("NULL"))));
                LOG.error(DFAPIMessage.logResponseMessage(9028, taskId));
            });

            postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
            postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
            postRestClientRequest.end();
        }
    }
}

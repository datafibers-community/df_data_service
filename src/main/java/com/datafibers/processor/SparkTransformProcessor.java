package com.datafibers.processor;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.DFAPIMessage;
import com.datafibers.util.HelpFunc;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.apache.log4j.Logger;

/**
 * For now, we communicate with Spark through Apache Livy
 */

public class SparkTransformProcessor {
    private static final Logger LOG = Logger.getLogger(SparkTransformProcessor.class);

    /**
     * forwardPostAsAddJar is a generic function to submit any spark jar to the livy.
     * This function is equal to the spark-submit. Submit status will refreshed in status thread separately.
     */
    public static void forwardPostAsAddJar(Vertx vertx, WebClient webClient, DFJobPOPJ dfJob, MongoClient mongo,
                                           String taskCollection, String sparkRestHost, int sparkRestPort,
                                           String sql) {
        // TODO to be implemented by livy batch api set
    }

    /**
     * forwardPostAsAddOne is a generic function to submit pyspark code taking sql statement to the livy.
     * This function will not response df ui. Since the UI is refreshed right way. Submit status will refreshed in status
     * thread separately.
     *
     * @param webClient vertx web client for rest
     * @param dfJob jd job object
     * @param mongo mongodb client
     * @param taskCollection mongo collection name to keep df tasks
     * @param sparkRestHost spark/livy rest hostname
     * @param sparkRestPort spark/livy rest port number
     * @param vertx used to initial blocking rest call for session status check
     * @param sql single sql statement of spark sql without ;
     */
    public static void forwardPostAsAddOne(Vertx vertx, WebClient webClient, DFJobPOPJ dfJob, MongoClient mongo,
                                           String taskCollection, String sparkRestHost, int sparkRestPort,
                                           String sql) {
        String taskId = dfJob.getId();
        // 1. Start a session using python spark, localhost:8998/sessions
        webClient.post(sparkRestPort, sparkRestHost, ConstantApp.LIVY_REST_URL_SESSIONS)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .sendJsonObject(new JsonObject().put("kind", "pyspark"), ar -> {
                    if (ar.succeeded()) {
                        String sessionId = ar.result().bodyAsJsonObject().getString("id");
                        dfJob.setJobConfig(ConstantApp.PK_LIVY_SESSION_ID, sessionId);

                        // 2. Check if session is in idle, http://localhost:8998/sessions/3
                        WorkerExecutor executor = vertx.createSharedWorkerExecutor(taskId,
                                ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
                        executor.executeBlocking(future -> {
                            String restURL = "http://" + sparkRestHost + ":" + sparkRestPort +
                                    ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + "/state";

                            HttpResponse<JsonNode> res;
                            // Keep checking session status until it is in idle
                            while(true) {
                                try {
                                    res = Unirest.get(restURL)
                                            .header(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                                                    ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                            .asJson();
                                    if(res.getBody().getObject().getString("state")
                                            .equalsIgnoreCase("idle")) break;
                                } catch (UnirestException e) {
                                    LOG.error(DFAPIMessage.logResponseMessage(9006,
                                            "exception - " + e.getCause()));
                                }
                            }

                            // 3. Once session is idle, submit sql code to the livy, localhost:8998/sessions/3/statements
                            // TODO support multiple sql statement separated by ;
                            String pySparkCode = "a = sqlContext.sql(\"" + sql + "\").collect()\n%json a";

                            webClient.post(sparkRestPort, sparkRestHost,
                                    ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + "/" +
                                            ConstantApp.LIVY_REST_URL_STATEMENTS)
                                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                                            ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                    .sendJsonObject(new JsonObject().put("code", pySparkCode),
                                            sar -> {
                                                if (ar.succeeded()) {
                                                    // 4. Get job submission status/result to keep in repo.
                                                    // Further status update comes from refresh status module in fibers
                                                    dfJob.setJobConfig(ConstantApp.PK_LIVY_STATEMENT_ID,
                                                                    ar.result().bodyAsJsonObject().getString("id"))
                                                            .setJobConfig(ConstantApp.PK_LIVY_STATEMENT_OUTPUT,
                                                                    ar.result().bodyAsJsonObject().getString("output"))
                                                            .setJobConfig(ConstantApp.PK_LIVY_STATEMENT_PROGRESS,
                                                                    ar.result().bodyAsJsonObject().getString("progress"))
                                                    .setStatus(ar.result().bodyAsJsonObject().getString("state")
                                                            .toUpperCase()); // Task status is statement status

                                                    mongo.updateCollection(taskCollection, new JsonObject().put("_id", taskId),
                                                            new JsonObject().put("$set", dfJob.toJson()), v -> {
                                                                if (v.failed()) {
                                                                    LOG.error(DFAPIMessage.logResponseMessage(1001,
                                                                            taskId + "error = " + v.cause()));
                                                                } else {
                                                                    LOG.info(DFAPIMessage.logResponseMessage(1005,
                                                                            taskId));
                                                                }
                                                            }
                                                    );
                                                }
                                            }
                                    );
                        }, res -> {});
                    } else {
                        LOG.error(DFAPIMessage.logResponseMessage(9010, taskId + " details - " + ar.cause()));
                    }
                });

    }

    /**
     * This method cancel a session by sessionId through livy rest API.
     * Job may not exist or got exception. In this case, just delete it for now.
     *
     * @param routingContext  response for rest client
     * @param webClient web client for rest
     * @param sparkRestHost spark/livy rest hostname
     * @param sparkRestPort spark/livy rest port number
     * @param mongoClient repo handler
     * @param mongoCOLLECTION collection to keep data
     * @param sessionId The livy session ID to cancel the job
     */
    public static void forwardDeleteAsCancelOne(RoutingContext routingContext, WebClient webClient,
                                                MongoClient mongoClient, String mongoCOLLECTION,
                                                String sparkRestHost, int sparkRestPort, String sessionId) {
        String id = routingContext.request().getParam("id");
        if (sessionId == null || sessionId.trim().isEmpty()) {
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            webClient.delete(sparkRestPort, sparkRestHost, ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId)
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .send(ar -> {
                                if (ar.succeeded()) {
                                    // Only if response is succeeded, delete from repo
                                    int response = (ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) ? 1002:9012;
                                    mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
                                            mar -> HelpFunc
                                                    .responseCorsHandleAddOn(routingContext.response())
                                                    .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                                    .end(DFAPIMessage.getResponseMessage(response, id)));
                                    LOG.info(DFAPIMessage.logResponseMessage(response, id));
                                } else { // TODO what's the return http code if session is timeout
                                    // If response is failed, repose df ui and still keep the task
                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                            .end(DFAPIMessage.getResponseMessage(9029));
                                    LOG.info(DFAPIMessage.logResponseMessage(9029, id));
                                }
                            }
                     );
        }
    }


    /**
     * This method is to update a task by creating livy session and resubmit the statement to livy.
     * In this case, we'll cancel the old session and submit update as new task.
     * When update task, we do not remove tasks from repo.
     *
     * @param routingContext  response for rest client
     * @param webClient vertx web client for rest
     * @param sparkRestHost flinbk rest hostname
     * @param sparkRestPort flink rest port number
     * @param mongoClient     repo handler
     * @param taskCollection collection to keep data
     */
    public static void forwardPutAsUpdateOne(Vertx vertx, RoutingContext routingContext, WebClient webClient,
                                             DFJobPOPJ dfJob, MongoClient mongoClient,
                                             String taskCollection, String sparkRestHost, int sparkRestPort) {

        // Submit new task using new session and statement
        forwardPostAsAddOne(
                vertx, webClient, dfJob,
                mongoClient, taskCollection,
                sparkRestHost, sparkRestPort,
                dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_SQL)
        );

        // Cancel the old session any way
        forwardDeleteAsCancelOne(
                routingContext, webClient,
                mongoClient, taskCollection,
                sparkRestHost, sparkRestPort,
                dfJob.getJobConfig().get(ConstantApp.PK_LIVY_SESSION_ID)
        );
    }

    /**
     * This method first decode the REST GET request to DFJobPOPJ object. Then, it updates its job status and repack
     * for REST GET. After that, it forward the GET to Livy API to get session and statement status including logging.
     * Once REST API forward is successful, response.
     *
     * @param routingContext This is the contect from REST API
     * @param webClient This is vertx non-blocking web client used for forwarding
     * @param flinkRestHost rest server host name
     * @param flinkRestPort rest server port number
     * @param taskId This is the id used to look up status
     * @param jobId transform job id
     */
    public static void forwardGetAsJobStatus(RoutingContext routingContext, WebClient webClient,
                                             String flinkRestHost, int flinkRestPort,
                                             String taskId, String jobId) {

        if (jobId == null || jobId.trim().isEmpty()) {
            LOG.warn(DFAPIMessage.logResponseMessage(9000, taskId));
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000, taskId,
                            "Cannot Get State Without JobId."));
        } else {
            webClient.get(flinkRestPort, flinkRestHost, ConstantApp.FLINK_REST_URL + "/" + jobId)
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .sendJsonObject(DFAPIMessage.getResponseJsonObj(1003),
                            ar -> {
                                if (ar.succeeded() && ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) {
                                    JsonObject jo = ar.result().bodyAsJsonObject();
                                    JsonArray subTaskArray = jo.getJsonArray("vertices");
                                    for (int i = 0; i < subTaskArray.size(); i++) {
                                        subTaskArray.getJsonObject(i)
                                                .put("subTaskId", subTaskArray.getJsonObject(i).getString("id"))
                                                .put("id", taskId + "_" + subTaskArray.getJsonObject(i).getString("id"))
                                                .put("jobId", jo.getString("jid"))
                                                .put("dfTaskState",
                                                        HelpFunc.getTaskStatusFlink(jo))
                                                .put("taskState", jo.getString("state"));
                                    }
                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .putHeader("X-Total-Count", subTaskArray.size() + "" )
                                            .end(Json.encodePrettily(subTaskArray.getList()));
                                    LOG.info(DFAPIMessage.logResponseMessage(1024, taskId));

                                } else {
                                    // If response is failed, repose df ui and still keep the task
                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                            .end(DFAPIMessage.getResponseMessage(9029, taskId,
                                                     "Cannot Found State for job " + jobId));
                                    LOG.info(DFAPIMessage.logResponseMessage(9029, taskId));
                                }
                            }
                    );
        }
    }
}

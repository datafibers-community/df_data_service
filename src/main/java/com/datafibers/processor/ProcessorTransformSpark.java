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
 * This is the utility class to communicate with Spark through Apache Livy Rest Service
 */

public class ProcessorTransformSpark {
    private static final Logger LOG = Logger.getLogger(ProcessorTransformSpark.class);

    /**
     * forwardPostAsAddJar is a generic function to submit any spark jar to the livy.
     * This function is equal to the spark-submit. Submit status will refreshed in status thread separately.
     */
    public static void forwardPostAsAddJar(Vertx vertx, WebClient webClient, DFJobPOPJ dfJob, MongoClient mongo,
                                           String taskCollection, String sparkRestHost, int sparkRestPort) {
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
     */
    public static void forwardPostAsAddOne(Vertx vertx, WebClient webClient, DFJobPOPJ dfJob, MongoClient mongo,
                                           String taskCollection, String sparkRestHost, int sparkRestPort) {
        String taskId = dfJob.getId();

        // Check all sessions submit a idle session. If all sessions are busy, create a new session
        webClient.get(sparkRestPort, sparkRestHost,
                ConstantApp.LIVY_REST_URL_SESSIONS)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .send(sar -> {
                            if (sar.succeeded()) {
                                String idleSessionId = "";
                                JsonArray sessionArray = sar.result().bodyAsJsonObject().getJsonArray("sessions");

                                for (int i = 0; i < sessionArray.size(); i++) {
                                    if(sessionArray.getJsonObject(i).getString("state")
                                            .equalsIgnoreCase("idle")) {
                                        idleSessionId = sessionArray.getJsonObject(i).getInteger("id").toString();
                                        break;
                                    }
                                }

                                if(idleSessionId.equalsIgnoreCase("")) {// No idle session, create one
                                    // 1. Start a session using python spark, post to localhost:8998/sessions
                                    webClient.post(sparkRestPort, sparkRestHost, ConstantApp.LIVY_REST_URL_SESSIONS)
                                            .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                                                    ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                            .sendJsonObject(new JsonObject().put("kind", "pyspark"), ar -> {
                                                if (ar.succeeded()) {
                                                    String newSessionId = ar.result().bodyAsJsonObject()
                                                            .getInteger("id").toString();
                                                    dfJob.setJobConfig(ConstantApp.PK_LIVY_SESSION_ID, newSessionId);
                                                    String restURL = "http://" + sparkRestHost + ":" + sparkRestPort +
                                                            ConstantApp.LIVY_REST_URL_SESSIONS + "/" + newSessionId +
                                                            "/state";

                                                    // 2. Wait until new session is in idle
                                                    WorkerExecutor executor = vertx.createSharedWorkerExecutor(taskId,
                                                            ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
                                                    executor.executeBlocking(future -> {

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
                                                                Thread.sleep(2000);
                                                            } catch (UnirestException|InterruptedException e) {
                                                                LOG.error(DFAPIMessage
                                                                        .logResponseMessage(9006,
                                                                                "exception - " + e.getCause()));
                                                            }
                                                        }

                                                        // 3. Once session is idle, submit sql code to the livy
                                                        addStatementToSession(
                                                                webClient, dfJob, sparkRestHost, sparkRestPort,
                                                                mongo, taskCollection, newSessionId
                                                        );
                                                    }, res -> {});
                                                } else {
                                                    LOG.error(DFAPIMessage.logResponseMessage(9010,
                                                            taskId + " Start new session failed with details - "
                                                                    + ar.cause()));
                                                }
                                            });

                                } else {
                                    addStatementToSession(
                                            webClient, dfJob, sparkRestHost, sparkRestPort,
                                            mongo, taskCollection, idleSessionId
                                    );
                                }
                            }
                });

    }

    /**
     * This method cancel a session by sessionId through livy rest API.
     * Job may not exist or got exception or timeout. In this case, just delete it for now.
     * If session Id is in idle, delete the session too.
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
            LOG.error(DFAPIMessage.logResponseMessage(9000, "sessionId is null in task " + id));
        } else {
            // Delete only if session is at idle since we'll share idle session first
            webClient.get(sparkRestPort, sparkRestHost,
                    ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + "/state")
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                            ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .send(sar -> {
                        if (sar.succeeded() &&
                                sar.result().statusCode() == ConstantApp.STATUS_CODE_OK &&
                                sar.result().bodyAsJsonObject().getString("state")
                                        .equalsIgnoreCase("idle")) {
                            webClient.delete(sparkRestPort, sparkRestHost,
                                    ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId)
                                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                                            ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                    .send(ar -> {
                                                if (ar.succeeded()) {
                                                    // Only if response is succeeded, delete from repo
                                                    int response =
                                                            (ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) ?
                                                                    1002 : 9012;
                                                    mongoClient.removeDocument(mongoCOLLECTION,
                                                            new JsonObject().put("_id", id),
                                                            mar -> HelpFunc
                                                                    .responseCorsHandleAddOn(routingContext.response())
                                                                    .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                                                    .end(DFAPIMessage.getResponseMessage(response, id)));
                                                    LOG.info(DFAPIMessage.logResponseMessage(response, id));
                                                } else {
                                                    // If response is failed, repose df ui and still keep the task
                                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                                            .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                                            .end(DFAPIMessage.getResponseMessage(9029));
                                                    LOG.info(DFAPIMessage.logResponseMessage(9029, id));
                                                }
                                            }
                                    );
                        } else { // session is busy/time out, delete the task from repo directly
                            mongoClient.removeDocument(mongoCOLLECTION,
                                    new JsonObject().put("_id", id),
                                    mar -> HelpFunc
                                            .responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .end(DFAPIMessage.getResponseMessage(1002, id)));
                            LOG.info(DFAPIMessage.logResponseMessage(1002, id));

                        }
                    });
        }
    }

    /**
     * This method is to update a task by creating livy session and resubmit the statement to livy.
     * Update should always use current session. If it is busy, the statement is in waiting state.
     * If the sesseion is closed or session id is not available, use new/idle session.
     * @param vertx  response for rest client
     * @param webClient vertx web client for rest
     * @param sparkRestHost flinbk rest hostname
     * @param sparkRestPort flink rest port number
     * @param mongoClient repo handler
     * @param taskCollection collection to keep data
     */
    public static void forwardPutAsUpdateOne(Vertx vertx, WebClient webClient,
                                             DFJobPOPJ dfJob, MongoClient mongoClient,
                                             String taskCollection, String sparkRestHost, int sparkRestPort) {

        // When session id is not available, use new/idle session to add new statement
        if(dfJob.getJobConfig() == null ||
                (dfJob.getJobConfig() != null && !dfJob.getJobConfig().containsKey(ConstantApp.PK_LIVY_STATEMENT_ID))) {
            // Submit new task using new/idle session and statement
            forwardPostAsAddOne(
                    vertx, webClient, dfJob,
                    mongoClient, taskCollection,
                    sparkRestHost, sparkRestPort
            );
        } else {
            String sessionId = dfJob.getJobConfig().get(ConstantApp.PK_LIVY_STATEMENT_ID);
            webClient.get(sparkRestPort, sparkRestHost,
                    ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + "/state")
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                            ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .send(sar -> { // If session is active, we always wait
                                if (sar.succeeded() && sar.result().statusCode() == ConstantApp.STATUS_CODE_OK) {
                                    addStatementToSession(
                                            webClient, dfJob,
                                            sparkRestHost, sparkRestPort, mongoClient, taskCollection, sessionId
                                    );
                                } else { // session is closed
                                    // Submit new task using new/idle session and statement
                                    forwardPostAsAddOne(
                                            vertx, webClient, dfJob,
                                            mongoClient, taskCollection,
                                            sparkRestHost, sparkRestPort
                                    );
                                }
                    });
        }
    }

    /**
     * This method first decode the REST GET request to DFJobPOPJ object. Then, it updates its job status and repack
     * for REST GET. After that, it forward the GET to Livy API to get session and statement status including logging.
     * Once REST API forward is successful, response. Right now, this is not being used by web ui.
     *
     * @param routingContext response for rest client
     * @param webClient This is vertx non-blocking web client used for forwarding
     * @param sparkRestHost flink rest hostname
     * @param sparkRestPort flink rest port number
     *
     */
    public static void forwardGetAsJobStatus(RoutingContext routingContext, WebClient webClient, DFJobPOPJ dfJob,
                                             String sparkRestHost, int sparkRestPort) {

        String sessionId = dfJob.getJobConfig().get(ConstantApp.PK_LIVY_SESSION_ID);
        String statementId = dfJob.getJobConfig().get(ConstantApp.PK_LIVY_STATEMENT_ID);
        String taskId = dfJob.getId();

        if (sessionId == null || sessionId.trim().isEmpty() || statementId == null || statementId.trim().isEmpty()) {
            LOG.warn(DFAPIMessage.logResponseMessage(9000, taskId));
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000, taskId,
                            "Cannot Get State Without Session Id/Statement Id."));
        } else {
            webClient.get(sparkRestPort, sparkRestHost,
                    ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + ConstantApp.LIVY_REST_URL_STATEMENTS)
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .send(ar -> {
                                if (ar.succeeded() && ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) {

                                    JsonObject jo = ar.result().bodyAsJsonObject();

                                    System.out.println("get status = " + jo);

                                    JsonArray subTaskArray = jo.getJsonArray("statements");
                                    JsonArray statusArray = new JsonArray();

                                    for (int i = 0; i < subTaskArray.size(); i++) {
                                        //Here, jobId = statementId, subTaskId = statementId
                                        statusArray.add(new JsonObject()
                                                .put("subTaskId", sessionId + "_" + subTaskArray.getJsonObject(i).getInteger("id"))
                                                .put("id", taskId + "_" + subTaskArray.getJsonObject(i).getInteger("id"))
                                                .put("jobId", sessionId)
                                                .put("dfTaskState",
                                                        HelpFunc.getTaskStatusSpark(subTaskArray.getJsonObject(i)))
                                                .put("taskState",
                                                        subTaskArray.getJsonObject(i).getString("state").toUpperCase())
                                                .put("statement", subTaskArray.getJsonObject(i).getString("code"))
                                                .put("output", HelpFunc.livyTableResultToRichText(subTaskArray.getJsonObject(i)))
                                        );
                                    }

                                    System.out.println("get status of array = " + statusArray);

                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .putHeader("X-Total-Count", statusArray.size() + "")
                                            .end(Json.encodePrettily(statusArray.getList()));
                                    LOG.info(DFAPIMessage.logResponseMessage(1024, taskId));

                                } else {
                                    // If response is failed, repose df ui and still keep the task
                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                            .end(DFAPIMessage.getResponseMessage(9029, taskId,
                                                    "Cannot Found State for job " + statementId));
                                    LOG.info(DFAPIMessage.logResponseMessage(9029, taskId));
                                }
                            }
                    );
        }
    }

    /**
     * Utilities to submit statement when session is in idle.
     *
     * @param webClient vertx web client for rest
     * @param dfJob jd job object
     * @param mongo mongodb client
     * @param taskCollection mongo collection name to keep df tasks
     * @param sparkRestHost spark/livy rest hostname
     * @param sparkRestPort spark/livy rest port number
     * @param sessionId livy session id
     */
    private static void addStatementToSession(WebClient webClient, DFJobPOPJ dfJob,
                                              String sparkRestHost, int sparkRestPort,
                                              MongoClient mongo, String taskCollection,
                                              String sessionId) {

        String sql = dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_SQL);
        // Support multiple sql statement separated by ; and comments by --
        String[] sqlList = HelpFunc.sqlCleaner(sql);
        // Here set stream back information. Later, the spark job status checker will upload the file to kafka
        Boolean streamBackFlag = false;
        String streamBackBasePath = "";
        if(dfJob.getConnectorConfig().containsKey(ConstantApp.PK_TRANSFORM_STREAM_BACK_FLAG) &&
                        dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_STREAM_BACK_FLAG).toString()
                        .contentEquals("true")) {
            streamBackFlag = true;
            streamBackBasePath = ConstantApp.TRANSFORM_STREAM_BACK_PATH + "/" + dfJob.getId() + "/";
            dfJob.getConnectorConfig().put(ConstantApp.PK_TRANSFORM_STREAM_BACK_PATH, streamBackBasePath); //set full path
        }

        String pySparkCode = HelpFunc.sqlToPySpark(sqlList, streamBackFlag, streamBackBasePath);

        webClient.post(sparkRestPort, sparkRestHost,
                ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId +
                        ConstantApp.LIVY_REST_URL_STATEMENTS)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                        ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .sendJsonObject(new JsonObject().put("code", pySparkCode),
                        sar -> {
                            if (sar.succeeded()) {
                                JsonObject response = sar.result().bodyAsJsonObject();
                                //System.out.println("Post returned = " + response);

                                // Get job submission status/result to keep in repo.
                                // Further status update comes from refresh status module in fibers
                                dfJob.setJobConfig(ConstantApp.PK_LIVY_SESSION_ID, sessionId)
                                        .setJobConfig(ConstantApp.PK_LIVY_STATEMENT_ID,
                                                response.getInteger("id").toString()
                                        )
                                        .setJobConfig(
                                                ConstantApp.PK_LIVY_STATEMENT_CODE,
                                                response.getString("code"));

                                mongo.updateCollection(taskCollection, new JsonObject().put("_id", dfJob.getId()),
                                        new JsonObject().put("$set", dfJob.toJson()), v -> {
                                            if (v.failed()) {
                                                LOG.error(DFAPIMessage.logResponseMessage(1001,
                                                        dfJob.getId() + "error = " + v.cause()));
                                            } else {
                                                LOG.info(DFAPIMessage.logResponseMessage(1005,
                                                        dfJob.getId()));
                                            }
                                        }
                                );
                            }
                        }
                );
    }

    /**
     * This is to get live job status for spark. Since spark now only has batch, we do not use it in web UI.
     * @param routingContext route context
     * @param webClient vertx web client for rest
     * @param dfJob jd job object
     * @param sparkRestHost spark/livy rest hostname
     * @param sparkRestPort spark/livy rest port number
     */
    @Deprecated
    public static void forwardGetAsJobStatusFromRepo(RoutingContext routingContext, WebClient webClient, DFJobPOPJ dfJob,
                                             String sparkRestHost, int sparkRestPort) {

        String sessionId = dfJob.getJobConfig().get(ConstantApp.PK_LIVY_SESSION_ID);
        String statementId = dfJob.getJobConfig().get(ConstantApp.PK_LIVY_STATEMENT_ID);
        String taskId = dfJob.getId();

        if (sessionId == null || sessionId.trim().isEmpty() || statementId == null || statementId.trim().isEmpty()) {
            LOG.warn(DFAPIMessage.logResponseMessage(9000, taskId));
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000, taskId,
                            "Cannot Get State Without Session Id/Statement Id."));
        } else {
            webClient.get(sparkRestPort, sparkRestHost,
                    ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + ConstantApp.LIVY_REST_URL_STATEMENTS)
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .send(ar -> {
                                if (ar.succeeded() && ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) {

                                    JsonObject jo = ar.result().bodyAsJsonObject();

                                    JsonArray subTaskArray = jo.getJsonArray("statements");
                                    JsonArray statusArray = new JsonArray();

                                    for (int i = 0; i < subTaskArray.size(); i++) {
                                        //Here, jobId = statementId, subTaskId = statementId
                                        statusArray.add(new JsonObject()
                                                        .put("subTaskId", sessionId + "_" + subTaskArray.getJsonObject(i).getInteger("id"))
                                                        .put("id", taskId + "_" + subTaskArray.getJsonObject(i).getInteger("id"))
                                                        .put("jobId", sessionId)
                                                        .put("dfTaskState",
                                                                HelpFunc.getTaskStatusSpark(subTaskArray.getJsonObject(i)))
                                                        .put("taskState",
                                                                subTaskArray.getJsonObject(i).getString("state").toUpperCase())
                                                        .put("statement", subTaskArray.getJsonObject(i).getString("code"))
                                                        .put("output",
                                                                "<table>\n" +
                                                                        "  <tr><th>Firstname</th><th>Lastname</th><th>Age</th></tr>\n" +
                                                                        "  <tr><td>Jill</td><td>Smith</td><td>50</td></tr>\n" +
                                                                        "  <tr><td>Eve</td><td>Jackson</td><td>94</td></tr>\n" +
                                                                        "  <tr><td>John</td><td>Doe</td><td>80</td></tr>\n" +
                                                                        "</table>")
                                                // HelpFunc.livyTableResultToArray(subTaskArray.getJsonObject(i)))
                                        );
                                    }

                                    System.out.println("get status of array = " + statusArray);

                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .putHeader("X-Total-Count", statusArray.size() + "")
                                            .end(Json.encodePrettily(statusArray.getList()));
                                    LOG.info(DFAPIMessage.logResponseMessage(1024, taskId));

                                } else {
                                    // If response is failed, repose df ui and still keep the task
                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                            .end(DFAPIMessage.getResponseMessage(9029, taskId,
                                                    "Cannot Found State for job " + statementId));
                                    LOG.info(DFAPIMessage.logResponseMessage(9029, taskId));
                                }
                            }
                    );
        }
    }


}

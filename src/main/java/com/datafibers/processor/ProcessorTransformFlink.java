package com.datafibers.processor;

import com.datafibers.util.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.apache.log4j.Logger;
import com.datafibers.model.DFJobPOPJ;

public class ProcessorTransformFlink {
    private static final Logger LOG = Logger.getLogger(ProcessorTransformFlink.class);

    /**
     * forwardPostAsSubmitJar is a generic function to submit specific jar file with proper configurations, such as jar para,
     * to the Flink Rest API. This is used for Flink SQL, UDF, and Table API submission with different client class.
     * This function will not response df ui. Since the UI is refreshed right way. Submit status will refreshed in status
     * thread separately.
     *
     * @param webClient vertx web client for rest
     * @param dfJob jd job object
     * @param mongo mongodb client
     * @param taskCollection mongo collection name to keep df tasks
     * @param flinkRestHost flinbk rest hostname
     * @param flinkRestPort flink rest port number
     * @param allowNonRestoredState
     * @param savepointPath
     * @param entryClass
     * @param parallelism number of jobs run in parallelism
     * @param programArgs parameters used by the jar files separated by " "
     */
    public static void forwardPostAsSubmitJar(WebClient webClient, DFJobPOPJ dfJob, MongoClient mongo,
                                      String taskCollection, String flinkRestHost, int flinkRestPort, String jarId,
                                      String allowNonRestoredState, String savepointPath, String entryClass,
                                      String parallelism, String programArgs) {
        String taskId = dfJob.getId();
        if (jarId.isEmpty()) {
            LOG.error(DFAPIMessage.logResponseMessage(9000, taskId));
        } else {
            // Search mongo to get the flink_jar_id
            webClient.post(flinkRestPort, flinkRestHost, ConstantApp.FLINK_REST_URL_JARS + "/" + jarId + "/run")
                    .addQueryParam("allowNonRestoredState", allowNonRestoredState)
                    .addQueryParam("savepointPath", savepointPath)
                    .addQueryParam("entry-class", entryClass)
                    .addQueryParam("parallelism", parallelism)
                    .addQueryParam("allowNonRestoredState", allowNonRestoredState)
                    .addQueryParam("program-args", programArgs)
                    .send(ar -> {
                        if (ar.succeeded()) {
                            String flinkJobId =  ar.result().bodyAsJsonObject()
                                            .getString(ConstantApp.FLINK_JOB_SUBMIT_RESPONSE_KEY);
                            String flinkError = ar.result().bodyAsJsonObject()
                                    .getString(ConstantApp.FLINK_JOB_ERROR_RESPONSE_KEY);

                            if(flinkJobId != null) {
                                dfJob.setFlinkIDToJobConfig(flinkJobId)
                                        .setStatus(ConstantApp.DF_STATUS.RUNNING.name());
                            }

                            LOG.debug("dfJob to Json = " + dfJob.toJson());

                            mongo.updateCollection(taskCollection, new JsonObject().put("_id", taskId),
                                    new JsonObject().put("$set", dfJob.toJson()), v -> {
                                        if (v.failed() || flinkJobId == null ) {
                                            LOG.error(DFAPIMessage.logResponseMessage(1001,
                                                    taskId + " has error " + flinkError));
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
        }
    }

    /**
     * This method cancel a flink job by jobId through Flink rest API
     * Job may not exist or got exception. In this case, just delete it for now.
     *
     * @param routingContext  response for rest client
     * @param webClient web client for rest
     * @param flinkRestHost flinbk rest hostname
     * @param flinkRestPort flink rest port number
     * @param mongoClient     repo handler
     * @param mongoCOLLECTION collection to keep data
     * @param jobID           The job ID to cancel for flink job
     */
    public static void forwardDeleteAsCancelJob(RoutingContext routingContext, WebClient webClient,
                                                MongoClient mongoClient, String mongoCOLLECTION,
                                                String flinkRestHost, int flinkRestPort, String jobID) {
        String id = routingContext.request().getParam("id");
        if (jobID == null || jobID.trim().isEmpty()) {
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            webClient.delete(flinkRestPort, flinkRestHost, ConstantApp.FLINK_REST_URL + "/" + jobID + "/cancel")
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .sendJsonObject(DFAPIMessage.getResponseJsonObj(1002),
                            ar -> {
                                if (ar.succeeded()) {
                                    // Only if response is succeeded, delete from repo
                                    int response = (ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) ? 1002:9012;
                                    mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
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
        }
    }


    /**
     * This method restart a flink job by cancel it then submit through Flink rest API
     * Wen restart job, we do not remove tasks from repo.
     *
     * @param routingContext  response for rest client
     * @param webClient vertx web client for rest
     * @param flinkRestHost flinbk rest hostname
     * @param flinkRestPort flink rest port number
     * @param mongoClient     repo handler
     * @param taskCollection collection to keep data
     * @param jobID           The job ID to cancel for flink job
     */
    public static void forwardPutAsRestartJob(RoutingContext routingContext, WebClient webClient,
                                              MongoClient mongoClient, String jarVersionCollection, String taskCollection,
                                              String flinkRestHost, int flinkRestPort, String jarId,
                                              String jobID, DFJobPOPJ dfJob,
                                              String allowNonRestoredState, String savepointPath, String entryClass,
                                              String parallelism, String programArgs) {
        String id = routingContext.request().getParam("id");
        if (jobID == null || jobID.trim().isEmpty()) {
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            webClient.delete(flinkRestPort, flinkRestHost, ConstantApp.FLINK_REST_URL + "/" + jobID + "/cancel")
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .sendJsonObject(DFAPIMessage.getResponseJsonObj(1002),
                            ar -> {
                                if (ar.succeeded()) {
                                    // If cancel response is succeeded, we'll submit the job
                                    int response = (ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) ? 1002:9012;
                                    LOG.info(DFAPIMessage.logResponseMessage(response, id));
                                    forwardPostAsSubmitJar(webClient,
                                            dfJob,
                                            mongoClient,
                                            taskCollection,
                                            flinkRestHost,
                                            flinkRestPort,
                                            jarId,
                                            allowNonRestoredState,
                                            savepointPath,
                                            entryClass,
                                            parallelism,
                                            programArgs);
                                } else {
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
     * This method first decode the REST GET request to DFJobPOPJ object. Then, it updates its job status and repack
     * for REST GET. After that, it forward the new GET to Flink API.
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

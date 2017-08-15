package com.datafibers.processor;

import com.datafibers.util.DFAPIMessage;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.hubrick.vertx.rest.MediaType;
import com.hubrick.vertx.rest.RestClient;
import com.hubrick.vertx.rest.RestClientRequest;
import org.json.JSONArray;
import org.json.JSONObject;

public class KafkaConnectProcessor {

    private static final Logger LOG = Logger.getLogger(KafkaConnectProcessor.class);
    
    public KafkaConnectProcessor(){}

    /**
     * This method first decode the REST GET request to DFJobPOPJ object. Then, it updates its job status and repack
     * for Kafka REST GET. After that, it forward the new GET to Kafka Connect.
     * Once REST API forward is successful, response.
     *
     * @param routingContext This is the contect from REST API
     * @param restClient This is vertx non-blocking rest client used for forwarding
     * @param taskId This is the id used to look up status
     */
    public static void forwardGetAsGetOne(RoutingContext routingContext, RestClient restClient, String taskId) {
        // Create REST Client for Kafka Connect REST Forward
        final RestClientRequest postRestClientRequest =
                restClient.get(ConstantApp.KAFKA_CONNECT_REST_URL + "/" + taskId + "/status", String.class,
                        portRestResponse -> {
                            JsonObject jo = new JsonObject(portRestResponse.getBody());
                            JsonArray subTaskArray = jo.getJsonArray("tasks");
                            for (int i = 0; i < subTaskArray.size(); i++) {
                                subTaskArray.getJsonObject(i)
                                        .put("subTaskId", subTaskArray.getJsonObject(i).getInteger("id"))
                                        .put("id", taskId)
                                        .put("jobId", taskId)
                                        .put("dfTaskState", HelpFunc.getTaskStatusKafka(new JSONObject(jo.toString())))
                                        .put("taskState", jo.getJsonObject("connector").getString("state"));
                            }

                            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                    .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                    .putHeader("X-Total-Count", subTaskArray.size() + "" )
                                    .end(Json.encodePrettily(subTaskArray.getList()));
                            LOG.info(DFAPIMessage.logResponseMessage(1023, taskId));
                        });

        postRestClientRequest.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_OK)
                    .end(DFAPIMessage.getResponseMessage(9006));
            LOG.error(DFAPIMessage.logResponseMessage(9006, taskId));
        });

        restClient.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_OK)
                    .end(DFAPIMessage.getResponseMessage(9028));
            LOG.error(DFAPIMessage.logResponseMessage(9028, taskId));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end();
    }

    /**
     * This method first decode the REST POST request to DFJobPOPJ object. Then, it updates its job status and repack
     * for Kafka REST POST. After that, it forward the new POST to Kafka Connect.
     * Once REST API forward is successful, update data to the local repository.
     *
     * @param routingContext This is the contect from REST API
     * @param restClient This is vertx non-blocking rest client used for forwarding
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param mongoCOLLECTION This is mongodb collection name
     * @param dfJobResponsed This is the response object return to rest client or ui or mongo insert
     */
    public static void forwardPOSTAsAddOne(RoutingContext routingContext, RestClient restClient, MongoClient mongoClient,
                                     String mongoCOLLECTION, DFJobPOPJ dfJobResponsed) {
        // Create REST Client for Kafka Connect REST Forward
        final RestClientRequest postRestClientRequest = restClient.post(ConstantApp.KAFKA_CONNECT_REST_URL, String.class,
                portRestResponse -> {
                    String rs = portRestResponse.getBody();
                    JsonObject jo = new JsonObject(rs);
                    LOG.debug("KAFKA_SERVER_ACK: " + portRestResponse.statusMessage() + " "
                            + portRestResponse.statusCode());

                    // Once REST API forward is successful, add the record to the local repository
                    mongoClient.insert(mongoCOLLECTION, dfJobResponsed.toJson(), r ->
                            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                    .setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                                    .end(Json.encodePrettily(dfJobResponsed)));
                    LOG.info(DFAPIMessage.logResponseMessage(1000, dfJobResponsed.getId()));
                });

        postRestClientRequest.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_OK)
                    .end(DFAPIMessage.getResponseMessage(9006));
            LOG.error(DFAPIMessage.logResponseMessage(9006, dfJobResponsed.getId()));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end(dfJobResponsed.toKafkaConnectJson().toString());

    }

    /**
     * This method first decode the REST PUT request to DFJobPOPJ object. Then, it updates its job status and repack
     * for Kafka REST PUT. After that, it forward the new POST to Kafka Connect.
     * Once REST API forward is successful, update data to the local repository.
     *
     * @param routingContext This is the contect from REST API
     * @param restClient This is vertx non-blocking rest client used for forwarding
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param mongoCOLLECTION This is mongodb collection name
     * @param dfJobResponsed This is the response object return to rest client or ui or mongo insert
     */
    public static void forwardPUTAsUpdateOne (RoutingContext routingContext, RestClient restClient, MongoClient mongoClient,
                                         String mongoCOLLECTION, DFJobPOPJ dfJobResponsed) {
        final String id = routingContext.request().getParam("id");
        LOG.info(DFAPIMessage.logResponseMessage(1016, id));

        final RestClientRequest postRestClientRequest =
                restClient.put(
                        ConstantApp.KAFKA_CONNECT_PLUGIN_CONFIG.
                                replace("CONNECTOR_NAME_PLACEHOLDER", dfJobResponsed.getConnectUid()),
                        String.class, portRestResponse -> {
                            LOG.debug("KAFKA_SERVER_ACK: " + portRestResponse.statusMessage() + " "
                                    + portRestResponse.statusCode());
                        });

        postRestClientRequest.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .end(DFAPIMessage.getResponseMessage(9021));
            LOG.error(DFAPIMessage.logResponseMessage(9021,
                    "UPDATE_IN_KAFKA_CONNECT_SERVER_FAILED"));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end(dfJobResponsed.mapToJsonString(dfJobResponsed.getConnectorConfig()));

        mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id), // Select a unique document
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", dfJobResponsed.toJson()), v -> {
                    if (v.failed()) {
                        routingContext.response()
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9003));
                        LOG.error(DFAPIMessage.logResponseMessage(9003, id));
                    } else {
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .end(DFAPIMessage.getResponseMessage(1000));
                        LOG.info(DFAPIMessage.logResponseMessage(1000, id));
                    }
                });
    }

    /**
     * This method first decode the REST DELETE request to DFJobPOPJ object. Then, it updates its job status and repack
     * for Kafka REST DELETE. After that, it forward the new DELETE to Kafka Connect.
     * Once REST API forward is successful, update data to the local repository.
     *
     * @param routingContext This is the contect from REST API
     * @param restClient This is vertx non-blocking rest client used for forwarding
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param mongoCOLLECTION This is mongodb collection name
     * @param dfJobResponsed This is the response object return to rest client or ui or mongo insert
     */
    public static void forwardDELETEAsDeleteOne (RoutingContext routingContext, RestClient restClient, MongoClient mongoClient,
                                                 String mongoCOLLECTION, DFJobPOPJ dfJobResponsed) {
        String id = routingContext.request().getParam("id");
        // Create REST Client for Kafka Connect REST Forward
        final RestClientRequest postRestClientRequest = restClient.delete(ConstantApp.KAFKA_CONNECT_REST_URL + "/" +
                        dfJobResponsed.getConnectUid(), String.class,
                portRestResponse -> {
                    LOG.debug("KAFKA_SERVER_ACK: " + portRestResponse.statusMessage() + " "
                            + portRestResponse.statusCode());
                    if(portRestResponse.statusCode() == ConstantApp.STATUS_CODE_OK_NO_CONTENT) {
                        // Once REST API forward is successful, delete the record to the local repository
                        mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
                                ar -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                        .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                        .end(DFAPIMessage.getResponseMessage(1002, id)));
                        LOG.info(DFAPIMessage.logResponseMessage(1002, "FOUND_CONNECT_NAME_IN_KAFKA_CONNECT"));
                    } else {
                        LOG.error(DFAPIMessage.logResponseMessage(9022, id));
                    }
                });

        postRestClientRequest.exceptionHandler(exception -> {

            // Once REST API forward is successful, delete the record to the local repository
            mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
                    ar -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .end(DFAPIMessage.getResponseMessage(9007)));
            LOG.info(DFAPIMessage.logResponseMessage(1002, "CANNOT_FIND_CONNECT_NAME_IN_KAFKA_CONNECT"));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end(DFAPIMessage.getResponseMessage(1002));
    }

    /**
     * This is new service poc
     */
    public static void DF_forwardPOSTAsAddOne(RoutingContext routingContext, RestClient restClient, MongoClient mongoClient,
                                              String mongoCOLLECTION, DFJobPOPJ dfJobResponsed) {
        // Create REST Client for Kafka Connect REST Forward
        final RestClientRequest postRestClientRequest = restClient.post(ConstantApp.KAFKA_CONNECT_REST_URL, String.class,
                portRestResponse -> {
                    String rs = portRestResponse.getBody();
                    JsonObject jo = new JsonObject(rs);
                    // Once REST API forward is successful, add the record to the local repository
                    /*mongoClient.insert(mongoCOLLECTION, dfJobResponsed.toJson(), r -> routingContext
                            .response().setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                            .putHeader("Access-Control-Allow-Origin", "*")
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                            .end(Json.encodePrettily(dfJobResponsed)));*/
                });

        postRestClientRequest.exceptionHandler(exception -> {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .putHeader("Access-Control-Allow-Origin", "*")
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(DFAPIMessage.getResponseMessage(9006));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end(dfJobResponsed.toKafkaConnectJson().toString());

    }

    /**
     * This new service poc
     */
    public static void DF_forwardPUTAsUpdateOne(RoutingContext routingContext, RestClient restClient, MongoClient mongoClient,
                                         String mongoCOLLECTION, DFJobPOPJ dfJobResponsed) {
        final String id = routingContext.request().getParam("id");
        LOG.info("connectorConfig has change. Will forward to Kafka Connect.");

        final RestClientRequest postRestClientRequest =
                restClient.put(
                        ConstantApp.KAFKA_CONNECT_PLUGIN_CONFIG.
                                replace("CONNECTOR_NAME_PLACEHOLDER", dfJobResponsed.getConnectUid()),
                        String.class, portRestResponse -> {
                        });

        postRestClientRequest.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .end(DFAPIMessage.getResponseMessage(9006));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end(dfJobResponsed.mapToJsonString(dfJobResponsed.getConnectorConfig()));

        mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id), // Select a unique document
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", dfJobResponsed.toJson()), v -> {
                    if (v.failed()) {
                        routingContext.response()
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9003));
                    } else {
                        HelpFunc.responseCorsHandleAddOn(routingContext.response()).end();
                    }
                });
    }
}

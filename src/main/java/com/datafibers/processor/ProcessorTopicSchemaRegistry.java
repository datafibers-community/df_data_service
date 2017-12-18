package com.datafibers.processor;

import com.datafibers.util.DFAPIMessage;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.net.ConnectException;
import io.vertx.ext.web.client.WebClient;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.log4j.Logger;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class ProcessorTopicSchemaRegistry {
    private static final Logger LOG = Logger.getLogger(ProcessorTopicSchemaRegistry.class);

    /**
     * Retrieve all subjects first; and then retrieve corresponding subject's schema information.
     * Here, we'll filter topic-value and topic-key subject since these are used by the kafka and CR.
     * These subject are not available until SourceRecord is available.
     * Use block rest client, but unblock using vertx worker.
     *
     * @param routingContext
     * @param schema_registry_host_and_port
     */
    public static void forwardGetAllSchemas(Vertx vertx, RoutingContext routingContext,
                                            String schema_registry_host_and_port) {
        StringBuffer returnString = new StringBuffer();
        WorkerExecutor executor = vertx.createSharedWorkerExecutor("forwardGetAllSchemas_pool_" + new ObjectId(),
                ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
        executor.executeBlocking(future -> {
            String restURI = "http://" + schema_registry_host_and_port + "/subjects";
            int status_code = ConstantApp.STATUS_CODE_OK;
            try {
                HttpResponse<String> res = Unirest
                        .get(restURI)
                        .header("accept", ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                        .asString();

                if (res == null) {
                    status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
                } else if (res.getStatus() != ConstantApp.STATUS_CODE_OK) {
                    status_code = res.getStatus();
                } else {
                    String subjects = res.getBody();
                    // ["Kafka-value","Kafka-key"]
                    LOG.debug("All subjects received are " + subjects);
                    StringBuffer strBuff = new StringBuffer();
                    int count = 0;
                    if (subjects.compareToIgnoreCase("[]") != 0) { // Has active subjects
                        for (String subject : subjects.substring(2, subjects.length() - 2).split("\",\"")) {
                            // If the subject is internal one, such as topic-key or topic-value
                            if(subject.contains("df_meta") || subject.contains("-value") || subject.contains("-key"))
                                continue;

                            HttpResponse<JsonNode> resSubject = Unirest
                                    .get(restURI + "/" + subject + "/versions/latest")
                                    .header("accept", ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                    .asJson();
                            if (resSubject == null) {
                                status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
                            } else if (resSubject.getStatus() != ConstantApp.STATUS_CODE_OK) {
                                status_code = resSubject.getStatus();
                            } else {
                                JSONObject jsonSchema = resSubject.getBody().getObject();
                                String compatibility =
                                        getCompatibilityOfSubject(schema_registry_host_and_port, subject);
                                if (compatibility == null || compatibility.isEmpty())
                                    compatibility = "NONE";
                                jsonSchema.put(ConstantApp.SCHEMA_REGISTRY_KEY_COMPATIBILITY, compatibility);
                                // Repack subject to id, id to schema id
                                jsonSchema.put("schemaId", jsonSchema.get("id"));
                                jsonSchema.put("id", subject);
                                String schema = jsonSchema.toString();
                                if (count == 0)
                                    strBuff.append("[");
                                count ++;
                                strBuff.append(schema).append(",");
                            }
                        }
                        if (count > 0)
                            returnString.append(strBuff.toString().substring(0, strBuff.toString().length() - 1) + "]");
                        //LOG.debug("returnString: " + returnString.toString());
                    }
                }
            } catch (JSONException | UnirestException e) {
                LOG.error(DFAPIMessage.logResponseMessage(9027, " exception -" + e.getCause()));
                status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
            }
            future.complete(status_code);
        }, res -> {
            Object result = HelpFunc.coalesce(res.result(), ConstantApp.STATUS_CODE_BAD_REQUEST);
            try {
                if (returnString == null || returnString.toString().isEmpty()) {
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(Integer.parseInt(result.toString()))
                            .putHeader("X-Total-Count", "0")
                            .end("[]");

                } else {
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(Integer.parseInt(result.toString()))
                            .putHeader("X-Total-Count", new JSONArray(returnString.toString()).length() + "")
                            .end(HelpFunc.stringToJsonFormat(
                                    HelpFunc.sortJsonArray(routingContext,
                                            new JSONArray(returnString.toString())
                                    ).toString())
                            );
                }
            } catch (JSONException je) {
                LOG.error(DFAPIMessage.logResponseMessage(9027, " exception - " + je.getCause()));
            }
            executor.close();
        });
    }

    /**
     * Retrieve the specified subject's schema information. Use unblock rest client.
     *
     * @param routingContext
     * @param webClient
     * @param schemaRegistryRestHost
     * @param schemaRegistryRestPort
     */
    public static void forwardGetOneSchema(RoutingContext routingContext, WebClient webClient,
                                           String schemaRegistryRestHost, int schemaRegistryRestPort) {
        final String subject = routingContext.request().getParam("id");
        webClient.get(schemaRegistryRestPort, schemaRegistryRestHost,
                ConstantApp.SR_REST_URL_SUBJECTS + "/" + subject + ConstantApp.SR_REST_URL_VERSIONS + "/latest")
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                .send(ar -> {
                    if (ar.succeeded() && (ar.result().statusCode() == ConstantApp.STATUS_CODE_OK)) {
                        // get compatibility of schema
                        JsonObject schema = ar.result().bodyAsJsonObject();
                        webClient.get(schemaRegistryRestPort, schemaRegistryRestHost,
                                ConstantApp.SR_REST_URL_CONFIG + "/" + subject)
                                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                                .send(arc -> {
                                        JsonObject res = arc.result().bodyAsJsonObject();
                                        // When failed to get compatibility, return NONE
                                        String compatibility =
                                                res.containsKey(ConstantApp.SCHEMA_REGISTRY_KEY_COMPATIBILITY_LEVEL)?
                                                res.getString(ConstantApp.SCHEMA_REGISTRY_KEY_COMPATIBILITY_LEVEL):
                                                "NONE";

                                        schema.put(ConstantApp.SCHEMA_REGISTRY_KEY_COMPATIBILITY, compatibility);

                                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                                .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                                .end(Json.encodePrettily(schema));
                                        LOG.info(DFAPIMessage.logResponseMessage(1030, subject));
                                });
                    } else {
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9041, subject));
                        LOG.error(DFAPIMessage.logResponseMessage(9041, subject));
                    }
                });
    }

    /**
     * Add one schema to schema registry with non-blocking rest client. Add schema and update schema have the same API
     * specification only difference is to packing subject and compatibility from different field.
     *
     * @param routingContext This is the connect from REST API
     * @param webClient This is non-blocking rest client used for forwarding
     * @param schemaRegistryRestHost Schema Registry Rest Host
     * @param schemaRegistryRestPort Schema Registry Rest Port
     */
    public static void forwardAddOneSchema(RoutingContext routingContext, WebClient webClient,
                                           String schemaRegistryRestHost, int schemaRegistryRestPort) {

        addOneSchemaCommon(routingContext, webClient,
                schemaRegistryRestHost, schemaRegistryRestPort,
                "Schema is created.", 1025,
                "Schema creation is failed", 9039
        );

    }

    /**
     * Update one schema including compatibility to schema registry with non-blocking rest client
     *
     * @param routingContext This is the connect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param schemaRegistryRestHost Schema Registry Rest Host
     * @param schemaRegistryRestPort Schema Registry Rest Port
     */
    public static void forwardUpdateOneSchema(RoutingContext routingContext, WebClient webClient,
                                              String schemaRegistryRestHost, int schemaRegistryRestPort) {

        addOneSchemaCommon(routingContext, webClient,
                schemaRegistryRestHost, schemaRegistryRestPort,
                "Schema is updated.", 1017,
                "Schema update is failed", 9023
        );
    }

    /**
     * This is commonly used utility
     *
     * @param routingContext This is the connect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param schemaRegistryRestHost Schema Registry Rest Host
     * @param schemaRegistryRestPort Schema Registry Rest Port
     * @param successMsg Message to response when succeeded
     * @param successCode Status code to response when succeeded
     * @param errorMsg Message to response when failed
     * @param errorCode Status code to response when failed
     */
    public static void addOneSchemaCommon(RoutingContext routingContext, WebClient webClient,
                                          String schemaRegistryRestHost, int schemaRegistryRestPort,
                                          String successMsg, int successCode, String errorMsg, int errorCode) {

        JsonObject jsonObj = routingContext.getBodyAsJson();
        JsonObject schemaObj = jsonObj.getJsonObject(ConstantApp.SCHEMA_REGISTRY_KEY_SCHEMA);

        if(!jsonObj.containsKey("id") && !jsonObj.containsKey(ConstantApp.SCHEMA_REGISTRY_KEY_SUBJECT))
            LOG.error(DFAPIMessage.logResponseMessage(9040, "Subject of Schema is missing."));

        // get subject from id (web ui assigned) and assign it to subject
        String subject = jsonObj.containsKey("id")? jsonObj.getString("id"):
                jsonObj.getString(ConstantApp.SCHEMA_REGISTRY_KEY_SUBJECT);

        // Set schema name from subject if it does not has name or empty
        if(!schemaObj.containsKey("name") || schemaObj.getString("name").isEmpty()) {
            schemaObj.put("name", subject);
            jsonObj.put(ConstantApp.SCHEMA_REGISTRY_KEY_SCHEMA, schemaObj);
        }

        String compatibility = jsonObj.containsKey(ConstantApp.SCHEMA_REGISTRY_KEY_COMPATIBILITY) ?
                jsonObj.getString(ConstantApp.SCHEMA_REGISTRY_KEY_COMPATIBILITY) : "NONE";

        webClient.post(schemaRegistryRestPort, schemaRegistryRestHost,
                ConstantApp.SR_REST_URL_SUBJECTS + "/" + subject + ConstantApp.SR_REST_URL_VERSIONS)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                .sendJsonObject( new JsonObject()
                                .put(ConstantApp.SCHEMA_REGISTRY_KEY_SCHEMA, schemaObj.toString()),
                        // Must toString above according SR API spec.
                        ar -> {
                            if (ar.succeeded()) {
                                LOG.info(DFAPIMessage.logResponseMessage(successCode, subject + "-SCHEMA"));
                                // Once successful, we will update schema compatibility
                                webClient.put(schemaRegistryRestPort, schemaRegistryRestHost,
                                        ConstantApp.SR_REST_URL_CONFIG + "/" + subject)
                                        .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                                                ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                                        .sendJsonObject(new JsonObject()
                                                .put(ConstantApp.SCHEMA_REGISTRY_KEY_COMPATIBILITY, compatibility),
                                                arc -> {
                                                    if (arc.succeeded()) {
                                                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                                                .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                                                .end(Json.encodePrettily(jsonObj));
                                                        LOG.info(DFAPIMessage.logResponseMessage(1017,
                                                                successMsg + "-COMPATIBILITY"));
                                                    } else {
                                                        // If response is failed, repose df ui and still keep the task
                                                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                                                .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                                                .end(DFAPIMessage.getResponseMessage(errorCode,
                                                                        subject, errorMsg + "-COMPATIBILITY"));
                                                        LOG.info(DFAPIMessage.logResponseMessage(errorCode,
                                                                subject + "-COMPATIBILITY"));
                                                    }
                                                }
                                        );
                            } else {
                                // If response is failed, repose df ui and still keep the task
                                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                        .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                        .end(DFAPIMessage.getResponseMessage(errorCode, subject,
                                                errorMsg + "-SCHEMA"));
                                LOG.info(DFAPIMessage.logResponseMessage(errorCode, subject  + "-SCHEMA"));
                            }
                        }
                );
    }

    /**
     * This method first decode the REST DELETE request to get schema subject. Then, it forward the DELETE to
     * Schema Registry.
     *
     * @param routingContext This is the connect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param schemaRegistryRestHost Schema Registry Rest Host
     * @param schemaRegistryRestPort Schema Registry Rest Port
     */
    public static void forwardDELETEAsDeleteOne (RoutingContext routingContext, WebClient webClient,
                                                 String schemaRegistryRestHost, int schemaRegistryRestPort) {

        String subject = routingContext.request().getParam("id");
        // Create REST Client for Kafka Connect REST Forward
        webClient.delete(schemaRegistryRestPort, schemaRegistryRestHost,
                ConstantApp.SR_REST_URL_SUBJECTS + "/" + subject)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                .sendJsonObject(DFAPIMessage.getResponseJsonObj(1026),
                        ar -> {
                            if (ar.succeeded() &&
                                    ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) {
                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .end(DFAPIMessage.getResponseMessage(1026, subject));
                                    LOG.info(DFAPIMessage.logResponseMessage(1026,
                                            "subject = " + subject));
                            } else {
                                // If response is failed, repose df ui and still keep the task
                                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                        .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                        .end(DFAPIMessage.getResponseMessage(9029, subject,
                                                "Schema Subject DELETE Failed"));
                                LOG.info(DFAPIMessage.logResponseMessage(9029, subject + "-"
                                        + ar.cause().getMessage()
                                ));
                            }
                        }
                );
    }

    /**
     * Get schema compatibility from schema registry.
     *
     * @param schemaUri
     * @param schemaSubject
     * @return
     * @throws ConnectException
     */
    public static String getCompatibilityOfSubject(String schemaUri, String schemaSubject) {
        String compatibility = null;

        String fullUrl = String.format("http://%s/config/%s", schemaUri, schemaSubject);
        HttpResponse<String> res = null;

        try {
            res = Unirest.get(fullUrl).header("accept", "application/vnd.schemaregistry.v1+json").asString();
            LOG.debug("Subject:" + schemaSubject + " " + res.getBody());
            if (res.getBody() != null) {
                if (res.getBody().indexOf("40401") > 0) {
                } else {
                    JSONObject jason = new JSONObject(res.getBody().toString());
                    // This attribute is different from confluent doc. TODo check update later
                    compatibility = jason.getString(ConstantApp.SCHEMA_REGISTRY_KEY_COMPATIBILITY_LEVEL);
                }
            }
        } catch (UnirestException e) {
            LOG.error(DFAPIMessage.logResponseMessage(9006, "exception - " + e.getCause()));
        }

        return compatibility;
    }

}

package com.datafibers.processor;

import com.datafibers.util.DFAPIMessage;
import com.datafibers.util.DFMediaType;
import com.hubrick.vertx.rest.MediaType;
import com.hubrick.vertx.rest.RestClientRequest;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.RoutingContext;
import java.net.ConnectException;
import java.util.Arrays;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.log4j.Logger;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.hubrick.vertx.rest.RestClient;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class SchemaRegisterProcessor { // TODO @Schubert add proper Log.info or error with proper format and error code
    private static final Logger LOG = Logger.getLogger(SchemaRegisterProcessor.class);

    /**
     * Retrieve all subjects first; and then retrieve corresponding subject's schema information.
     * Here, we'll filter topic-value and topic-key subject since these are used by the kafka and CR.
     * These subject are not available until SourceRecord is available.
     * Use block rest client, but unblock using vertx worker.
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
                            // If the subject is internal one, such as topic-key or topic-value or df_meta
                            if(subject.equalsIgnoreCase("df_meta") ||
                                    subject.contains("-value") ||
                                    subject.contains("-key")) continue;

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
                                jsonSchema.put(ConstantApp.COMPATIBILITY, compatibility);
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
                        LOG.debug("returnString: " + returnString.toString());
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
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(Integer.parseInt(result.toString()))
                        .putHeader("X-Total-Count", new JSONArray(returnString.toString()).length() + "")
                        .end(HelpFunc.stringToJsonFormat(
                                HelpFunc.sortJsonArray(routingContext,
                                        new JSONArray(returnString.toString())).toString()));
            } catch (JSONException je) {
                LOG.error(DFAPIMessage.logResponseMessage(9027, " exception -" + je.getCause()));
            }
            executor.close();
        });
    }

    /**
     * Retrieve the specified subject's schema information. Use block rest client, but unblock using vertx worker.
     *
     * @param vertx
     * @param routingContext
     * @param schema_registry_host_and_port
     */
    public static void forwardGetOneSchema(Vertx vertx, RoutingContext routingContext,
                                           String schema_registry_host_and_port) {
        LOG.debug("SchemaRegisterProcessor.forwardGetOneSchema is called.");

        final String subject = routingContext.request().getParam("id");
        StringBuffer returnString = new StringBuffer();

        WorkerExecutor executor = vertx.createSharedWorkerExecutor("getOneSchema_pool_" + subject,
                ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
        executor.executeBlocking(future -> {
            String restURI = "http://" + schema_registry_host_and_port + "/subjects/" + subject + "/versions/latest";
            int status_code = ConstantApp.STATUS_CODE_OK;

            if (subject == null) {
                status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
            } else {
                try {
                    HttpResponse<String> res = Unirest.get(restURI)
                            .header("accept", ConstantApp.AVRO_REGISTRY_CONTENT_TYPE).asString();

                    if (res == null) {
                        status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
                    } else if (res.getStatus() != ConstantApp.STATUS_CODE_OK) {
                        status_code = res.getStatus();
                    } else {
                        JSONObject json = new JSONObject(res.getBody());

                        // Get the subject's compatibility
                        String compatibility = getCompatibilityOfSubject(schema_registry_host_and_port, subject);
                        if (compatibility != null && !compatibility.isEmpty()) {
                            json.put(ConstantApp.COMPATIBILITY, compatibility);
                        }

                        returnString.append(json.toString());
                        status_code = ConstantApp.STATUS_CODE_OK;
                    }
                } catch (JSONException | UnirestException e) {
                    LOG.error("SchemaRegisterProcessor - forwardGetOneSchema() - Exception message: " + e.getCause());
                    status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
                }
            }
            future.complete(status_code);
        }, res -> {
            Object result = HelpFunc.coalesce(res.result(), ConstantApp.STATUS_CODE_BAD_REQUEST);
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(Integer.parseInt(result.toString()))
                    .end(HelpFunc.stringToJsonFormat(returnString.toString()));
            executor.close();
        });
    }

    /**
     * Add one schema to schema registry with non-blocking rest client
     * @param routingContext
     * @param schema_registry_host_and_port
     */
    public static void forwardAddOneSchema(RoutingContext routingContext, RestClient rc_schema,
                                           String schema_registry_host_and_port) {

        JSONObject schema;
        String subject;
        String compatibility;
        String restURI;

        JSONObject jsonObj = new JSONObject(routingContext.getBodyAsString());
        schema = jsonObj.getJSONObject(ConstantApp.SCHEMA);
        // get subject from id and assign it to subject
        subject = jsonObj.getString("id");
        jsonObj.put(ConstantApp.SUBJECT, subject);
        // Set schema name from subject if it does not has name or empty
        if(!schema.has("name")) {
            schema.put("name", subject);
        } else if (schema.getString("name").isEmpty()) {
            schema.put("name", subject);
        }
        compatibility = jsonObj.optString(ConstantApp.COMPATIBILITY);
        LOG.debug("Schema|subject|compatibility: " + schema.toString() + "|" + subject + "|" + compatibility);

        restURI = "http://" + schema_registry_host_and_port + "/subjects/" + subject + "/versions";

        // Add the new schema
        final RestClientRequest postRestClientRequest = rc_schema.post(restURI, String.class,
                portRestResponse -> {
                    String rs = portRestResponse.getBody();
                    if (rs != null) {
                        LOG.info("Add schema status code " + portRestResponse.statusCode());

                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                .end(DFAPIMessage.logResponseMessage(1025, "schema - " + subject + " is created"));
                    }
                }
        );

        postRestClientRequest.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .end(DFAPIMessage.getResponseMessage(9006));
            LOG.error(DFAPIMessage.logResponseMessage(9006, exception.toString()));

        });

        rc_schema.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9028));
            LOG.error(DFAPIMessage.logResponseMessage(9028, exception.getMessage()));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMA_REGISTRY_JSON));
        JSONObject object = new JSONObject().put("schema", schema.toString());
        LOG.debug("Schema object.toString(): " + object.toString());
        postRestClientRequest.end(object.toString());

        // Set compatibility to the subject
        if (compatibility != null && compatibility.trim().length() > 0) {
            restURI = "http://" + schema_registry_host_and_port + "/config/" + subject;
            final RestClientRequest postRestClientRequest2 = rc_schema.put(restURI, portRestResponse -> {
                LOG.info("Update Config Compatibility status code " + portRestResponse.statusCode());
                if (routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_OK) {
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .end(DFAPIMessage.logResponseMessage(1025,
                                    "schema is created with proper compatibility "));
                }
            });

            postRestClientRequest2.exceptionHandler(exception -> {
                if (routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_CONFLICT
                        && routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_OK) {
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                            .end(DFAPIMessage.getResponseMessage(9006));
                    LOG.error(DFAPIMessage.logResponseMessage(9006, exception.toString()));
                }
            });

            rc_schema.exceptionHandler(exception -> {
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                        .end(DFAPIMessage.getResponseMessage(9028));
                LOG.error(DFAPIMessage.logResponseMessage(9028, exception.getMessage()));
            });

            postRestClientRequest2.setContentType(MediaType.APPLICATION_JSON);
            postRestClientRequest2.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMA_REGISTRY_JSON));
            JSONObject jsonToBeSubmitted = new JSONObject().put(ConstantApp.COMPATIBILITY, compatibility);
            LOG.debug("Compatibility object2.toString(): " + jsonToBeSubmitted.toString());
            postRestClientRequest2.end(jsonToBeSubmitted.toString());
        }

    }

    /**
     * Update one schema to schema registry with non-blocking rest client
     * @param routingContext
     * @param schema_registry_host_and_port
     */
    public static void forwardUpdateOneSchema(RoutingContext routingContext, RestClient rc_schema,
                                           String schema_registry_host_and_port) {
        JSONObject schema;
        String subject;
        String compatibility;
        String restURI;
        JSONObject schema1;
        JSONObject jsonForSubmit;

        String formInfo = routingContext.getBodyAsString();
        LOG.debug("Received the body is: " + formInfo);

        JSONObject jsonObj = new JSONObject(formInfo);

        try {
            schema = jsonObj.getJSONObject(ConstantApp.SCHEMA);
            schema1 = new JSONObject(schema.toString()); // TODO this is redundant?
            LOG.debug("=== schema is: " + schema1.toString());
        } catch (Exception ex) {
            schema1 = new JSONObject().put("type", jsonObj.getString(ConstantApp.SCHEMA));
            LOG.debug("=== schema with no key is: " + schema1.toString());
        }

        subject = jsonObj.getString(ConstantApp.SUBJECT);
        compatibility = jsonObj.optString(ConstantApp.COMPATIBILITY);
        LOG.debug("subject|compatibility: " + subject + "|" + compatibility);

        restURI = "http://" + schema_registry_host_and_port + "/subjects/" + subject + "/versions";

        final RestClientRequest postRestClientRequest = rc_schema.post(restURI, String.class,
                portRestResponse -> {
                    String rs = portRestResponse.getBody();
                    if (rs != null) {
                        LOG.info("Update schema status code: " + portRestResponse.statusCode());
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                .end(DFAPIMessage.getResponseMessage(1017));
                    }
                }
        );

        postRestClientRequest.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .end(DFAPIMessage.getResponseMessage(9023));
            DFAPIMessage.logResponseMessage(9023, "UPDATE_SCHEMA_FAILED");
        });

        rc_schema.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9028));
            LOG.error(DFAPIMessage.logResponseMessage(9028, exception.getMessage()));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMA_REGISTRY_JSON));
        jsonForSubmit = new JSONObject().put("schema", schema1.toString());
        LOG.debug("Schema send to update is: " + jsonForSubmit.toString());

        postRestClientRequest.end(jsonForSubmit.toString());

        // Set compatibility to the subject
        if (compatibility != null && compatibility.trim().length() > 0) {
            restURI = "http://" + schema_registry_host_and_port + "/config/" + subject;
            final RestClientRequest postRestClientRequest2 = rc_schema.put(restURI, portRestResponse -> {
                if (routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_OK) {
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .end(DFAPIMessage.logResponseMessage(1017, "SCHEMA_UPDATE"));
                    LOG.info(DFAPIMessage.logResponseMessage(1017, "SCHEMA_UPDATE"));
                }
            });

            postRestClientRequest2.exceptionHandler(exception -> {
                if (routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_CONFLICT
                        && routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_OK) {
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                            .end(DFAPIMessage.getResponseMessage(9023));
                    LOG.error(DFAPIMessage.logResponseMessage(9023, "SCHEMA_UPDATE"));
                }
            });

            rc_schema.exceptionHandler(exception -> {
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                        .end(DFAPIMessage.getResponseMessage(9028));
                LOG.error(DFAPIMessage.logResponseMessage(9028, exception.getMessage()));
            });

            postRestClientRequest2.setContentType(MediaType.APPLICATION_JSON);
            postRestClientRequest2.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMA_REGISTRY_JSON));

            JSONObject jsonToBeSubmitted = new JSONObject().put(ConstantApp.COMPATIBILITY, compatibility);
            postRestClientRequest2.end(jsonToBeSubmitted.toString());
        }
    }

    /**
     * This method first decode the REST DELETE request to DFJobPOPJ object. Then, it updates its job status and repack
     * for SR REST DELETE. After that, it forward the new DELETE to Schema Registry.
     *
     * @param routingContext This is the contect from REST API
     * @param rc_schema This is vertx non-blocking rest client used for forwarding
     * @param schema_registry_host_and_port Schema Registry Rest
     */
    public static void forwardDELETEAsDeleteOne (RoutingContext routingContext, RestClient rc_schema,
                                                 String schema_registry_host_and_port) {
        final String subject = routingContext.request().getParam("id");
        if(subject == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, subject));
        } else {
            String restURI = "http://" + schema_registry_host_and_port + "/subjects/" + subject;
            // Create REST Client for Kafka Connect REST Forward
            final RestClientRequest postRestClientRequest =
                    rc_schema.delete(restURI, String.class, portRestResponse -> {
                        if(portRestResponse.statusCode() == ConstantApp.STATUS_CODE_OK) {
                            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                    .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                    .end(DFAPIMessage.getResponseMessage(1026, subject));
                            LOG.info(DFAPIMessage.logResponseMessage(1026, "subject = " + subject));
                        } else {
                            LOG.error(DFAPIMessage.logResponseMessage(9022, "subject = " + subject));
                        }

                        portRestResponse.exceptionHandler(exception -> {
                            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                    .end(DFAPIMessage.getResponseMessage(9029));
                            LOG.error(DFAPIMessage.logResponseMessage(9029, "subject = " + subject));
                        });
                    });

            postRestClientRequest.exceptionHandler(exception -> {
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                        .end(DFAPIMessage.getResponseMessage(9029));
                LOG.info(DFAPIMessage.logResponseMessage(9029, "subject = " + subject + exception.toString()));
            });

            rc_schema.exceptionHandler(exception -> {
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                        .end(DFAPIMessage.getResponseMessage(9028));
                LOG.error(DFAPIMessage.logResponseMessage(9028, exception.getMessage()));
            });

            postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
            postRestClientRequest.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMA_REGISTRY_JSON));
            postRestClientRequest.end(DFAPIMessage.getResponseMessage(1026));
        }
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
            LOG.debug("getCompatibilityOfSubject res.getBody(): " + res.getBody());
            if (res.getBody() != null) {
                if (res.getBody().indexOf("40401") > 0) {
                } else {
                    JSONObject jason = new JSONObject(res.getBody().toString());
                    compatibility = jason.getString(ConstantApp.COMPATIBILITY_LEVEL);
                }
            }
        } catch (UnirestException e) {
            LOG.error(DFAPIMessage.logResponseMessage(9006, "exception - " + e.getCause()));
        }

        return compatibility;
    }

}

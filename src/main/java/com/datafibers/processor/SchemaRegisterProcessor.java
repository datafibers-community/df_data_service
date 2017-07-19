package com.datafibers.processor;

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
import org.json.JSONObject;
import org.apache.log4j.Logger;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.hubrick.vertx.rest.RestClient;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class SchemaRegisterProcessor {
    private static final Logger LOG = Logger.getLogger(SchemaRegisterProcessor.class);

    /**
     * Retrieve all subjects first; and then retrieve corresponding subject's schema information
     * Use block rest client, but unblock using vertx worker.
     * @param routingContext
     * @param schema_registry_host_and_port
     */
    public static void forwardGetAllSchemas(Vertx vertx, RoutingContext routingContext,
                                            String schema_registry_host_and_port) {
        LOG.debug("SchemaRegisterProcessor.forwardGetAllSchemas is called ");
        StringBuffer returnString = new StringBuffer();
        WorkerExecutor executor = vertx.createSharedWorkerExecutor("forwardGetAllSchemas_pool_" + new ObjectId(),
                ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
        executor.executeBlocking(future -> {
            // Call some blocking API that takes a significant amount of time to return
            String restURI = "http://" + schema_registry_host_and_port + "/subjects";
            int status_code = ConstantApp.STATUS_CODE_OK;

            LOG.debug("Starting List All Subjects @" + restURI);

            try {
                HttpResponse<String> res = Unirest.get(restURI)
                        .header("accept", ConstantApp.AVRO_REGISTRY_CONTENT_TYPE).asString();

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
                            // {"subject":"Kafka-value","version":1,"id":1,"schema":"\"string\""}
                            HttpResponse<JsonNode> resSubject = Unirest
                                    .get(restURI + "/" + subject + "/versions/latest")
                                    .header("accept", ConstantApp.APPLICATION_JSON_CHARSET_UTF_8).asJson();

                            if (resSubject == null) {
                                status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
                            } else if (resSubject.getStatus() != ConstantApp.STATUS_CODE_OK) {
                                status_code = resSubject.getStatus();
                            } else {
                                String schema = resSubject.getBody().toString();

                                String compatibility =
                                        getCompatibilityOfSubject(schema_registry_host_and_port, subject);
                                LOG.debug("compatibility: " + compatibility);

                                if (compatibility != null && !compatibility.isEmpty()) {
                                    JSONObject jsonSchema = new JSONObject(schema);
                                    jsonSchema.put(ConstantApp.COMPATIBILITY, compatibility);
                                    schema = jsonSchema.toString();
                                }

                                LOG.debug("schema: " + schema);

                                if (count == 0) {
                                    strBuff.append("[");
                                }

                                count++;
                                strBuff.append(schema).append(",");
                            }
                        }

                        LOG.debug("strBuf.toString(): " + strBuff.toString() + ", count = " + count);

                        if (count > 0) {
                            returnString.append(strBuff.toString().substring(0, strBuff.toString().length() - 1) + "]");
                        }

                        LOG.debug("returnString: " + returnString.toString());

                    }
                }
            } catch (JSONException | UnirestException e) {
                LOG.error("SchemaRegisterProcessor - forwardGetAllSchemas() - Exception message: " + e.getCause());
                e.printStackTrace();
                status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
            }

            LOG.debug("status_code:" + status_code);
            future.complete(status_code);
        }, res -> {
            Object result = HelpFunc.coalesce(res.result(), ConstantApp.STATUS_CODE_BAD_REQUEST);
            routingContext.response().setStatusCode(Integer.parseInt(result.toString()))
                    .putHeader("Access-Control-Allow-Origin", "*")
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(HelpFunc.stringToJsonFormat(returnString.toString()));
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
            routingContext.response().setStatusCode(Integer.parseInt(result.toString()))
                    .putHeader("Access-Control-Allow-Origin", "*")
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
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
        subject = jsonObj.getString(ConstantApp.SUBJECT);
        compatibility = jsonObj.optString(ConstantApp.COMPATIBILITY);
        LOG.debug("Schema|subject|compatibility: " + schema.toString() + "|" + subject + "|" + compatibility);

        restURI = "http://" + schema_registry_host_and_port + "/subjects/" + subject + "/versions";

        // Add the new schema
        final RestClientRequest postRestClientRequest = rc_schema.post(restURI, String.class,
                portRestResponse -> {
                    String rs = portRestResponse.getBody();

                    if (rs != null) {
                        LOG.info("Add schema status code " + portRestResponse.statusCode());
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                                .putHeader("Access-Control-Allow-Origin", "*")
                                .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                                .end();
                    }
                }
        );

        postRestClientRequest.exceptionHandler(exception -> {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .putHeader("Access-Control-Allow-Origin", "*")
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                    .end();
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMAREGISTRY_JSON));

        JSONObject object = new JSONObject().put("schema", schema.toString());
        LOG.debug("Schema object.toString(): " + object.toString());
        postRestClientRequest.end(object.toString());

        // Set compatibility to the subject
        if (compatibility != null && compatibility.trim().length() > 0) {
            restURI = "http://" + schema_registry_host_and_port + "/config/" + subject;
            final RestClientRequest postRestClientRequest2 = rc_schema.put(restURI, portRestResponse -> {
                LOG.info("Update Config Compatibility status code " + portRestResponse.statusCode());
                if (routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_OK) {
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .putHeader("Access-Control-Allow-Origin", "*")
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                            .end();
                }
            });

            postRestClientRequest2.exceptionHandler(exception -> {
                if (routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_CONFLICT
                        && routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_OK) {
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                            .putHeader("Access-Control-Allow-Origin", "*")
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                            .end();
                }
            });

            postRestClientRequest2.setContentType(MediaType.APPLICATION_JSON);
            postRestClientRequest2.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMAREGISTRY_JSON));
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
                        routingContext
                                .response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                                .putHeader("Access-Control-Allow-Origin", "*")
                                .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                                .end();
                    }
                }
        );

        postRestClientRequest.exceptionHandler(exception -> {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .putHeader("Access-Control-Allow-Origin", "*")
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                    .end("Update one schema POST request exception - " + exception.toString());
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMAREGISTRY_JSON));

        jsonForSubmit = new JSONObject().put("schema", schema1.toString());
        LOG.debug("Schema send to update is: " + jsonForSubmit.toString());

        postRestClientRequest.end(jsonForSubmit.toString());

        // Set compatibility to the subject
        if (compatibility != null && compatibility.trim().length() > 0) {
            restURI = "http://" + schema_registry_host_and_port + "/config/" + subject;
            final RestClientRequest postRestClientRequest2 = rc_schema.put(restURI, portRestResponse -> {
                if (routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_OK) {
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .putHeader("Access-Control-Allow-Origin", "*")
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                            .end(portRestResponse.statusMessage());
                }
            });

            postRestClientRequest2.exceptionHandler(exception -> {
                if (routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_CONFLICT
                        && routingContext.response().getStatusCode() != ConstantApp.STATUS_CODE_OK) {
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                            .putHeader("Access-Control-Allow-Origin", "*")
                            .putHeader(ConstantApp.CONTENT_TYPE, "application/vnd.schemaregistry.v1+json")
                            .end("Update one schema - compatibility POST request exception - " + exception.toString());
                }
            });

            postRestClientRequest2.setContentType(MediaType.APPLICATION_JSON);
            postRestClientRequest2.setAcceptHeader(Arrays.asList(DFMediaType.APPLICATION_SCHEMAREGISTRY_JSON));

            JSONObject jsonToBeSubmitted = new JSONObject().put(ConstantApp.COMPATIBILITY, compatibility);
            LOG.debug("Compatibility sent is: " + jsonToBeSubmitted.toString());
            postRestClientRequest2.end(jsonToBeSubmitted.toString());
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
            LOG.error("getCompatibilityOfSubject(), error message: " + e.getCause());
        }

        return compatibility;
    }

}

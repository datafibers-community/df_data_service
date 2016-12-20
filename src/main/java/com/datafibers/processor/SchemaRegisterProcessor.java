package com.datafibers.processor;

import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.RoutingContext;
import java.net.ConnectException;
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
     *
     * @param routingContext
     * @param rc_schema
     * @param schema_registry_host_and_port
     */
    public static void forwardGetAllSchemas(Vertx vertx, RoutingContext routingContext, RestClient rc_schema, String schema_registry_host_and_port) {
        LOG.debug("=== forwardGetAllSchemas === ");
        StringBuffer returnString = new StringBuffer();
        WorkerExecutor executor = vertx.createSharedWorkerExecutor("forwardGetAllSchemas_pool_" + new ObjectId(),
                ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
        executor.executeBlocking(future -> {
            // Call some blocking API that takes a significant amount of time to return
            String restURI = "http://" + schema_registry_host_and_port + "/subjects";
            int status_code = ConstantApp.STATUS_CODE_OK;

            LOG.debug("=== Step 1: Starting List All Subjects ... restURI: " + restURI);

            try {
                HttpResponse<String> res = Unirest.get(restURI).header("accept", "application/vnd.schemaregistry.v1+json").asString();

                if (res == null) {
                    status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
                } else if (res.getStatus() != ConstantApp.STATUS_CODE_OK) {
                    status_code = res.getStatus();
                } else {
                    String subjects = res.getBody();
                    // ["Kafka-value","Kafka-key"]
                    LOG.debug("==== Step 2: res ==> res.getBody(): " + res);
                    LOG.debug("==== Step 3: All subjects ==> subjects: " + subjects);
                    StringBuffer strBuff = new StringBuffer();
                    int count = 0;

                    if (subjects.compareToIgnoreCase("[]") != 0) { // Has active subjects
                        for (String subject : subjects.substring(2, subjects.length() - 2).split("\",\"")) {
                            // Get connector config: curl -X GET -i http://localhost:8081/subjects/Kafka-value/versions/latest
                            // {"subject":"Kafka-value","version":1,"id":1,"schema":"\"string\""}
                            HttpResponse<JsonNode> resSubject = Unirest.get(restURI + "/" + subject + "/versions/latest")
                                    .header("accept", "application/json").asJson();

                            if (resSubject == null) {
                                status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
                            } else if (resSubject.getStatus() != ConstantApp.STATUS_CODE_OK) {
                                status_code = resSubject.getStatus();
                            } else {
                                JsonNode resSchema = resSubject.getBody();
                                LOG.debug("==== Step 41: resSchema: " + resSchema);

                                String schema = resSchema.toString().replace("\\\"", "");
                                LOG.debug("==== Step 42: schema - remove \": " + schema);

                                String compatibility = getCompatibilityOfSubject(schema_registry_host_and_port, subject);
                                LOG.debug("==== Step 51: compatibility: " + compatibility);

                                if (compatibility != null && !compatibility.isEmpty()) {
                                    JSONObject jsonSchema = new JSONObject(schema);
                                    jsonSchema.put(ConstantApp.COMPATIBILITY, compatibility);
                                    schema = jsonSchema.toString();

                                    LOG.debug("==== Step 52: jsonSchema.toString(): " + jsonSchema.toString());
                                }

                                if (count == 0) {
                                    strBuff.append("[");
                                }

                                count++;
                                strBuff.append(schema).append(",");
                            }
                        }

                        LOG.debug("==== Step 6: strBuf.toString(): " + strBuff.toString() + ", count = " + count);

                        if (count > 0) {
                            returnString.append(strBuff.toString().substring(0, strBuff.toString().length() - 1) + "]");
                        }

                        LOG.debug("==== Step 7: returnString: " + returnString.toString());

                    }
                }
            } catch (JSONException | UnirestException e) {
                LOG.error("SchemaRegisterProcessor - forwardGetAllSchemas() - Exception message: " + e.getCause());
                e.printStackTrace();
                status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
            }

            LOG.debug("Step 8:  status_code:" + status_code);
            future.complete(status_code);
        }, res -> {
            Object result = HelpFunc.coalesce(res.result(), ConstantApp.STATUS_CODE_BAD_REQUEST);
            routingContext.response().setStatusCode(Integer.parseInt(result.toString()))
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(HelpFunc.stringToJsonFormat(returnString.toString()));
            executor.close();
        });

    }

    /**
     * Retrieve the specified subject's schema information.
     *
     * @param vertx
     * @param routingContext
     * @param rc_schema
     * @param schema_registry_host_and_port
     */
    public static void forwardGetOneSchema(Vertx vertx, RoutingContext routingContext, RestClient rc_schema, String schema_registry_host_and_port) {
        LOG.debug("=== forwardGetOneSchema === ");

        final String subject = routingContext.request().getParam("id");
        StringBuffer returnString = new StringBuffer();

        WorkerExecutor executor = vertx.createSharedWorkerExecutor("getOneSchema_pool_" + subject, ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
        executor.executeBlocking(future -> {
            String restURI = "http://" + schema_registry_host_and_port + "/subjects/" + subject + "/versions/latest";
            int status_code = ConstantApp.STATUS_CODE_OK;

            if (subject == null) {
                status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
            } else {
                try {
                    HttpResponse<String> res = Unirest.get(restURI).header("accept", "application/vnd.schemaregistry.v1+json").asString();

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
            LOG.debug("Step 11:  status_code: " + status_code);
            future.complete(status_code);
        }, res -> {
            Object result = HelpFunc.coalesce(res.result(), ConstantApp.STATUS_CODE_BAD_REQUEST);
            routingContext.response().setStatusCode(Integer.parseInt(result.toString()))
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(HelpFunc.stringToJsonFormat(returnString.toString()));
            executor.close();
        });
    }

    /**
     * curl -X GET -i http://localhost:8081/config/finance-value
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
            LOG.debug("===== res.getBody(): " + res.getBody());
            if (res.getBody() != null) {
                if (res.getBody().indexOf("40401") > 0) {
                } else {
                    JSONObject jason = new JSONObject(res.getBody().toString());
                    compatibility = jason.getString(ConstantApp.COMPATIBILITYLEVEL);
                }
            }
        } catch (UnirestException e) {
            LOG.error("===== DF DataProcessor, getCompatibilityOfSubject(), error message: " + e.getCause());
        }

        return compatibility;
    }

}

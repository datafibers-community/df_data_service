package com.datafibers.processor;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;

import java.net.ConnectException;
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.hubrick.vertx.rest.MediaType;
import com.hubrick.vertx.rest.RestClient;
import com.hubrick.vertx.rest.RestClientRequest;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class KafkaConnectProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectProcessor.class);

    public KafkaConnectProcessor(){

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
                    LOG.debug("json object name: " + jo.getString("name"));
                    LOG.debug("json object config: " + jo.getJsonObject("config"));
                    LOG.debug("json object tasks: " + jo.getMap().get("tasks"));
                    LOG.info("received response from Kafka server: " + portRestResponse.statusMessage());
                    LOG.info("received response from Kafka server: " + portRestResponse.statusCode());

                    // Once REST API forward is successful, add the record to the local repository
                    mongoClient.insert(mongoCOLLECTION, dfJobResponsed.toJson(), r -> routingContext
                            .response().setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                            .end(Json.encodePrettily(dfJobResponsed.setId(r.result()))));
                });

        postRestClientRequest.exceptionHandler(exception -> {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(errorMsg(10, "POST Request exception - " + exception.toString()));
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
        LOG.info("connectorConfig has change. Will forward to Kafka Connect.");

        final RestClientRequest postRestClientRequest =
                restClient.put(
                        ConstantApp.KAFKA_CONNECT_PLUGIN_CONFIG.
                                replace("CONNECTOR_NAME_PLACEHOLDER", dfJobResponsed.getConnector()),
                        String.class, portRestResponse -> {
                            LOG.info("received response from Kafka server: " + portRestResponse.statusMessage());
                            LOG.info("received response from Kafka server: " + portRestResponse.statusCode());
                        });

        postRestClientRequest.exceptionHandler(exception -> {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(errorMsg(31, "POST Request exception - " + exception.toString()));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end(dfJobResponsed.mapToJsonString(dfJobResponsed.getConnectorConfig()));

        mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id), // Select a unique document
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", dfJobResponsed.toJson()), v -> {
                    if (v.failed()) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(errorMsg(32, "updateOne to repository is failed."));
                    } else {
                        routingContext.response()
                                .putHeader(ConstantApp.CONTENT_TYPE,
                                        ConstantApp.APPLICATION_JSON_CHARSET_UTF_8).end();
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
                        dfJobResponsed.getConnector(), String.class,
                portRestResponse -> {
                    LOG.info("received response from Kafka server: " + portRestResponse.statusMessage());
                    LOG.info("received response from Kafka server: " + portRestResponse.statusCode());
                    if(portRestResponse.statusCode() == ConstantApp.STATUS_CODE_OK_NO_CONTENT) {
                        // Once REST API forward is successful, delete the record to the local repository
                        mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
                                ar -> routingContext.response().end(id + " is deleted from repository."));
                    } else {
                        LOG.error("DELETE conflict and rebalance is in process.");
                    }
                });

        postRestClientRequest.exceptionHandler(exception -> {

            // Once REST API forward is successful, delete the record to the local repository
            mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
                    ar -> routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                            .end(errorMsg(40, "Not Found in KAFKA Connect, " +
                                    "But delete from repository. The not found exception - " + exception.toString())));
            LOG.info("Cannot find the connector name in DELETE request in Kafka Connect. Remove from local repo only.");
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end("");
    }
    
    /**
     * Retrieve all subjects first; and then retrieve corresponding subject's schema information
     * 
     * @param routingContext
     * @param rc_schema
     * @param schema_registry_host_and_port
     */
    public static int forwardGetAllSchemas(RoutingContext routingContext, RestClient rc_schema, String schema_registry_host_and_port) {
    		// curl -X GET -i http://localhost:8081/subjects
    		String restURI = "http://" + schema_registry_host_and_port + "/subjects";
    		LOG.debug("=== Step 1: Starting List All Subjects ... restURI: " + restURI);
    		String returnString = "";
    		
    		try {
    			HttpResponse<String> res = Unirest.get(restURI).header("accept", "application/vnd.schemaregistry.v1+json").asString();
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
    					JsonNode resSchema = resSubject.getBody();
    					LOG.debug("==== Step 41: resSchema: " + resSchema);
    					
    					String schema = resSchema.toString().replace("\\\"", "");
    					LOG.debug("==== Step 42: schema - remove \": " + schema);
    					
    					String compatibility = HelpFunc.getCompatibilityOfSubject(schema_registry_host_and_port, subject);
    					
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
    				
    				LOG.debug("==== Step 6: strBuf.toString(): " + strBuff.toString() + ", count = " + count);
    				
    				if (count > 0) {
    					returnString = strBuff.toString().substring(0, strBuff.toString().length() -1) + "]";
    				}
    				
    				LOG.debug("==== Step 7: returnString: " + returnString);
    				
    				routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(returnString);
    			} else {
    				routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(returnString);
    			}
    			
    			return ConstantApp.STATUS_CODE_OK;
    		} catch (JSONException e) {
    			routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                .end(HelpFunc.errorMsg(31, "POST Request exception - " + e.toString()));
    			e.printStackTrace();
    			
    			return ConstantApp.STATUS_CODE_CONFLICT;
    		} catch (UnirestException e) {
    			routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                .end(HelpFunc.errorMsg(31, "POST Request exception - " + e.toString()));
    			e.printStackTrace();
    			
    			return ConstantApp.STATUS_CODE_CONFLICT;
    		} catch (Exception e) {
	        	routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
	            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
	            .end(HelpFunc.errorMsg(31, "POST Request exception - " + e.toString()));
				e.printStackTrace();
	            LOG.error("Flink Submit Exception" + e.getCause());
	            
	            return ConstantApp.STATUS_CODE_CONFLICT;
        }
    }
    
    
    public static int forwardGetOneSchema(RoutingContext routingContext, RestClient rc_schema, String subject, String schema_registry_host_and_port) {
    	// String restURI = "http://" + this.kafka_connect_rest_host + ":" + this.kafka_registry_rest_port + "/subjects/" + subject + "/versions/latest";
    	String restURI = "http://" + schema_registry_host_and_port + "/subjects/" + subject + "/versions/latest";
    	
        LOG.debug("=== restURI:" + restURI);
        
        if (subject == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(HelpFunc.errorMsg(20, "id is null in your request."));
            return ConstantApp.STATUS_CODE_BAD_REQUEST;
        } else {
        	try {
    			HttpResponse<String> res = Unirest.get(restURI).header("accept", "application/vnd.schemaregistry.v1+json").asString();
    			String schema1 = res.getBody();
    			LOG.debug("==== restURI2 ==> restURI2: " + restURI);
    			LOG.debug("==== schema1 ==> schema1: " + schema1);
    			
    			String schema2 = schema1.replace(":\"{", ":{");
    			LOG.debug("==== schema21 remove first quotation ==> : " + schema2);
    			
    			schema2 = schema2.replace("}]}\"", "}]}");
    			LOG.debug("==== schema22 remove second quotation ==> : " + schema2);
    			
    			schema2 = schema2.replace("\\\"", "");
    			LOG.debug("==== schema23: remove slash quotation ==> : " + schema2);
    			
    			JSONObject json = new JSONObject(schema2);
    			
    			// Get the subject's compatibility
				String compatibility = HelpFunc.getCompatibilityOfSubject(schema_registry_host_and_port, subject);
				LOG.debug("==== Step 51: compatibility: " + compatibility);
				
				if (compatibility != null && !compatibility.isEmpty()) {
					json.put(ConstantApp.COMPATIBILITY, compatibility);
				}
				
				LOG.debug("==== json.toString(): ==> : " + json.toString());
    			
    			routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(json.toString());
    			
    			return ConstantApp.STATUS_CODE_OK;
    		} catch (JSONException e) {
    			 routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                 .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                 .end(HelpFunc.errorMsg(31, "POST Request exception - " + e.toString()));
    			e.printStackTrace();
    			
    			return ConstantApp.STATUS_CODE_CONFLICT;
    		} catch (UnirestException e) {
    			routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                .end(HelpFunc.errorMsg(31, "POST Request exception - " + e.toString()));
    			e.printStackTrace();
    			
    			return ConstantApp.STATUS_CODE_CONFLICT;
    		}
        }
    	
    }
    
    
    /**
     * Print error message in better JSON format
     *
     * @param error_code
     * @param msg
     * @return
     */
    public static String errorMsg(int error_code, String msg) {
        return Json.encodePrettily(new JsonObject()
                .put("code", String.format("%6d", error_code))
                .put("message", msg));
    }
}

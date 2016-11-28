package com.datafibers.processor;

import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.RoutingContext;

import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.hubrick.vertx.rest.RestClient;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class SchemaRegisterForward {
	 private static final Logger LOG = LoggerFactory.getLogger(SchemaRegisterForward.class);
	 
    /**
     * Retrieve all subjects first; and then retrieve corresponding subject's schema information
     * 
     * @param routingContext
     * @param rc_schema
     * @param schema_registry_host_and_port
     */
    public static void forwardGetAllSchemas(Vertx vertx, RoutingContext routingContext, RestClient rc_schema, String schema_registry_host_and_port) {
    	LOG.debug("=== forwardGetAllSchemas === ");
    	
    	int maxRunTime = 6000;  // 5 minutes of waiting for response
    	StringBuffer returnString = new StringBuffer();
    	UUID uuid = UUID.randomUUID();
    	
    	WorkerExecutor executor = vertx.createSharedWorkerExecutor("forwardGetAllSchemas_pool_" + uuid, ConstantApp.WORKER_POOL_SIZE, maxRunTime);
    	
    	executor.executeBlocking(future -> {
    		// Call some blocking API that takes a significant amount of time to return
    		// curl -X GET -i http://localhost:8081/subjects
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
	    			    }
	    			
	    				LOG.debug("==== Step 6: strBuf.toString(): " + strBuff.toString() + ", count = " + count);
	    				
	    				if (count > 0) {
	    					returnString.append(strBuff.toString().substring(0, strBuff.toString().length() -1) + "]");
	    				}
	    				
	    				LOG.debug("==== Step 7: returnString: " + returnString.toString());
	    				
	    			}
    			}
	    	} catch (JSONException | UnirestException e) {
    			LOG.error("SchemaRegisterForward - forwardGetAllSchemas() - Exception message: " + e.getCause());
    			e.printStackTrace();
    			status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
    		}
    		
    		LOG.debug("Step 8:  status_code: " + status_code);
    		future.complete(status_code);
        }, res -> {
            Object result = HelpFunc.coalesce(res.result(), ConstantApp.STATUS_CODE_BAD_REQUEST);
            
            LOG.debug("==== Step 9: returnString: " + returnString.toString() + ", result = " + result.toString());
            
        	routingContext.response().setStatusCode(Integer.parseInt(result.toString()))
        	.putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
        	.end(returnString.toString());
            
        	LOG.debug("Step 10:  BLOCKING CODE IS TERMINATE?FINISHED: " + res.cause());
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
    	LOG.debug("=== id:" + subject);
    	int maxRunTime = 6000;
    	StringBuffer returnString = new StringBuffer();
    	
    	WorkerExecutor executor = vertx.createSharedWorkerExecutor("getOneSchema_pool_" + subject, ConstantApp.WORKER_POOL_SIZE, maxRunTime);
    	
    	executor.executeBlocking(future -> {
    		// Call some blocking API that takes a significant amount of time to return
    		// String restURI = "http://" + this.kafka_connect_rest_host + ":" + this.kafka_registry_rest_port + "/subjects/" + subject + "/versions/latest";
        	String restURI = "http://" + schema_registry_host_and_port + "/subjects/" + subject + "/versions/latest";
        	int status_code = ConstantApp.STATUS_CODE_OK;
        	
            LOG.debug("=== restURI:" + restURI);
            
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
	        			String schema1 = res.getBody();
	        			LOG.debug("==== restURI ==>" + restURI);
	        			LOG.debug("==== schema1 ==>" + schema1);
	        			
	        			String schema2 = schema1.replace(":\"{", ":{");
	        			LOG.debug("==== schema21 remove first quotation ==>" + schema2);
	        			
	        			schema2 = schema2.replace("}]}\"", "}]}");
	        			LOG.debug("==== schema22 remove second quotation ==>" + schema2);
	        			
	        			schema2 = schema2.replace("\\\"", "");
	        			LOG.debug("==== schema23: remove slash quotation ==>" + schema2);
	        			
	        			JSONObject json = new JSONObject(schema2);
	        			
	        			// Get the subject's compatibility
	    				String compatibility = HelpFunc.getCompatibilityOfSubject(schema_registry_host_and_port, subject);
	    				LOG.debug("==== Step 51: compatibility: " + compatibility);
	    				
	    				if (compatibility != null && !compatibility.isEmpty()) {
	    					json.put(ConstantApp.COMPATIBILITY, compatibility);
	    				}
	    				
	    				LOG.debug("==== json.toString(): ==> : " + json.toString());
	    				returnString.append(json.toString());
	        			
	        			status_code = ConstantApp.STATUS_CODE_OK;
    				}
	        	} catch (JSONException | UnirestException e) {
	        		LOG.error("SchemaRegisterForward - forwardGetAllSchemas() - Exception message: " + e.getCause());
        			status_code = ConstantApp.STATUS_CODE_BAD_REQUEST;
        		}
            }
    		
    		LOG.debug("Step 11:  status_code: " + status_code);
    		future.complete(status_code);
        }, res -> {
        	Object result = HelpFunc.coalesce(res.result(), ConstantApp.STATUS_CODE_BAD_REQUEST);
        	LOG.debug("Step 12:  status_code: " + result.toString());
        	LOG.debug("Step 13:  returnString.toString(): " + returnString.toString());
        	
    		routingContext.response().setStatusCode(Integer.parseInt(result.toString()))
            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
            .end(returnString.toString());
        	
        	LOG.debug("Step 14:  BLOCKING CODE IS TERMINATE?FINISHED: " + res.cause());
        	executor.close();
        });
    }
    
}

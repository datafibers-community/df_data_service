package com.datafibers.processor;

import io.vertx.ext.web.RoutingContext;

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
    
}

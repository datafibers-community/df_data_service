package com.datafibers.util;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.sling.commons.json.JSONArray;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.avro.Schema.Type.RECORD;

public class SchemaRegistryClient {

    private static final Logger LOG = Logger.getLogger(SchemaRegistryClient.class);
    public static final String KEY_FIELD = "symbol";
    public static final String VALUE_FIELD = "refresh_time";
    public static final String HTTP_HEADER_APPLICATION_JSON_CHARSET = "application/json; charset=utf-8";
    public static final String AVRO_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    public static Schema getSchemaFromRegistry (String schemaUri, String schemaSubject, String schemaVersion) {

        if(schemaVersion == null) schemaVersion = "latest";
        String fullUrl = String.format("%s/subjects/%s/versions/%s", schemaUri, schemaSubject, schemaVersion);

        String schemaString;
        BufferedReader br = null;
        try {
            StringBuilder response = new StringBuilder();
            String line;
            br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));
            while ((line = br.readLine()) != null) {
                response.append(line);
            }

            JsonNode responseJson = new ObjectMapper().readValue(response.toString(), JsonNode.class);
            schemaString = responseJson.get("schema").getValueAsText();

            try {
                return new Schema.Parser().parse(schemaString);
            } catch (SchemaParseException ex) {
                LOG.error(String.format("Unable to successfully parse schema from: %s", schemaString), ex);
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static Schema getSchemaFromRegistrywithDefault (String schemaUri, String schemaSubject, String schemaVersion) {
        if(!schemaUri.contains("http")) {
            schemaUri = "http://" + schemaUri;
        }
        if(schemaVersion == null) schemaVersion = "latest";
        String fullUrl = String.format("%s/subjects/%s/versions/%s", schemaUri, schemaSubject, schemaVersion);

        String schemaString;
        BufferedReader br = null;
        try {
            StringBuilder response = new StringBuilder();
            String line;
            br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));
            while ((line = br.readLine()) != null) {
                response.append(line);
            }

            JsonNode responseJson = new ObjectMapper().readValue(response.toString(), JsonNode.class);
            schemaString = responseJson.get("schema").getValueAsText();

            try {
                return new Schema.Parser().parse(schemaString);
            } catch (SchemaParseException ex) {
                LOG.error(String.format("Unable to successfully parse schema from: %s", schemaString), ex);
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static Schema getVersionedSchemaFromProperty (Properties properties, String schemaVersion) {

        String schemaUri;
        String schemaSubject = "";

        if (properties.getProperty("schema.registry") == null) {
            schemaUri = "http://localhost:8081";
        } else {
            schemaUri = "http://" + properties.getProperty("schema.registry");
        }

        if (properties.getProperty("schema.subject") == null) {
            LOG.error("schema.subject must be set in the property");
        } else {
            schemaSubject = properties.getProperty("schema.subject");
        }

        return getSchemaFromRegistry(schemaUri, schemaSubject, schemaVersion);
    }

    public static String getLatestSchemaNodeFromProperty (Properties properties) {

        String schemaUri;
        String schemaSubject = "";

        if (properties.getProperty("schema.registry") == null) {
            schemaUri = "http://localhost:8081";
        } else {
            schemaUri = "http://" + properties.getProperty("schema.registry");
        }

        if (properties.getProperty("schema.subject") == null) {
            LOG.error("schema.subject must be set in the property");
            //schemaSubject = topic + "-value";
        } else {
            schemaSubject = properties.getProperty("schema.subject");
        }

        String schemaVersion = "latest";
        String fullUrl = String.format("%s/subjects/%s/versions/%s", schemaUri, schemaSubject, schemaVersion);

        String schemaString="";
        BufferedReader br = null;
        try {
        	StringBuilder response = new StringBuilder();
        	String line;
        	br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));
        	while ((line = br.readLine()) != null) {
        		response.append(line);
        	}

        	JsonNode responseJson = new ObjectMapper().readValue(response.toString(), JsonNode.class);
        	schemaString = responseJson.get("schema").getValueAsText();
        	LOG.warn("schemaString: " + schemaString);
        	return schemaString;
         } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
        }

    public static Schema getLatestSchemaFromProperty (Properties properties, String schemaSubjectAttributeName) {

        String schemaUri;
        String schemaSubject = "";

        if (properties.getProperty("schema.registry") == null) {
            schemaUri = "http://localhost:8081";
        } else {
            schemaUri = "http://" + properties.getProperty("schema.registry");
        }

        if (properties.getProperty(schemaSubjectAttributeName) == null) {
            LOG.error("schema.subject must be set in the property");
            //schemaSubject = topic + "-value";
        } else {
            schemaSubject = properties.getProperty(schemaSubjectAttributeName);
        }

        return getSchemaFromRegistry(schemaUri, schemaSubject, "latest");
    }

    public static Schema getLatestSchemaFromProperty (Properties properties) {

        String schemaUri;
        String schemaSubject = "";

        if (properties.getProperty("schema.registry") == null) {
            schemaUri = "http://localhost:8081";
        } else {
            schemaUri = "http://" + properties.getProperty("schema.registry");
        }

        if (properties.getProperty("schema.subject") == null) {
            LOG.error("schema.subject must be set in the property");
            //schemaSubject = topic + "-value";
        } else {
            schemaSubject = properties.getProperty("schema.subject");
        }

        return getSchemaFromRegistry(schemaUri, schemaSubject, "latest");
    }

    public static int getLatestSchemaIDFromProperty (Properties properties, String schemaSubjectAttributeName) {

        String schemaUri;
        String schemaSubject = "";
        int schemaId=0;

        if (properties.getProperty("schema.registry") == null) {
            schemaUri = "http://localhost:8081";
        } else {
            schemaUri = "http://" + properties.getProperty("schema.registry");
        }

        if (properties.getProperty(schemaSubjectAttributeName) == null) {
            LOG.error("schema.subject must be set in the property");
            return -1;
        } else {
            schemaSubject = properties.getProperty(schemaSubjectAttributeName);
        }

        String schemaVersion = "latest";
        String fullUrl = String.format("%s/subjects/%s/versions/%s", schemaUri, schemaSubject, schemaVersion);

        BufferedReader br = null;
        try {
            StringBuilder response = new StringBuilder();
            String line;
            br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));
            while ((line = br.readLine()) != null) {
                response.append(line);
            }

            JsonNode responseJson = new ObjectMapper().readValue(response.toString(), JsonNode.class);
            schemaId = responseJson.get("id").getValueAsInt();

         } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return schemaId;
    }

    public static String[] getFieldNames (Schema schema) {

        List<String> stringList = new ArrayList<String>();
        if (RECORD.equals(schema.getType()) && schema.getFields() != null && !schema.getFields().isEmpty()) {
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                stringList.add(field.name());
            }
        }
        String[] fieldNames = stringList.toArray( new String[] {} );
        return fieldNames;
    }

    public static String[] getFieldNamesFromProperty (Properties properties) {
        Schema schema = getLatestSchemaFromProperty(properties);

        List<String> stringList = new ArrayList<String>();
        if (RECORD.equals(schema.getType()) && schema.getFields() != null && !schema.getFields().isEmpty()) {
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                stringList.add(field.name());
            }
        }
        String[] fieldNames = stringList.toArray( new String[] {} );
        return fieldNames;
    }

    public static String[] getFieldNamesFromProperty (Properties properties, String schemaSubjectAttributeName) {
        Schema schema = getLatestSchemaFromProperty(properties, schemaSubjectAttributeName);

        List<String> stringList = new ArrayList<String>();
        if (RECORD.equals(schema.getType()) && schema.getFields() != null && !schema.getFields().isEmpty()) {
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                stringList.add(field.name());
            }
        }
        String[] fieldNames = stringList.toArray( new String[] {} );
        return fieldNames;
    }

    public static Class<?>[] getFieldTypes (Schema schema) {

        Class<?>[] fieldTypes = new Class[schema.getFields().size()];
        int index = 0;
        String typeName;

        try {
            if (RECORD.equals(schema.getType()) && schema.getFields() != null && !schema.getFields().isEmpty()) {
                for (org.apache.avro.Schema.Field field : schema.getFields()) {
                    typeName = field.schema().getType().getName().toLowerCase();
                    // Mapping Avro type to Java type - TODO Complex type is not supported yet
                    switch (typeName) {
                        case "boolean":
                        case "string":
                        case "long":
                        case "float":
                            fieldTypes[index] = Class.forName("java.lang." + StringUtils.capitalize(typeName));
                            break;
                        case "bytes":
                            fieldTypes[index] = Class.forName("java.lang.Byte");
                            break;
                        case "int":
                            fieldTypes[index] = Class.forName("java.lang.Integer");
                            break;
                        default:
                            fieldTypes[index] = Class.forName("java.lang.String");
                    }
                    index ++;
                }
            }
        } catch (ClassNotFoundException cnf) {
            cnf.printStackTrace();
        }

        return fieldTypes;
    }

    public static Class<?>[] getFieldTypesFromProperty (Properties properties) {

        Schema schema = getLatestSchemaFromProperty(properties);

        Class<?>[] fieldTypes = new Class[schema.getFields().size()];
        int index = 0;
        String typeName;

        try {
            if (RECORD.equals(schema.getType()) && schema.getFields() != null && !schema.getFields().isEmpty()) {
                for (org.apache.avro.Schema.Field field : schema.getFields()) {
                    typeName = field.schema().getType().getName().toLowerCase();
                    // Mapping Avro type to Java type - TODO Complex type is not supported yet
                    switch (typeName) {
                        case "boolean":
                        case "string":
                        case "long":
                        case "float":
                            fieldTypes[index] = Class.forName("java.lang." + StringUtils.capitalize(typeName));
                            break;
                        case "bytes":
                            fieldTypes[index] = Class.forName("java.lang.Byte");
                            break;
                        case "int":
                            fieldTypes[index] = Class.forName("java.lang.Integer");
                            break;
                        default:
                            fieldTypes[index] = Class.forName("java.lang.String");
                    }
                    index ++;
                }
            }
        } catch (ClassNotFoundException cnf) {
            cnf.printStackTrace();
        }

        return fieldTypes;
    }

    public static Class<?>[] getFieldTypesFromProperty (Properties properties, String schemaSubjectAttributeName) {

        Schema schema = getLatestSchemaFromProperty(properties, schemaSubjectAttributeName);

        Class<?>[] fieldTypes = new Class[schema.getFields().size()];
        int index = 0;
        String typeName;

        try {
            if (RECORD.equals(schema.getType()) && schema.getFields() != null && !schema.getFields().isEmpty()) {
                for (org.apache.avro.Schema.Field field : schema.getFields()) {
                    typeName = field.schema().getType().getName().toLowerCase();
                    // Mapping Avro type to Java type - TODO Complex type is not supported yet
                    switch (typeName) {
                        case "boolean":
                        case "string":
                        case "long":
                        case "float":
                            fieldTypes[index] = Class.forName("java.lang." + StringUtils.capitalize(typeName));
                            break;
                        case "bytes":
                            fieldTypes[index] = Class.forName("java.lang.Byte");
                            break;
                        case "int":
                            fieldTypes[index] = Class.forName("java.lang.Integer");
                            break;
                        default:
                            fieldTypes[index] = Class.forName("java.lang.String");
                    }
                    index ++;
                }
            }
        } catch (ClassNotFoundException cnf) {
            cnf.printStackTrace();
        }

        return fieldTypes;
    }

    public static TypeInformation<?>[] getFieldTypesInfoFromProperty (Properties properties) {

        Schema schema = getLatestSchemaFromProperty(properties);

        TypeInformation<?>[] fieldTypes = new TypeInformation[schema.getFields().size()];
        int index = 0;
        String typeName;

        try {
            if (RECORD.equals(schema.getType()) && schema.getFields() != null && !schema.getFields().isEmpty()) {
                for (org.apache.avro.Schema.Field field : schema.getFields()) {
                    typeName = field.schema().getType().getName().toLowerCase();
                    // Mapping Avro type to Java type - TODO Complex type is not supported yet
                    switch (typeName) {
                        case "boolean":
                        case "string":
                        case "long":
                        case "float":
                            fieldTypes[index] = TypeInformation.of(Class.forName("java.lang." + StringUtils.capitalize(typeName)));
                            break;
                        case "bytes":
                            fieldTypes[index] = TypeInformation.of(Class.forName("java.lang.Byte"));
                            break;
                        case "int":
                            fieldTypes[index] = TypeInformation.of(Class.forName("java.lang.Integer"));
                            break;
                        default:
                            fieldTypes[index] = TypeInformation.of(Class.forName("java.lang.String"));
                    }
                    index ++;
                }
            }
        } catch (ClassNotFoundException cnf) {
            cnf.printStackTrace();
        }

        return fieldTypes;
    }

    public static TypeInformation<?>[] getFieldTypesInfoFromProperty (Properties properties, String schemaSubjectAttributeName) {

        Schema schema = getLatestSchemaFromProperty(properties, schemaSubjectAttributeName);

        TypeInformation<?>[] fieldTypes = new TypeInformation[schema.getFields().size()];
        int index = 0;
        String typeName;

        try {
            if (RECORD.equals(schema.getType()) && schema.getFields() != null && !schema.getFields().isEmpty()) {
                for (org.apache.avro.Schema.Field field : schema.getFields()) {
                    typeName = field.schema().getType().getName().toLowerCase();
                    // Mapping Avro type to Java type - TODO Complex type is not supported yet
                    switch (typeName) {
                        case "boolean":
                        case "string":
                        case "long":
                        case "float":
                            fieldTypes[index] = TypeInformation.of(Class.forName("java.lang." + StringUtils.capitalize(typeName)));
                            break;
                        case "bytes":
                            fieldTypes[index] = TypeInformation.of(Class.forName("java.lang.Byte"));
                            break;
                        case "int":
                            fieldTypes[index] = TypeInformation.of(Class.forName("java.lang.Integer"));
                            break;
                        default:
                            fieldTypes[index] = TypeInformation.of(Class.forName("java.lang.String"));
                    }
                    index ++;
                }
            }
        } catch (ClassNotFoundException cnf) {
            cnf.printStackTrace();
        }

        return fieldTypes;
    }

    public static void addSchemaIfNotAvailable(Properties properties) {

        String schemaUri;
        String subject = properties.getProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT);
        String schemaString = properties.getProperty(ConstantApp.PK_SCHEMA_STR_OUTPUT);
        String srKey = ConstantApp.PK_KAFKA_SCHEMA_REGISTRY_HOST_PORT.replace("_", ".");

        if (properties.getProperty(srKey) == null) {
            schemaUri = "http://localhost:8081";
        } else {
            schemaUri = "http://" + properties.getProperty(srKey);
        }

        String schemaRegistryRestURL = schemaUri + "/subjects/" + subject + "/versions";

        try {
            HttpResponse<String> schemaRes = Unirest.get(schemaRegistryRestURL + "/latest")
                    .header("accept", HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .asString();

            if(schemaRes.getStatus() == ConstantApp.STATUS_CODE_NOT_FOUND) { // Add the meta sink schema
                Unirest.post(schemaRegistryRestURL)
                        .header("accept", HTTP_HEADER_APPLICATION_JSON_CHARSET)
                        .header("Content-Type", AVRO_REGISTRY_CONTENT_TYPE)
                        .body(schemaString).asString();
                LOG.info("Subject - " + subject + " Not Found, so create it.");
            } else {
                LOG.info("Subject - " + subject + " Found.");
            }
        } catch (UnirestException ue) {
            ue.printStackTrace();
        }
    }

    public static void addSchemaFromTableResult(String schemaUri, String subject, Table result) {

        if (schemaUri == null) {
            schemaUri = "http://localhost:8081";
        }

        if(!schemaUri.startsWith("http")) {
            schemaUri = "http://" + schemaUri;
        }

        String schemaRegistryRestURL = schemaUri + "/subjects/" + subject + "/versions";

        try {
            HttpResponse<String> schemaRes = Unirest.get(schemaRegistryRestURL + "/latest")
                    .header("accept", HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .asString();

            if(schemaRes.getStatus() == ConstantApp.STATUS_CODE_NOT_FOUND) { // Add the meta sink schema
                schemaRes = Unirest.post(schemaRegistryRestURL)
                        .header("accept", HTTP_HEADER_APPLICATION_JSON_CHARSET)
                        .header("Content-Type", AVRO_REGISTRY_CONTENT_TYPE)
                        .body(tableAPIToAvroSchema(result, subject))
                        .asString();

                LOG.info("Subject - " + subject + " Not Found, so create it." + schemaRes.getStatus());
            } else {
                LOG.info("Subject - " + subject + " Found.");
            }
        } catch (UnirestException ue) {
            ue.printStackTrace();
        }
    }

    public static String tableAPIToAvroSchema(Table result, String subject) {
        JsonArray fields = new JsonArray();
        for(String colName : result.getSchema().getColumnNames()) {
            fields.add(new JsonObject()
                    .put("name", colName)
                    .put("type", tableTypeToAvroType(result.getSchema().getType(colName).toString())));
        }

        return new JsonObject().put("schema", new JsonObject()
                .put("type", "record")
                .put("name", subject)
                .put("fields", fields).toString()).toString();


    }

    public static String tableTypeToAvroType(String type) {
        String returnType;
        String cleanedType = type.toLowerCase().replaceAll("some", "").replace("(", "").replace(")", "");
        switch (cleanedType) {
            case "integer":
            case "short":
                returnType = "int";
                break;
            case "byte":
                returnType = "bytes";
                break;
            case "timestamp":
            case "time":
            case "date":
            case "decimal":
            case "interval_months":
            case "interval_millis":
            case "primitive_array":
            case "object_array":
            case "map":
                returnType = "string";
                break;
            default:
                returnType = cleanedType;
        }
        return returnType;
    }


}

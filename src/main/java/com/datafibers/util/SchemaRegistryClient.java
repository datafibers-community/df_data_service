package com.datafibers.util;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.log4j.Logger;
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
}

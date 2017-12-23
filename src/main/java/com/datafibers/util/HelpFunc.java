package com.datafibers.util;

import com.datafibers.model.DFJobPOPJ;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * List of help functions to be used for all DF classes.
 */
public class HelpFunc {

    final static int max = 1000;
    final static int min = 1;
    final static String underscore = "_";
    final static String period = ".";
    final static String space = " ";
    final static String colon = ":";

    /**
     * Loop the enum of ConnectType to add all connects to the list by l
     */
    public static void addSpecifiedConnectTypetoList(List<String> list, String type_regx) {

        for (ConstantApp.DF_CONNECT_TYPE item : ConstantApp.DF_CONNECT_TYPE.values()) {
            if(item.name().matches(type_regx)) list.add(item.name());
        }
    }

    /**
     * Return the first not null objects in the list of arguments
     * @param a
     * @param b
     * @param <T>
     * @return object
     */
    public static <T> T coalesce(T a, T b) {
        return a == null ? b : a;
    }

    /**
     * This function will search JSONSTRING to find patterned keys_1..n. If it has key_ignored_mark subkey, the element
     * will be removed. For example {"connectorConfig_1":{"config_ignored":"test"}, "connectorConfig_2":{"test":"test"}}
     * will be cleaned as {"connectorConfig":{"test":"test"}}
     *
     * This will also remove any comments in "\/* *\/"
     * This is deprecated once new UI is released
     *
     * @param jsonString
     * @param key_ingored_mark: If the
     * @return cleaned json string
     */
    public static String cleanJsonConfigIgnored(String jsonString, String key_pattern, String key_ingored_mark) {
        JSONObject json = new JSONObject(jsonString.replaceAll("\\s+?/\\*.*?\\*/", ""));
        int index = 0;
        int index_found = 0;
        String json_key_to_check;
        while (true) {
            if (index == 0) {
                json_key_to_check = key_pattern.replace("_", "");
            } else json_key_to_check = key_pattern + index;

            if (json.has(json_key_to_check)) {
                if (json.getJSONObject(json_key_to_check).has(key_ingored_mark)) {
                    json.remove(json_key_to_check);
                } else index_found = index;
            } else break;
            index++;
        }
        if (index_found > 0)
            json.put(key_pattern.replace("_", ""), json.getJSONObject(key_pattern + index_found)).remove(key_pattern + index_found);
        return json.toString();
    }

    /**
     * This function will search topics in connectorConfig. If the topics are array, convert it to string.
     *
     * @param jsonString
     * @return cleaned json string
     */
    public static String convertTopicsFromArrayToString(String jsonString, String topicsKeyAliasString) {
        JSONObject json = new JSONObject(jsonString.replaceAll("\\s+?/\\*.*?\\*/", ""));
        if(json.has("connectorConfig")) {
            for(String topicsKey : topicsKeyAliasString.split(",")) {
                if(json.getJSONObject("connectorConfig").has(topicsKey)) {
                    Object topicsObj = json.getJSONObject("connectorConfig").get(topicsKey);
                    if(topicsObj instanceof JSONArray){ // if it is array, convert it to , separated string
                        JSONArray topicsJsonArray = (JSONArray) topicsObj;
                        json.getJSONObject("connectorConfig")
                                .put(topicsKey, topicsJsonArray.join(",").replace("\"", ""));
                    }
                }
            }
        }
        return json.toString();
    }

    /**
     * A default short-cut call for clean raw json for connectConfig.
     * List of cleaning functions will be called through this
     * @param JSON_STRING
     * @return cleaned json string
     */
    public static String cleanJsonConfig(String JSON_STRING) {
        String cleanedJsonString;
        cleanedJsonString = convertTopicsFromArrayToString(JSON_STRING, ConstantApp.PK_DF_TOPICS_ALIAS);
        return cleanedJsonString;
    }

    /**
     * Generate a file name for the UDF Jar uploaded to avoid naming conflict
     * @param inputName
     * @return fileName
     */
    public static String generateUniqueFileName(String inputName) {
        Date curDate = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String DateToStr = format.format(curDate);

        DateToStr = DateToStr.replaceAll(space, underscore);
        DateToStr = DateToStr.replaceAll(colon, underscore);

        // nextInt excludes the top value so we have to add 1 to include the top value
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min) + 1) + min;

        if (inputName != null && inputName.indexOf(period) > 0) {
            int firstPart = inputName.indexOf(period);
            String fileName = inputName.substring(0, firstPart);
            String fileExtension = inputName.substring(firstPart + 1);
            DateToStr = fileName + underscore + DateToStr + underscore + randomNum + "." + fileExtension;
        } else {
            DateToStr = inputName + underscore + DateToStr + underscore + randomNum;
        }

        return DateToStr;
    }

    /**
     * Get the current folder of the running jar file
     * @return jarFilePath
     */
    public String getCurrentJarRunnigFolder() {
        String jarPath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();  // "/home/vagrant/df-data-service-1.0-SNAPSHOT-fat.jar";
        int i = jarPath.lastIndexOf("/");

        if (i > 0) {
            jarPath = jarPath.substring(0, i + 1);
        }

        return jarPath;
    }

    /**
     * Build program parameters for Flink Jar in rest call
     * @param dfJob
     * @param kafkaRestHostName
     * @param SchemaRegistryRestHostName
     * @return
     */
    public static JsonObject getFlinkJarPara(DFJobPOPJ dfJob, String kafkaRestHostName, String SchemaRegistryRestHostName ) {

        String allowNonRestoredState = "false";
        String savepointPath = "";
        String parallelism = "1";
        String entryClass = "";
        String programArgs = "";

        if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_SQLA2A.name()) {
            entryClass = ConstantApp.FLINK_SQL_CLIENT_CLASS_NAME;
            programArgs = String.join(" ", kafkaRestHostName, SchemaRegistryRestHostName,
                    dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_INPUT),
                    dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_OUTPUT),
                    dfJob.getConnectorConfig().get(ConstantApp.PK_FLINK_TABLE_SINK_KEYS),
                    HelpFunc.coalesce(dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_CONSUMER_GROURP),
                            ConstantApp.DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK),
                    // Use 0 as we only support one query in flink
                    "\"" + sqlCleaner(dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_SQL))[0] + "\"");
        } else if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_UDF.name()) {
            entryClass = dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_JAR_CLASS_NAME);
            programArgs = dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_JAR_PARA);
        } else if(dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_Script.name()) {
            entryClass = dfJob.getConnectorConfig().get(ConstantApp.FLINK_TABLEAPI_CLIENT_CLASS_NAME);
            programArgs = dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_JAR_PARA);
        }

        return new JsonObject()
                .put("allowNonRestoredState", allowNonRestoredState)
                .put("savepointPath", savepointPath)
                .put("parallelism", parallelism)
                .put("entryClass", entryClass)
                .put("programArgs", programArgs);
    }

    /**
     * Find mongo sorting options
     * @param routingContext
     * @param sortField
     * @param sortOrderField
     * @return
     */
    public static FindOptions getMongoSortFindOption(RoutingContext routingContext, String sortField, String sortOrderField) {
        String sortName = HelpFunc.coalesce(routingContext.request().getParam(sortField), "_id");
        if(sortName.equalsIgnoreCase("id")) sortName = "_" + sortName; //Mongo use _id
        int sortOrder = HelpFunc.strCompare(
                HelpFunc.coalesce(routingContext.request().getParam(sortOrderField), "ASC"), "ASC", 1, -1);
        return new FindOptions().setSort(new JsonObject().put(sortName, sortOrder));
    }

    public static FindOptions getMongoSortFindOption(RoutingContext routingContext) {
        return getMongoSortFindOption(routingContext, "_sort", "_order");
    }

    /**
     * Get Connector or Transform sub-task status from task array on specific status keys
     * {
     *  "name": "59852ad67985372a792eafce",
     *  "connector": {
     *  "state": "RUNNING",
     *  "worker_id": "127.0.1.1:8083"
     *  },
     *  "tasks": [
     *  {
     *      "state": "RUNNING",
     *      "id": 0,
     *      "worker_id": "127.0.1.1:8083"
     *  }
     *  ]
     * }
     * @param taskStatus Responsed json object
     * @return
     */
    public static String getTaskStatusKafka(JSONObject taskStatus) {

        if(taskStatus.has("connector")) {
            // Check sub-task status ony if the high level task is RUNNING
            if(taskStatus.getJSONObject("connector").getString("state")
                    .equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()) &&
                    taskStatus.has("tasks")) {
                JSONArray subTask = taskStatus.getJSONArray("tasks");
                String status = ConstantApp.DF_STATUS.RUNNING.name();
                for (int i = 0; i < subTask.length(); i++) {
                    if (!subTask.getJSONObject(i).getString("state")
                            .equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())) {
                        status = ConstantApp.DF_STATUS.RWE.name();
                        break;
                    }
                }
                return status;
            } else {
                return taskStatus.getJSONObject("connector").getString("state");
            }
        } else {
            return ConstantApp.DF_STATUS.NONE.name();
        }
    }

    public static String getTaskStatusKafka(JsonObject taskStatus) {

        if(taskStatus.containsKey("connector")) {
            // Check sub-task status ony if the high level task is RUNNING
            if(taskStatus.getJsonObject("connector").getString("state")
                    .equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()) &&
                    taskStatus.containsKey("tasks")) {
                JsonArray subTask = taskStatus.getJsonArray("tasks");
                String status = ConstantApp.DF_STATUS.RUNNING.name();
                for (int i = 0; i < subTask.size(); i++) {
                    if (!subTask.getJsonObject(i).getString("state")
                            .equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())) {
                        status = ConstantApp.DF_STATUS.RWE.name();
                        break;
                    }
                }
                return status;
            } else {
                return taskStatus.getJsonObject("connector").getString("state");
            }
        } else {
            return ConstantApp.DF_STATUS.NONE.name();
        }
    }

    /**
     * Mapping rest state to df status category
     * @param taskStatus
     * @return
     */
    public static String getTaskStatusFlink(JsonObject taskStatus) {
        if(taskStatus.containsKey("state")) {
            // Check sub-task status ony if the high level task is RUNNING
            if(taskStatus.getString("state").equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()) &&
                    taskStatus.containsKey("vertices")) {
                JsonArray subTask = taskStatus.getJsonArray("vertices");
                String status = ConstantApp.DF_STATUS.RUNNING.name();
                for (int i = 0; i < subTask.size(); i++) {
                    if (!subTask.getJsonObject(i).getString("status")
                            .equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())) {
                        status = ConstantApp.DF_STATUS.RWE.name();
                        break;
                    }
                }
                return status;
            } else {
                return taskStatus.getString("state");
            }
        } else {
            return ConstantApp.DF_STATUS.NONE.name();
        }
    }
    /**
     * Mapping livy statement rest state to df status category
     * @param taskStatus
     * @return
     */
    public static String getTaskStatusSpark(JsonObject taskStatus) {
        if(taskStatus.containsKey("state")) {
            // Check sub-task status ony if the high level task is RUNNING
            String restState = taskStatus.getString("state").toUpperCase();
            String returnState;
            switch(restState) {
                case "RUNNING":
                case "CANCELLING":
                    returnState = ConstantApp.DF_STATUS.RUNNING.name();
                    break;
                case "WAITING":
                    returnState = ConstantApp.DF_STATUS.UNASSIGNED.name();
                    break;
                case "AVAILABLE": {
                    if(taskStatus.getJsonObject("output").getString("status").equalsIgnoreCase("ok")){
                        returnState = ConstantApp.DF_STATUS.FINISHED.name();
                    } else {
                        // Some sql runtime error will have output status as "error", but statement state as "available"
                        returnState = ConstantApp.DF_STATUS.FAILED.name();
                    }
                    break;
                }
                case "ERROR":
                    returnState = ConstantApp.DF_STATUS.FAILED.name();
                    break;
                case "CANCELLED":
                    returnState = ConstantApp.DF_STATUS.CANCELED.name();
                    break;
                default:
                    returnState = restState;
                    break;
            }
            return returnState;
        } else {
            return ConstantApp.DF_STATUS.NONE.name();
        }
    }

    /**
     * Search the json object in the list of keys contains specified values
     * @param keyRoot
     * @param keyString
     * @param containsValue
     * @return searchCondition
     */
    public static JsonObject getContainsTopics(String keyRoot, String keyString, String containsValue) {
        JsonArray ja = new JsonArray();
        for(String key : keyString.split(",")) {
                ja.add(new JsonObject()
                        .put(keyRoot+ "." + key,
                                new JsonObject().put("$regex", ".*" + containsValue + ".*")
                        )
                );
        }
        return new JsonObject().put("$or", ja);

    }

    /**
     * Used to format the livy result to better rich text so that it can show in the web ui better
     * @param livyStatementResult
     * @return
     */
    public static String livyTableResultToRichText(JsonObject livyStatementResult) {
        String tableHeader =
                "<style type=\"text/css\">" +
                        ".myOtherTable { background-color:#FFFFFF;border-collapse:collapse;color:#000}" +
                        ".myOtherTable th { background-color:#99ceff;color:black;width:50%; }" +
                        ".myOtherTable td, .myOtherTable th { padding:5px;border:0; }" +
                        ".myOtherTable td { border-bottom:1px dotted #BDB76B; }" +
                        "</style><table class=\"myOtherTable\">";
        //String tableHeader = "<table border=\"1\",cellpadding=\"4\">";
        String tableTrailer = "</table>";
        String dataRow = "";

        if (livyStatementResult.getJsonObject("output").containsKey("data")) {
            JsonObject dataJason = livyStatementResult.getJsonObject("output").getJsonObject("data");

            if (livyStatementResult.getString("code").contains("%table")) { //livy magic word %table
                JsonObject output = dataJason.getJsonObject("application/vnd.livy.table.v1+json");
                JsonArray header = output.getJsonArray("headers");
                JsonArray data = output.getJsonArray("data");
                String headerRow = "<tr><th>";

                if (data.size() == 0) return "";

                String separator = "</th><th>";
                for (int i = 0; i < header.size(); i++) {
                    if(i == header.size() - 1) separator = "";
                    headerRow = headerRow + header.getJsonObject(i).getString("name") + separator;
                }

                headerRow = headerRow + "</th></tr>";

                for (int i = 0; i < data.size(); i++) {
                    dataRow = dataRow + jsonArrayToString(data.getJsonArray(i),
                            "<tr><td>", "</td><td>", "</td></tr>");
                }

                return tableHeader + headerRow + dataRow + tableTrailer;

            } else if (livyStatementResult.getString("code").contains("%json")) { // livy magic word %json
                JsonArray data = dataJason.getJsonArray("application/json");

                if (data.size() == 0) return "";

                for (int i = 0; i < data.size(); i++) {
                    dataRow = dataRow + jsonArrayToString(data.getJsonArray(i),
                            "<tr><td>", "</td><td>", "</td></tr>");
                }

                return tableHeader + dataRow + tableTrailer;

            } else { // livy no magic word
                String data = dataJason.getString("text/plain");
                return data.trim().replaceAll("\\[|]", "")
                        .replaceAll(",Row", ",</br>Row");
            }
        } else {
            return "";
        }
    }

    /**
     * Used to format Livy statement result to a list a fields and types for schema creation. Only support %table now
     * @param livyStatementResult
     * @return string of fields with type
     */
    public static String livyTableResultToAvroFields(JsonObject livyStatementResult, String subject) {

        if (livyStatementResult.getJsonObject("output").containsKey("data")) {
            JsonObject dataJason = livyStatementResult.getJsonObject("output").getJsonObject("data");

            if (livyStatementResult.getString("code").contains("%table")) { //livy magic word %table
                JsonObject output = dataJason.getJsonObject("application/vnd.livy.table.v1+json");
                JsonArray header = output.getJsonArray("headers");

                for (int i = 0; i < header.size(); i++) {
                    header.getJsonObject(i).put("type", typeHive2Avro(header.getJsonObject(i).getString("type")));
                }

                JsonObject schema = new JsonObject().put("type", "record").put("name", subject).put("fields", header);

                return schema.toString();
            }
        } else {
            return "";
        }
        return "";
    }

    public static String mapToJsonStringFromHashMapD2U(HashMap<String, String> hm) {
        return mapToJsonFromHashMapD2U(hm).toString();
    }

    /**
     * Utility to remove dot from json attribute to underscore for web ui
     * @param hm
     * @return
     */
    public static JsonObject mapToJsonFromHashMapD2U(HashMap<String, String> hm) {
        JsonObject json = new JsonObject();
        for (String key : hm.keySet()) {
            json.put(key.replace('.','_'), hm.get(key)); // replace . in keys so that kafka connect property with . has no issues
        }
        return json;
    }

    /**
     * Utility to replace underscore from json attribute to dot for kafka connect
     * @param hm
     * @return
     */
    public static String mapToJsonStringFromHashMapU2D(HashMap<String, String> hm) {
        return mapToJsonFromHashMapU2D(hm).toString();
    }

    /**
     * Utility to replace underscore from json attribute to dot for kafka connect
     * @param hm
     * @return
     */
    public static JsonObject mapToJsonFromHashMapU2D(HashMap<String, String> hm) {
        JsonObject json = new JsonObject();
        for (String key : hm.keySet()) {
            json.put(key.replace('_','.'), hm.get(key)); // replace . in keys so that kafka connect property with . has no issues
        }
        return json;
    }

    /**
     * Utility to extract HashMap from json for DFPOPJ
     * @param jo
     * @return
     */
    public static HashMap<String, String> mapToHashMapFromJson( JsonObject jo) {
        HashMap<String, String> hm = new HashMap();
        for (String key : jo.fieldNames()) {
            hm.put(key, jo.getString(key));
        }
        return hm;
    }

    /**
     * This is mainly to bypass security control for response.
     * @param response
     */
    public static HttpServerResponse responseCorsHandleAddOn(HttpServerResponse response) {
        return response
                .putHeader("Access-Control-Allow-Origin", "*")
                .putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
                .putHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, X-Total-Count")
                .putHeader("Access-Control-Expose-Headers", "X-Total-Count")
                .putHeader("Access-Control-Max-Age", "60")
                .putHeader("X-Total-Count", "1" ) // Overwrite this in the sub call
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET);
    }

    /**
     * Convert string to Json format by remove first " and end " and replace \" to "
     * @param srcStr String to format
     * @return String formatted
     */
    public static String stringToJsonFormat(String srcStr) {
        if (srcStr.isEmpty()) return "[]";
        // .replace("\"\"", "\"") is used to fix issue on STRING SCHEMA,
        // where the origin schema show as "schema":"\"string\"" == replace as ==> "schema":""string""
        // Then, replace as "schema":"string"
        return srcStr.replace("\"{", "{").replace("}\"", "}").replace("\\\"", "\"").replace("\"\"", "\"");
    }

    /**
     * Comparing string ease of lambda expression
     * @param a
     * @param b
     * @param <T>
     * @return object
     */
    public static <T> T strCompare(String a, String b, T c, T d) {
        if (a.equalsIgnoreCase(b)) return c;
        else return d;
    }

    /**
     * Sort JsonArray when we do not use mongodb
     * @param routingContext
     * @param jsonArray
     * @return
     */
    public static JSONArray sortJsonArray(RoutingContext routingContext, JSONArray jsonArray) {

        String sortKey = HelpFunc.coalesce(routingContext.request().getParam("_sort"), "id");
        String sortOrder = HelpFunc.coalesce(routingContext.request().getParam("_order"), "ASC");

        JSONArray sortedJsonArray = new JSONArray();

        List<JSONObject> jsonValues = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            jsonValues.add(jsonArray.getJSONObject(i));
        }
        Collections.sort( jsonValues, new Comparator<JSONObject>() {

            @Override
            public int compare(JSONObject a, JSONObject b) {
                String valA = new String();
                String valB = new String();

                try {
                    valA = a.get(sortKey).toString();
                    valB = b.get(sortKey).toString();
                }
                catch (JSONException e) {
                    e.printStackTrace();
                }

                if(sortOrder.equalsIgnoreCase("ASC")) {
                    return valA.compareTo(valB);
                } else {
                    return -valA.compareTo(valB);
                }
            }
        });

        for (int i = 0; i < jsonArray.length(); i++) {
            sortedJsonArray.put(jsonValues.get(i));
        }
        return sortedJsonArray;
    }

    /**
     * Get input of sql statements and remove comments line, extract \n and extra ;
     * @param sqlInput
     * @return array of cleaned sql statement without ;
     */
    public static String[] sqlCleaner(String sqlInput) {
        //Use @ to create extra space and \n for removing comments
        sqlInput = sqlInput.replaceAll("\n", "@\n");
        String cleanedSQL = "";
        for(String line : sqlInput.split("\n")) { // remove all comments in each line
            cleanedSQL = cleanedSQL + StringUtils.substringBefore(line, "--");
        }
        return cleanedSQL.replace("@", " ").split(";");
    }

    /**
     * Convert spark SQL to pyspark code. If stream the result back is needed, add additional code.
     * @return
     */
    public static String sqlToPySpark(String[] sqlList, boolean streamBackFlag, String streamPath) {

        String pySparkCode = "";
        if(!streamPath.startsWith("file://")) streamPath = "file://" + streamPath;

        for(int i = 0; i < sqlList.length; i++) {
            if(i == sqlList.length - 1) { // This is the last query
                // Check if we need to stream the result set
                if(streamBackFlag) {
                    pySparkCode = pySparkCode + "sqlContext.sql(\"" + sqlList[i] +
                            "\").coalesce(1).write.format(\"json\").mode(\"overwrite\").save(\"" + streamPath + "\")\n";
                }
                // Keep result only for the last query and only top 10 rows
                pySparkCode = pySparkCode + "a = sqlContext.sql(\"" + sqlList[i] + "\").take(10)\n%table a";
            } else {
                pySparkCode = pySparkCode + "sqlContext.sql(\"" + sqlList[i] + "\")\n";
            }
        }

        return pySparkCode;
    }

    /**
     * Convert Json Array to String with proper begin, separator, and end string.
     * @param ja
     * @param begin
     * @param separator
     * @param end
     * @return
     */
    public static String jsonArrayToString(JsonArray ja, String begin, String separator, String end) {
        for (int i = 0; i < ja.size(); i++) {
            if(i == ja.size() - 1) separator = "";
            begin = begin + ja.getValue(i).toString() + separator;
        }
        return begin + end;
    }

    /**
     * Map hive type (from Livy statement result) to avro schema type
     * @param hiveType
     * @return type in Avro
     */
    private static String typeHive2Avro(String hiveType) {

        String avroType;
        switch(hiveType) {
            case "NULL_TYPE":
                avroType = "null";
                break;
            case "BYTE_TYPE":
                avroType = "bytes";
                break;
            case "DATE_TYPE":
            case "TIMESTAMP_TYPE":
            case "STRING_TYPE":
            case "VARCHAR_TYPE":
            case "CHAR_TYPE":
                avroType = "string";
                break;
            case "BOOLEAN_TYPE":
                avroType = "boolean";
                break;
            case "INT_TYPE":
                avroType = "int";
                break;
            case "DOUBLE_TYPE":
                avroType = "double";
                break;
            case "FLOAT_TYPE":
            case "DECIMAL_TYPE":
                avroType = "float";
                break;
            default:
                avroType = "string";
                break;
        }
        return avroType;
    }

    /**
     * Upload Flink client to flink rest server
     * @param postURL
     * @param jarFilePath
     * @return
     */
    public static String uploadJar(String postURL, String jarFilePath) {
        HttpResponse<String> jsonResponse = null;
        try {
            jsonResponse = Unirest.post(postURL)
                    .field("file", new File(jarFilePath))
                    .asString();
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        JsonObject response = new JsonObject(jsonResponse.getBody());
        if(response.containsKey("filename")) {
            return response.getString("filename");
        } else {
            return "";
        }
    }
    /**
     * Helper class to update mongodb status
     * @param mongo
     * @param COLLECTION
     * @param updateJob
     * @param LOG
     */
    public static void updateRepoWithLogging(MongoClient mongo, String COLLECTION, DFJobPOPJ updateJob, Logger LOG) {
        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", updateJob.toJson()), v -> {
                    if (v.failed()) {
                        LOG.error(DFAPIMessage.logResponseMessage(9003, updateJob.getId() + "cause:" + v.cause()));
                    } else {
                        LOG.info(DFAPIMessage.logResponseMessage(1021, updateJob.getId()));
                    }
                }
        );
    }
}

package com.datafibers.util;

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
import io.vertx.ext.web.RoutingContext;
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
        cleanedJsonString = cleanJsonConfigIgnored(JSON_STRING, "connectorConfig_", "config_ignored");
        cleanedJsonString = convertTopicsFromArrayToString(cleanedJsonString, ConstantApp.PK_DF_TOPICS_ALIAS);
        System.out.println("cleanedJsonString = " + cleanedJsonString);
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

    public static String getTaskStatusFlink(JSONObject taskStatus) {
        if(taskStatus.has("state")) {
            // Check sub-task status ony if the high level task is RUNNING
            if(taskStatus.getString("state").equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()) &&
                    taskStatus.has("vertices")) {
                JSONArray subTask = taskStatus.getJSONArray("vertices");
                String status = ConstantApp.DF_STATUS.RUNNING.name();
                for (int i = 0; i < subTask.length(); i++) {
                    if (!subTask.getJSONObject(i).getString("status")
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
     * Search the json object in the list of keys contains specified values
     * @param jo
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

    public static String mapToJsonStringFromHashMapD2U(HashMap<String, String> hm) {
        return mapToJsonFromHashMapD2U(hm).toString();
    }

    public static JsonObject mapToJsonFromHashMapD2U(HashMap<String, String> hm) {
        JsonObject json = new JsonObject();
        for (String key : hm.keySet()) {
            json.put(key.replace('.','_'), hm.get(key)); // replace . in keys so that kafka connect property with . has no issues
        }
        return json;
    }

    public static String mapToJsonStringFromHashMapU2D(HashMap<String, String> hm) {
        return mapToJsonFromHashMapU2D(hm).toString();
    }

    public static JsonObject mapToJsonFromHashMapU2D(HashMap<String, String> hm) {
        JsonObject json = new JsonObject();
        for (String key : hm.keySet()) {
            json.put(key.replace('_','.'), hm.get(key)); // replace . in keys so that kafka connect property with . has no issues
        }
        return json;
    }

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

    public static String uploadJar(String postURL, String jarFilePath) {
        HttpResponse<String> jsonResponse = null;
        try {
            jsonResponse = Unirest.post(postURL)
                    .field("file", new File(jarFilePath))
                    .asString();
        } catch (UnirestException e) {
            e.printStackTrace();
        }
        return jsonResponse.getBody();
    }
}

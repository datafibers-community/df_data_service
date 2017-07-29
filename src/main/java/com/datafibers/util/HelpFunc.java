package com.datafibers.util;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.web.RoutingContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
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
     * Prepare response message in better JSON format
     *
     * @param responseCode
     * @return ResponseMsg
     */
    public static String responseMsg(int responseCode) {
        String responseMsg;
        switch(responseCode) {
            case 1000: responseMsg = "INFO - CREATED";
                break;
            case 1001: responseMsg = "INFO - UPDATED";
                break;
            case 1002: responseMsg = "INFO - DELETED";
                break;
            case 9000: responseMsg = "ERROR - ID_IS_NULL_IN_REQUEST";
                break;
            case 9001: responseMsg = "ERROR - ID_NOT_FOUND_IN_REPO";
                break;
            case 9002: responseMsg = "ERROR - ID_SEARCH_EXCEPTION_IN_REPO";
                break;
            case 9003: responseMsg = "ERROR - ID_UPDATE_EXCEPTION_IN_REPO";
                break;
            case 9004: responseMsg = "ERROR - ID_DELETE_EXCEPTION_IN_REPO";
                break;
            case 9005: responseMsg = "ERROR - ID_INSERT_EXCEPTION_IN_REPO";
                break;
            case 9006: responseMsg = "ERROR - POST_CLIENT_EXCEPTION";
                break;
            case 9007: responseMsg = "ERROR - ID_NOT_FOUND_IN_KAFKA_CONNECT";
                break;
            default: responseMsg = "ERROR - INVALID_RESPONSE_CODE_PARAMETER";
                break;
        }
        return Json.encodePrettily(new JsonObject()
                .put("code", String.format("%06d", responseCode))
                .put("message", responseMsg));
    }

    public static String responseMsg(String responseKey, String responseVal) {
        return Json.encodePrettily(new JsonObject().put(responseKey, responseVal));
    }

    /**
     * This function will search JSONSTRING to find patterned keys_1..n. If it has key_ignored_mark subkey, the element
     * will be removed. For example {"connectorConfig_1":{"config_ignored":"test"}, "connectorConfig_2":{"test":"test"}}
     * will be cleaned as {"connectorConfig":{"test":"test"}}
     *
     * This will also remove any comments in "\/* *\/"
     *
     * @param JSON_STRING
     * @param key_ingored_mark: If the
     * @return cleaned json string
     */
    public static String cleanJsonConfig(String JSON_STRING, String key_pattern, String key_ingored_mark) {
        JSONObject json = new JSONObject(JSON_STRING.replaceAll("\\s+?/\\*.*?\\*/", ""));
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
     * A default short-cut call for cleanJsonConfig
     * @param JSON_STRING
     * @return cleaned json string
     */
    public static String cleanJsonConfig(String JSON_STRING) {
        return cleanJsonConfig(JSON_STRING, "connectorConfig_", "config_ignored");
    }

    /**
     * Print list of Properties
     * @param prop
     * @return
     */
    public static String getPropertyAsString(Properties prop) {
        StringWriter writer = new StringWriter();
        prop.list(new PrintWriter(writer));
        return writer.getBuffer().toString();
    }

    /**
     * Loop the enum of ConnectType to add all connects to the list by l
     */
    public static void addSpecifiedConnectTypetoList(List<String> list, String type_regx) {

        for (ConstantApp.DF_CONNECT_TYPE item : ConstantApp.DF_CONNECT_TYPE.values()) {
            if(item.name().matches(type_regx)) list.add(item.name());
        }
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
                .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8);
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
}

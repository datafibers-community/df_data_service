package com.datafibers.util;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to keep all API response message
 */
public final class DFAPIMessage {

    private static final Map<Integer, String> messageMap;

    static
    {
        messageMap = new HashMap<Integer, String>();

        messageMap.put(1000, "INFO - RECORD_CREATED_IN_REPO");
        messageMap.put(1001, "INFO - RECORD_UPDATED_IN_REPO");
        messageMap.put(1002, "INFO - RECORD_DELETED_IN_REPO");
        messageMap.put(9000, "EXCP - ID_IS_NULL_IN_REQUEST");
        messageMap.put(9001, "EXCP - ID_NOT_FOUND_IN_REPO");
        messageMap.put(9002, "EXCP - ID_SEARCH_EXCEPTION_IN_REPO");
        messageMap.put(9003, "EXCP - ID_UPDATE_EXCEPTION_IN_REPO");
        messageMap.put(9004, "EXCP - ID_DELETE_EXCEPTION_IN_REPO");
        messageMap.put(9005, "EXCP - ID_INSERT_EXCEPTION_IN_REPO");
        messageMap.put(9006, "EXCP - POST_CLIENT_EXCEPTION");
        messageMap.put(9007, "EXCP - ID_NOT_FOUND_IN_KAFKA_CONNECT");
    }

    public static String getResponseMessage(int responseCode, String comment) {

        int code;
        String message;

        if (messageMap.containsKey(responseCode)) {
            code = responseCode;
            message = messageMap.get(responseCode);
            if (!comment.equalsIgnoreCase("")) message = message + "[" + comment + "]";
        } else {
            code = 9999;
            message = "EXCP - INVALID_RESPONSE_CODE_PARAMETER";
        }

        return Json.encodePrettily(new JsonObject()
                .put("code", String.format("%06d", code))
                .put("message", messageMap.get(message)));
    }

    public static String getResponseMessage(int responseCode) {
        return getResponseMessage(responseCode, "");
    }

    public static String getCustomizedResponseMessage(String responseKey, String responseVal) {
        return Json.encodePrettily(new JsonObject().put(responseKey, responseVal));
    }

}

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
        messageMap = new HashMap<>();

        messageMap.put(1000, "INFO - RECORD_CREATED_IN_REPO");
        messageMap.put(1001, "INFO - RECORD_UPDATED_IN_REPO");
        messageMap.put(1002, "INFO - RECORD_DELETED_IN_REPO");
        messageMap.put(1003, "INFO - RECORD_FOUND_IN_REPO");
        messageMap.put(1004, "INFO - CONNECT_CONFIG_NAME_SET_TO_CUID");
        messageMap.put(1005, "INFO - FLINK_JOB_SUBMIT_SUCCESSFULLY");
        messageMap.put(1006, "INFO - FLINK_JOB_CANCEL_SUCCESSFULLY");
        messageMap.put(1007, "INFO - NO_CHANGES_IN_CONFIG");
        messageMap.put(1008, "INFO - DF_META_SCHEMA_CREATED_IN_SCHEMA_REGISTRY");
        messageMap.put(1009, "INFO - DF_META_SCHEMA_EXISTED_IN_SCHEMA_REGISTRY");
        messageMap.put(1010, "INFO - DF_METADATA_SINK_STARTED_SUCCESSFULLY");
        messageMap.put(1011, "INFO - CONNECTS_TO_IMPORT_FROM_KAFKA_CONNECT");
        messageMap.put(1012, "INFO - IMPORT_CONNECTS_TO_REPO_SUCCESSFULLY");
        messageMap.put(1013, "INFO - NO_ACTIVE_CONNECTS_TO_IMPORT");
        messageMap.put(1014, "INFO - IMPORT_ACTIVE_CONNECTS_COMPLETE_AT_STARTUP");
        messageMap.put(1015, "INFO - IMPORT_ACTIVE_CONNECTS_COMPLETE_AT_STARTUP");
        messageMap.put(1016, "INFO - FOUND_CHANGES_IN_CONFIG");
        messageMap.put(1017, "INFO - SCHEMA_IS_UPDATED");
        messageMap.put(9000, "EXCP - ID_IS_NULL_IN_REQUEST");
        messageMap.put(9001, "EXCP - ID_NOT_FOUND_IN_REPO");
        messageMap.put(9002, "EXCP - ID_SEARCH_EXCEPTION_IN_REPO");
        messageMap.put(9003, "EXCP - ID_UPDATE_EXCEPTION_IN_REPO");
        messageMap.put(9004, "EXCP - ID_DELETE_EXCEPTION_IN_REPO");
        messageMap.put(9005, "EXCP - ID_INSERT_EXCEPTION_IN_REPO");
        messageMap.put(9006, "EXCP - REST_CLIENT_EXCEPTION");
        messageMap.put(9007, "EXCP - ID_NOT_FOUND_IN_KAFKA_CONNECT");
        messageMap.put(9008, "EXCP - KAFKA_CONNECT_NOT_ENABLED");
        messageMap.put(9009, "EXCP - FLINK_NOT_ENABLED");
        messageMap.put(9010, "EXCP - FLINK_JOB_SUBMIT_EXCEPTION");
        messageMap.put(9011, "EXCP - FLINK_JOB_ID_NOT_FOUND");
        messageMap.put(9012, "EXCP - FLINK_JOB_CANCEL_EXCEPTION");
        messageMap.put(9013, "EXCP - PROGRAM_INVOCATION_EXCEPTION");
        messageMap.put(9014, "EXCP - MONGODB_CLIENT_EXCEPTION");
        messageMap.put(9015, "EXCP - METADATA_SINK_START_EXCEPTION");
        messageMap.put(9016, "EXCP - IMPORT_CONNECTS_TO_REPO_FAILED");
        messageMap.put(9017, "EXCP - UPDATE_POPJ_STATUS_FAILED");
        messageMap.put(9018, "EXCP - UID_SEARCH_EXCEPTION_IN_REPO");
        messageMap.put(9019, "EXCP - WEB_UI_EXCEPTION");
        messageMap.put(9020, "EXCP - CML_PARSER_EXCEPTION");
        messageMap.put(9021, "EXCP - ID_UPDATE_EXCEPTION_IN_KAFKA_CONNECT");
        messageMap.put(9022, "EXCP - ID_DELETE_EXCEPTION_IN_KAFKA_CONNECT");
        messageMap.put(9023, "EXCP - SCHEMA_UPDATE_FAILED");
    }

    public static String getResponseMessage(int responseCode, String comments) {

        int code;
        String message;
        JsonObject response = new JsonObject();

        if (messageMap.containsKey(responseCode)) {
            code = responseCode;
            message = messageMap.get(responseCode);

        } else {
            code = 9999;
            message = "EXCP - INVALID_RESPONSE_CODE_PARAMETER";
        }

        response.put("code", String.format("%06d", code)).put("message", messageMap.get(responseCode));
        if (!comments.equalsIgnoreCase("")) response.put("comments", comments);

        return Json.encodePrettily(response);
    }

    public static String getResponseMessage(int responseCode) {
        return getResponseMessage(responseCode, "");
    }

    public static String logResponseMessage(int responseCode, String comments) {
        return new JsonObject(getResponseMessage(responseCode, comments)).toString();
    }

    public static String getCustomizedResponseMessage(String responseKey, String responseVal) {
        return Json.encodePrettily(new JsonObject().put(responseKey, responseVal));
    }

}

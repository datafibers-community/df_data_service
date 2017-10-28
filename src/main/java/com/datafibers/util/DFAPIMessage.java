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

        messageMap.put(1000, "RECORD_CREATED_IN_REPO");
        messageMap.put(1001, "RECORD_UPDATED_IN_REPO");
        messageMap.put(1002, "RECORD_DELETED_IN_REPO");
        messageMap.put(1003, "RECORD_FOUND_IN_REPO");
        messageMap.put(1004, "CONNECT_CONFIG_NAME_SET_TO_CUID");
        messageMap.put(1005, "FLINK_JOB_SUBMIT_SUCCESSFULLY");
        messageMap.put(1006, "FLINK_JOB_CANCEL_SUCCESSFULLY");
        messageMap.put(1007, "NO_CHANGES_IN_CONFIG");
        messageMap.put(1008, "DF_META_SCHEMA_CREATED_IN_SCHEMA_REGISTRY");
        messageMap.put(1009, "DF_META_SCHEMA_EXISTED_IN_SCHEMA_REGISTRY");
        messageMap.put(1010, "DF_META_SINK_STARTED_SUCCESSFULLY");
        messageMap.put(1011, "CONNECTS_TO_IMPORT_FROM_KAFKA_CONNECT");
        messageMap.put(1012, "IMPORT_CONNECTS_TO_REPO_SUCCESSFULLY");
        messageMap.put(1013, "NO_ACTIVE_CONNECTS_TO_IMPORT");
        messageMap.put(1014, "IMPORT_ACTIVE_CONNECTS_COMPLETED_AT_STARTUP");
        messageMap.put(1015, "IMPORT_ACTIVE_CONNECTS_STARTED_AT_STARTUP");
        messageMap.put(1016, "FOUND_CHANGES_IN_CONFIG");
        messageMap.put(1017, "SCHEMA_IS_UPDATED");
        messageMap.put(1018, "DEFAULT_CONNECTOR_CLASS_USED");
        messageMap.put(1019, "REGULAR_UPDATE_CONNECT_STATUS_FOUND_CHANGES");
        messageMap.put(1020, "REGULAR_UPDATE_CONNECT_STATUS_NO_CHANGES");
        messageMap.put(1021, "REGULAR_UPDATE_TRANSFORM_STATUS_FOUND_CHANGES");
        messageMap.put(1022, "REGULAR_UPDATE_TRANSFORM_STATUS_NO_CHANGES");
        messageMap.put(1023, "RETURNED_CONNECT_STATUS_IN_DETAIL");
        messageMap.put(1024, "RETURNED_TRANSFORM_STATUS_IN_DETAIL");
        messageMap.put(1025, "SCHEMA_IS_CREATED");
        messageMap.put(1026, "SCHEMA_IS_DELETED");
        messageMap.put(1027, "TOPIC_IS_SUBSCRIBED");
        messageMap.put(1028, "DF_JAR_IS_UPLOADED_FOR_FLINK");
        messageMap.put(9000, "ID_IS_NULL_IN_REQUEST");
        messageMap.put(9001, "ID_NOT_FOUND_IN_REPO");
        messageMap.put(9002, "ID_SEARCH_EXCEPTION_IN_REPO");
        messageMap.put(9003, "ID_UPDATE_EXCEPTION_IN_REPO");
        messageMap.put(9004, "ID_DELETE_EXCEPTION_IN_REPO");
        messageMap.put(9005, "ID_INSERT_EXCEPTION_IN_REPO");
        messageMap.put(9006, "REST_CLIENT_REQUEST_EXCEPTION");
        messageMap.put(9007, "ID_NOT_FOUND_IN_KAFKA_CONNECT");
        messageMap.put(9008, "KAFKA_CONNECT_NOT_ENABLED");
        messageMap.put(9009, "FLINK_NOT_ENABLED");
        messageMap.put(9010, "FLINK_JOB_SUBMIT_EXCEPTION");
        messageMap.put(9011, "FLINK_JOB_ID_NOT_FOUND");
        messageMap.put(9012, "FLINK_JOB_CANCEL_EXCEPTION");
        messageMap.put(9013, "PROGRAM_INVOCATION_EXCEPTION");
        messageMap.put(9014, "MONGODB_CLIENT_EXCEPTION");
        messageMap.put(9015, "METADATA_SINK_START_EXCEPTION");
        messageMap.put(9016, "IMPORT_CONNECTS_TO_REPO_FAILED");
        messageMap.put(9017, "UPDATE_POPJ_STATUS_FAILED");
        messageMap.put(9018, "UID_SEARCH_EXCEPTION_IN_REPO");
        messageMap.put(9019, "WEB_UI_EXCEPTION");
        messageMap.put(9020, "CML_PARSER_EXCEPTION");
        messageMap.put(9021, "ID_UPDATE_EXCEPTION_IN_KAFKA_CONNECT");
        messageMap.put(9022, "ID_DELETE_EXCEPTION_IN_KAFKA_CONNECT");
        messageMap.put(9023, "SCHEMA_UPDATE_FAILED");
        messageMap.put(9024, "DF_INSTALLED_DATA_NOT_FOUND");
        messageMap.put(9025, "ID_UPDATE_EXCEPTION_IN_FLINK_REST");
        messageMap.put(9026, "ID_DELETE_EXCEPTION_IN_FLINK_REST");
        messageMap.put(9027, "FORWARD_GET_ALL_SCHEMA");
        messageMap.put(9028, "REST_CLIENT_EXCEPTION");
        messageMap.put(9029, "REST_CLIENT_RESPONSE_EXCEPTION");
        messageMap.put(9030, "AVRO_CONSUMER_TOPIC_SUBSCRIBE_ERROR");
        messageMap.put(9031, "AVRO_CONSUMER_EXCEPTION");
        messageMap.put(9032, "NOT_VALID_STATUS_TO_PAUSE_OR_RESUME");
        messageMap.put(9033, "TASK_PAUSE_OR_RESUME_FAILED_REVERTED_PRE-STATUS");
        messageMap.put(9034, "FAILED_TO_REVERT_PRE-STATUS");
        messageMap.put(9035, "FAILED_TO_GET_DF_JAR_ID_FROM_REPO");

    }

    public static String getResponseMessage(int responseCode, String comments) {
        String messageType = responseCode >= 9000 ? "ERROR" : "INFO";
        JsonObject response = new JsonObject();
        response.put("message", messageType + " " + String.format("%04d", responseCode) + " - " + messageMap.get(responseCode));
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

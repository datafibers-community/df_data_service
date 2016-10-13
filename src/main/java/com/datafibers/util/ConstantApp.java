package com.datafibers.util;

public final class ConstantApp {

    // DF Service generic settings
    public static final int REGULAR_REFRESH_STATUS_TO_REPO = 10000;

    // Vertx REST Client settings
    public static final int REST_CLIENT_CONNECT_TIMEOUT = 1000;
    public static final int REST_CLIENT_GLOBAL_REQUEST_TIMEOUT = 5000;
    public static final Boolean REST_CLIENT_KEEP_LIVE = true;
    public static final int REST_CLIENT_MAX_POOL_SIZE = 500;

    // DF REST endpoint URLs for all processors, connects and transforms
    public static final String DF_PROCESSOR_REST_URL = "/api/df/processor";

    // DF Connects REST endpoint URLs
    public static final String DF_CONNECTS_REST_URL = "/api/df/ps";
    public static final String DF_CONNECTS_INSTALLED_CONNECTS_REST_URL = "/api/df/installed_connects";
    public static final String DF_CONNECTS_REST_URL_WILD = "/api/df/ps*";
    public static final String DF_CONNECTS_REST_URL_WITH_ID = DF_CONNECTS_REST_URL + "/:id";

    // DF Transforms REST endpoint URLs
    public static final String DF_TRANSFORMS_REST_URL = "/api/df/tr";
    public static final String DF_TRANSFORMS_INSTALLED_TRANSFORMS_REST_URL = "/api/df/installed_transforms";
    public static final String DF_TRANSFORMS_REST_URL_WILD = "/api/df/tr*";
    public static final String DF_TRANSFORMS_REST_URL_WITH_ID = DF_TRANSFORMS_REST_URL + "/:id";
    public static final String DF_TRANSFORMS_UPLOAD_FILE_REST_URL_WILD = "/api/df/uploaded_files*";
    public static final String DF_TRANSFORMS_UPLOAD_FILE_REST_URL = "/api/df/uploaded_files";

    // Kafka CONNECT endpoint URLs
    public static final String KAFKA_CONNECT_REST_URL = "/connectors";
    public static final String KAFKA_CONNECT_PLUGIN_REST_URL = "/connector-plugins";
    public static String KAFKA_CONNECT_PLUGIN_CONFIG = "/connectors/CONNECTOR_NAME_PLACEHOLDER/config";

    // Kafka Other default settings
    public static String DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK = "df_trans_flink_group_id";

    // HTTP req/res constants
    public static final String CONTENT_TYPE = "content-type";
    public static final String APPLICATION_JSON_CHARSET_UTF_8 = "application/json; charset=utf-8";
    public static final String TEXT_HTML = "text/html";

    // HTTP status codes
    public static final int STATUS_CODE_OK = 200;
    public static final int STATUS_CODE_OK_CREATED = 201;
    public static final int STATUS_CODE_OK_NO_CONTENT = 204;
    public static final int STATUS_CODE_BAD_REQUEST = 400;
    public static final int STATUS_CODE_NOT_FOUND = 404;
    public static final int STATUS_CODE_CONFLICT = 409;

    public enum DF_STATUS {
        UNASSIGNED,         // The Kafka connector/task has not yet been assigned to a worker.
        RUNNING,            // The Kafka connector/task is running.
        PAUSED,             // The Kafka connector/task has been administratively paused.
        FAILED,             // The Kafka connector/task has failed (usually by raising an exception, which is reported in the status output).
        LOST,               // The Kafka connect restart and lost the connector job in DF repository. These jobs should be removed manually.
        NONE
    }

    public enum DF_CONNECT_TYPE {
        KAFKA_SOURCE,       // Kafka Connector import data into Kafka
        KAFKA_SINK,         // Kafka Connector export data out of Kafka
        EVENTBUS_SOURCE,    // The plugin import data into Vertx Event Bus
        EVENTBUS_SINK,      // The plugin export data out of Vertx Event Bus
        HDFS_SOURCE,        // The plugin import data into HDFS
        HDFS_SINK,          // The plugin export data out of HDFS
        HIVE_SOURCE,        // The plugin import data into Hive
        HIVE_SINK,          // The plugin export data out of Hive
        FLINK_TRANS,        // Flink streaming SQL
        FLINK_JOINS,        // Flink streaming of Data Join
        FLINK_UDF,          // Flink user defined jar/program
        SPARK_TRANS,        // Spark streaming SQL
        SPARK_JOINS,        // Spark streaming of Data Join
        SPARK_UDF,          // Spark user defined jar/program
        HIVE_TRANS,         // Hive batch SQL
        HIVE_JOINS,         // Hive batch join
        NONE
    }

}

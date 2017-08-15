package com.datafibers.util;

import java.util.HashMap;
import java.util.Map;

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
    public static final String DF_PROCESSOR_REST_URL_WILD = "/api/df/processor*";
    public static final String DF_PROCESSOR_REST_URL_WITH_ID = DF_PROCESSOR_REST_URL + "/:id";

    // DF REST endpoint URLs for all processors installed information and default config
    public static final String DF_PROCESSOR_CONFIG_REST_URL = "/api/df/config";
    public static final String DF_PROCESSOR_CONFIG_REST_URL_WILD = "/api/df/config*";
    public static final String DF_PROCESSOR_CONFIG_REST_URL_WITH_ID = DF_PROCESSOR_CONFIG_REST_URL + "/:id";

    // DF Connects REST endpoint URLs
    public static final String DF_CONNECTS_REST_URL = "/api/df/ps";
    public static final String DF_CONNECTS_REST_URL_WILD = "/api/df/ps*";
    public static final String DF_CONNECTS_REST_URL_WITH_ID = DF_CONNECTS_REST_URL + "/:id";

    // TODO - This is for API POC - Can be removed later
    public static final String DF_CONNECTS_REST_URL2 = "/api/df/ps/new";
    public static final String DF_CONNECTS_REST_URL_WITH_ID2 = DF_CONNECTS_REST_URL2 + "/:id";
    public static final String DF_CONNECTS_REST_START_URL_WITH_ID = "/api/df/ps/new/start" + "/:id";

    // DF Transforms REST endpoint URLs
    public static final String DF_TRANSFORMS_REST_URL = "/api/df/tr";
    public static final String DF_TRANSFORMS_REST_URL_WILD = "/api/df/tr*";
    public static final String DF_TRANSFORMS_REST_URL_WITH_ID = DF_TRANSFORMS_REST_URL + "/:id";
    public static final String DF_TRANSFORMS_UPLOAD_FILE_REST_URL_WILD = "/api/df/uploaded_files*";
    public static final String DF_TRANSFORMS_UPLOAD_FILE_REST_URL = "/api/df/uploaded_files";

    // DF Schema registry endpoint URLs
    public static final String DF_SCHEMA_REST_URL = "/api/df/schema";
    public static final String DF_SCHEMA_REST_URL_WILD = "/api/df/schema*";
    public static final String DF_SCHEMA_REST_URL_WITH_ID = DF_SCHEMA_REST_URL + "/:id";
    public static final String AVRO_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    // DF process history endpoint URLs
    public static final String DF_PROCESS_HIST_REST_URL = "/api/df/hist";
    public static final String DF_PROCESS_HIST_URL_WILD = "/api/df/hist*";

    // DF log endpoint URLs
    public static final String DF_LOGGING_REST_URL = "/api/df/logs";
    public static final String DF_LOGGING_REST_URL_WILD = "/api/df/logs*";
    public static final String DF_LOGGING_REST_URL_WITH_ID = DF_LOGGING_REST_URL + "/:id";

    // DF task status endpoint URLs
    public static final String DF_TASK_STATUS_REST_URL = "/api/df/status";
    public static final String DF_TASK_STATUS_REST_URL_WILD = "/api/df/status*";
    public static final String DF_TASK_STATUS_REST_URL_WITH_ID = DF_TASK_STATUS_REST_URL + "/:id";

    // Kafka Connect endpoint URLs
    public static final String KAFKA_CONNECT_REST_URL = "/connectors";
    public static final String KAFKA_CONNECT_PLUGIN_REST_URL = "/connector-plugins";
    public static String KAFKA_CONNECT_PLUGIN_CONFIG = "/connectors/CONNECTOR_NAME_PLACEHOLDER/config";

    // Kafka Other default settings
    public static String DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK = "df_trans_flink_group_id";

    // Flink rest api setting
    public static final String FLINK_REST_URL = "/jobs";
    public static final String FLINK_DUMMY_JOB_ID = "00000000000000000000000000000000";

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

    // Kafka Confluent Protocol
    public static final byte MAGIC_BYTE = 0x0;
    public static final int idSize = 4;

    public enum DF_STATUS {
        UNASSIGNED,         // The Kafka connector/task has not yet been assigned to a worker.
        RUNNING,            // The Kafka connector/task is running.
        PAUSED,             // The Kafka connector/task has been administratively paused.
        FAILED,             // The Kafka connector/task has failed.
        LOST,               // The Kafka connect restart and lost the connector job in DF repository.
        CANCELED,           // Job (Flink) is canceled
        RWE,                // Connector/Transform is running with one of task is failed - RUNNING_WITH_ERROR
        FINISHED,
        NONE
    }

    /*
     Type to differ from connect or transform. We delivered the ConnectorCategory field from the word before 1st _
     The pattern here is [ConnectorCategory]_[Engine]_[Details]
      */
    public enum DF_CONNECT_TYPE {
        CONNECT_KAFKA_SOURCE,       // Kafka Connector import data into Kafka
        CONNECT_KAFKA_SOURCE_AVRO,  // DF Generic Avro source
        CONNECT_KAFKA_SINK,         // Kafka Connector export data out of Kafka
        CONNECT_EVENTBUS_SOURCE,    // The plugin import data into Vertx Event Bus
        CONNECT_EVENTBUS_SINK,      // The plugin export data out of Vertx Event Bus
        CONNECT_KAFKA_HDFS_SOURCE,  // The plugin import data into HDFS
        CONNECT_KAFKA_HDFS_SINK,    // The plugin export data out of HDFS
        CONNECT_HIVE_SOURCE,        // The plugin import data into Hive
        CONNECT_HIVE_SINK,          // The plugin export data out of Hive
        CONNECT_MONGODB_SOURCE,       // The plugin import data into mongodb
        CONNECT_MONGODB_SINK,         // The plugin export data out of mongodb
        TRANSFORM_FLINK_SQL_GENE,     // Flink streaming SQL
        TRANSFORM_FLINK_SQL_A2A,      // Flink streaming SQL from Avro to Avro
        TRANSFORM_FLINK_SQL_J2J,      // Flink streaming SQL from Json to Json
        TRANSFORM_FLINK_SCRIPT,       // Flink streaming of Table API
        TRANSFORM_FLINK_UDF,          // Flink user defined jar/program
        TRANSFORM_SPARK_SQL,          // Spark streaming SQL
        TRANSFORM_SPARK_BATCH_SQL,    // Spark streaming SQL
        TRANSFORM_SPARK_JOINS,        // Spark streaming of Data Join
        TRANSFORM_SPARK_UDF,          // Spark user defined jar/program
        TRANSFORM_HIVE_TRANS,         // Hive batch SQL
        TRANSFORM_HIVE_JOINS,         // Hive batch join
        INTERNAL_METADATA_COLLECT,    // Reserved metadata sinl
        NONE
    }

    // Schema registry properties
    public static final String SCHEMA = "schema";
    public static final String COMPATIBILITY = "compatibility";
    public static final String SUBJECT = "subject";
    public static final String COMPATIBILITY_LEVEL = "compatibilityLevel";
    public static final int WORKER_POOL_SIZE = 20; // VERT.X Worker pool size
    public static final int MAX_RUNTIME = 6000;  // VERT.X Worker timeout in 6 sec

    // Properties keys for UI
    public static final String PK_TRANSFORM_CUID = "cuid";
    public static final String PK_FLINK_SUBMIT_JOB_ID = "flink.submit.job.id";

    public static final String PK_SCHEMA_ID_INPUT = "schema.ids.in";
    public static final String PK_SCHEMA_ID_OUTPUT = "schema.ids.out";
    public static final String PK_SCHEMA_STR_INPUT = "schema.string.in";
    public static final String PK_SCHEMA_STR_OUTPUT = "schema.string.out";
    public static final String PK_SCHEMA_SUB_INPUT = "schema.subject.in";
    public static final String PK_SCHEMA_SUB_OUTPUT = "schema.subject.out";

    public static final String PK_KAFKA_HOST_PORT = "bootstrap.servers";
    public static final String PK_KAFKA_CONSUMER_GROURP = "group.id";
    public static final String PK_KAFKA_SCHEMA_REGISTRY_HOST_PORT = "schema.registry";
    public static final String PK_KAFKA_CONNECTOR_CLASS = "connector.class";
    public static final String PK_DF_TOPICS_ALIAS = "topics,topic.in";
    public static final String PK_FLINK_TABLE_SINK_KEYS = "sink.key.fields";

    public static final String PK_KAFKA_TOPIC_INPUT = "topic.in";
    public static final String PK_KAFKA_TOPIC_OUTPUT = "topic.out";
    public static final String PK_TRANSFORM_SQL = "trans.sql";
    public static final String PK_TRANSFORM_SCRIPT = "trans.script";
}

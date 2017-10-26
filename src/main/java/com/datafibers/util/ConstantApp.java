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

    // DF subject/topic to tasks mapping endpoint URLs
    public static final String DF_SUBJECT_TO_TASK_REST_URL = "/api/df/s2t";
    public static final String DF_SUBJECT_TO_TASK_REST_URL_WILD = "/api/df/s2t*";
    public static final String DF_SUBJECT_TO_TASK_REST_URL_WITH_ID = DF_SUBJECT_TO_TASK_REST_URL + "/:id";

    // DF subject/topic simple consumer endpoint URLs
    public static final String DF_AVRO_CONSUMER_REST_URL = "/api/df/avroconsumer";
    public static final String DF_AVRO_CONSUMER_REST_URL_WILD = "/api/df/avroconsumer*";
    public static final String DF_AVRO_CONSUMER_REST_URL_WITH_ID = DF_AVRO_CONSUMER_REST_URL + "/:id";

    // DF subject/topic partition information endpoint URLs
    public static final String DF_SUBJECT_TO_PAR_REST_URL = "/api/df/s2p";
    public static final String DF_SUBJECT_TO_PAR_REST_URL_WILD = "/api/df/s2p*";
    public static final String DF_SUBJECT_TO_PAR_REST_URL_WITH_ID = DF_SUBJECT_TO_PAR_REST_URL + "/:id";

    // Kafka Connect endpoint URLs
    public static final String KAFKA_CONNECT_REST_URL = "/connectors";
    public static final String KAFKA_CONNECT_PLUGIN_REST_URL = "/connector-plugins";
    public static String KAFKA_CONNECT_PLUGIN_CONFIG = "/connectors/CONNECTOR_NAME_PLACEHOLDER/config";
    public static final String KAFKA_CONNECT_ACTION_PAUSE = "pause";
    public static final String KAFKA_CONNECT_ACTION_RESUME = "resume";

    // Kafka Other default settings
    public static String DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK = "df_trans_flink_group_id";
    public static String DF_CONNECT_KAFKA_CONSUMER_GROUP_ID = "df_connect_avro_consumer_group_id";
    public static int DF_CONNECT_KAFKA_CONSUMER_POLL_TIMEOUT = 100;
    public static int AVRO_CONSUMER_BATCH_SIE = 10;

    // Flink rest api setting
    public static final String FLINK_REST_URL = "/jobs";
    public static final String FLINK_REST_URL_JARS = "/jars";
    public static final String FLINK_REST_URL_JARS_UPLOAD = FLINK_REST_URL_JARS + "/upload";
    public static final String FLINK_JAR_ID_IN_MONGO = "df_jar_uploaded_to_flink";
    public static final String FLINK_JAR_VALUE_IN_MONGO = "filename";
    public static final String FLINK_JOB_SUBMIT_RESPONSE_KEY = "jobid";
    public static final String FLINK_SQL_CLIENT_CLASS_NAME = "com.datafibers.util.FlinkAvroSQLClient";
    public static final String FLINK_TABLEAPI_CLIENT_CLASS_NAME = "com.datafibers.util.FlinkAvroTableAPIClient";
    public static final String FLINK_DUMMY_JOB_ID = "00000000000000000000000000000000";

    // HTTP req/res constants
    public static final String HTTP_HEADER_CONTENT_TYPE = "content-type";
    public static final String HTTP_HEADER_APPLICATION_JSON_CHARSET = "application/json; charset=utf-8";
    public static final String HTTP_HEADER_TOTAL_COUNT = "X-Total-Count";

    // HTTP status codes
    public static final int STATUS_CODE_OK = 200;
    public static final int STATUS_CODE_OK_CREATED = 201;
    public static final int STATUS_CODE_OK_ACCEPTED = 202;
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
        CONNECT_SOURCE_KAFKA_AvroFile,
        CONNECT_SINK_KAFKA_AvroFile,
        CONNECT_SINK_KAFKA_FlatFile,
        CONNECT_SOURCE_KAFKA_FlatFile,
        CONNECT_SOURCE_KAFKA_JDBC,
        CONNECT_SINK_KAFKA_JDBC,
        CONNECT_SOURCE_MONGODB_AvroDB,
        CONNECT_SINK_MONGODB_AvroDB,
        CONNECT_SOURCE_HDFS_AvroFile,
        CONNECT_SINK_HDFS_AvroFile,
        CONNECT_SOURCE_STOCK_AvroFile,
        TRANSFORM_EXCHANGE_FLINK_SQLA2A,
        TRANSFORM_EXCHANGE_FLINK_Script,
        TRANSFORM_EXCHANGE_FLINK_UDF,
        TRANSFORM_EXCHANGE_SPARK_SQL,          // Spark streaming SQL
        TRANSFORM_EXCHANGE_SPARK_BatchSQL,    // Spark streaming SQL
        TRANSFORM_EXCHANGE_SPARK_JOINS,        // Spark streaming of Data Join
        TRANSFORM_EXCHANGE_SPARK_UDF,          // Spark user defined jar/program
        TRANSFORM_EXCHANGE_HIVE_TRANS,         // Hive batch SQL
        TRANSFORM_EXCHANGE_HIVE_JOINS,         // Hive batch join
        INTERNAL_METADATA_COLLECT,    // Reserved metadata sinl
        NONE
    }

    // Kafka properties

    // Schema registry properties
    public static final String SCHEMA = "schema";
    public static final String COMPATIBILITY = "compatibility";
    public static final String SUBJECT = "subject";
    public static final String COMPATIBILITY_LEVEL = "compatibilityLevel";
    public static final String PARTITIONS = "partitions";
    public static final String REPLICATION_FACTOR = "replicationFactor";
    public static final int WORKER_POOL_SIZE = 20; // VERT.X Worker pool size
    public static final int MAX_RUNTIME = 120000;  // VERT.X Worker timeout in 6 sec
    public static final String SCHEMA_URI_KEY = "schema.registry.url";

    // Properties keys for admin
    public static final String PK_DF_TOPICS_ALIAS = "topics,topic_in";
    public static final String PK_DF_TOPIC_ALIAS = "topic,topic_out";
    public static final String PK_DF_ALL_TOPIC_ALIAS = PK_DF_TOPICS_ALIAS + "," + PK_DF_TOPIC_ALIAS;

    // Properties keys for UI
    public static final String PK_TRANSFORM_CUID = "cuid";
    public static final String PK_FLINK_SUBMIT_JOB_ID = "flink_job_id";

    public static final String PK_SCHEMA_ID_INPUT = "schema_ids_in";
    public static final String PK_SCHEMA_ID_OUTPUT = "schema_ids_out";
    public static final String PK_SCHEMA_STR_INPUT = "schema_string_in";
    public static final String PK_SCHEMA_STR_OUTPUT = "schema_string_out";
    public static final String PK_SCHEMA_SUB_INPUT = "schema_subject_in";
    public static final String PK_SCHEMA_SUB_OUTPUT = "schema_subject_out";

    public static final String PK_KAFKA_HOST_PORT = "bootstrap_servers";
    public static final String PK_KAFKA_CONSUMER_GROURP = "group_id";
    public static final String PK_KAFKA_SCHEMA_REGISTRY_HOST_PORT = "schema_registry";
    public static final String PK_KAFKA_CONNECTOR_CLASS = "connector_class";
    public static final String PK_FLINK_TABLE_SINK_KEYS = "sink_key_fields";

    public static final String PK_KAFKA_TOPIC_INPUT = "topic_in";
    public static final String PK_KAFKA_TOPIC_OUTPUT = "topic_out";
    public static final String PK_TRANSFORM_SQL = "trans_sql";
    public static final String PK_TRANSFORM_SCRIPT = "trans_script";
    public static final String PK_TRANSFORM_JAR_CLASS_NAME = "trans_jar";
    public static final String PK_TRANSFORM_JAR_PARA = "trans_jar_para";
}

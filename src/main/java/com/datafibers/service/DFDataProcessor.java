package com.datafibers.service;

import com.datafibers.model.DFLogPOPJ;
import com.datafibers.model.DFModelPOPJ;
import com.datafibers.processor.*;
import com.datafibers.util.*;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;
import io.vertx.ext.web.handler.TimeoutHandler;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.apache.log4j.Logger;
import com.datafibers.model.DFJobPOPJ;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.log4mongo.MongoDbAppender;

/**
 * DF Producer is used to route producer service to kafka connect rest or lunch locally.
 * The overall status is maintained in the its local database - mongodb
 */

public class DFDataProcessor extends AbstractVerticle {

    // Generic attributes
    public static String COLLECTION;
    public static String COLLECTION_MODEL;
    public static String COLLECTION_INSTALLED;
    public static String COLLECTION_META;
    public static String COLLECTION_LOG;
    private static String repo_conn_str;
    private static String repo_hostname;
    private static String repo_port;
    private static String repo_db;
    private MongoClient mongo;
    private MongoAdminClient mongoDFInstalled;
    private WebClient wc_schema;
    private WebClient wc_connect;
    private WebClient wc_flink;
    private WebClient wc_spark;
    private WebClient wc_refresh;
    private WebClient wc_streamback;
    private static String df_jar_path;
    private static String df_jar_name;
    private static String flink_jar_id;

    private static Integer df_rest_port;

    // Connects attributes
    private static Boolean kafka_connect_enabled;
    private static String kafka_connect_rest_host;
    private static Integer kafka_connect_rest_port;
    private static Boolean kafka_connect_import_start;

    // Transforms attributes flink
    public static Boolean transform_engine_flink_enabled;
    private static String flink_server_host;
    private static Integer flink_rest_server_port;
    private static String flink_rest_server_host_port;

    // Transforms attributes spark
    public static Boolean transform_engine_spark_enabled;
    private static String spark_livy_server_host;
    private static Integer spark_livy_server_port;
    private static String spark_livy_server_host_port;

    // Kafka attributes
    private static String kafka_server_host;
    private static Integer kafka_server_port;
    public static String kafka_server_host_and_port;

    // Schema Registry attributes
    private static String schema_registry_host_and_port;
    private static Integer schema_registry_rest_port;
    private static String schema_registry_rest_hostname;

    // Web HDFS attributes
    private static String webhdfs_host_and_port;
    private static Integer webhdfs_rest_port;
    private static String webhdfs_rest_hostname;


    private static final Logger LOG = Logger.getLogger(DFDataProcessor.class);

    @Override
    public void start(Future<Void> fut) {

//        VertxOptions options = new VertxOptions();
//        options.setBlockedThreadCheckInterval(1000*60*60);

        this.df_jar_path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        this.df_jar_name = new File(df_jar_path).getName();

        /**
         * Get all application configurations
         **/
        // Get generic variables
        this.COLLECTION = config().getString("db.collection.name", "df_processor");
        this.COLLECTION_MODEL = config().getString("db.collection_model.name", "df_model");
        this.COLLECTION_INSTALLED = config().getString("db.collection_installed.name", "df_installed");
        this.COLLECTION_META = config().getString("db.metadata.collection.name", "df_meta");
        this.COLLECTION_LOG = config().getString("db.log.collection.name", "df_log");
        this.repo_db = config().getString("db.name", "DEFAULT_DB");
        this.repo_conn_str = config().getString("repo.connection.string", "mongodb://localhost:27017");
        this.repo_hostname = repo_conn_str.replace("//", "").split(":")[1];
        this.repo_port = repo_conn_str.replace("//", "").split(":")[2];

        this.df_rest_port = config().getInteger("rest.port.df.processor", 8080);

        // Get Connects config
        this.kafka_connect_enabled = config().getBoolean("kafka.connect.enable", Boolean.TRUE);
        this.kafka_connect_rest_host = config().getString("kafka.connect.rest.host", "localhost");
        this.kafka_connect_rest_port = config().getInteger("kafka.connect.rest.port", 8083);
        this.kafka_connect_import_start = config().getBoolean("kafka.connect.import.start", Boolean.TRUE);

        // Check Flink Transforms config
        this.transform_engine_flink_enabled = config().getBoolean("transform.engine.flink.enable", Boolean.TRUE);
        this.flink_server_host = config().getString("flink.server.host", "localhost");
        this.flink_rest_server_port = config().getInteger("flink.rest.server.port", 8001); // Same to Flink Web Dashboard
        this.flink_rest_server_host_port = (this.flink_server_host.contains("http")?
                this.flink_server_host : "http://" + this.flink_server_host) + ":" + this.flink_rest_server_port;

        // Check Spark Transforms config
        this.transform_engine_spark_enabled = config().getBoolean("transform.engine.spark.enable", Boolean.TRUE);
        this.spark_livy_server_host = config().getString("spark.livy.server.host", "localhost");
        this.spark_livy_server_port = config().getInteger("spark.livy.server.port", 8998);
        this.spark_livy_server_host_port = (this.flink_server_host.contains("http")?
                this.spark_livy_server_host : "http://" + this.spark_livy_server_host) + ":" +
                this.spark_livy_server_port;

        // Kafka config
        this.kafka_server_host = this.kafka_connect_rest_host;
        this.kafka_server_port = config().getInteger("kafka.server.port", 9092);
        this.kafka_server_host_and_port = this.kafka_server_host + ":" + this.kafka_server_port.toString();

        // Schema Registry
        this.schema_registry_rest_hostname = kafka_connect_rest_host;
        this.schema_registry_rest_port = config().getInteger("kafka.schema.registry.rest.port", 8081);
        this.schema_registry_host_and_port = this.schema_registry_rest_hostname + ":" + this.schema_registry_rest_port;

        // WebHDFS
        this.webhdfs_rest_port = config().getInteger("webhdfs.server.port", 50070);
        this.webhdfs_rest_hostname = config().getString("webhdfs.server.host", "localhost");
        this.webhdfs_host_and_port = this.webhdfs_rest_hostname + ":" + this.webhdfs_rest_port;


        // Application init in separate thread and report complete once done
        vertx.executeBlocking(future -> {

            /**
             * Create all application client
             **/
            // MongoDB client for metadata repository
            JsonObject mongoConfig = new JsonObject().put("connection_string", repo_conn_str).put("db_name", repo_db);
            mongo = MongoClient.createShared(vertx, mongoConfig);

            // df_meta mongo client to find default connector.class
            mongoDFInstalled = new MongoAdminClient(repo_hostname, repo_port, repo_db, COLLECTION_INSTALLED);

            // Cleanup Log in mongodb
            if (config().getBoolean("db.log.cleanup.on.start", Boolean.TRUE))
                new MongoAdminClient(repo_hostname, repo_port, repo_db).truncateCollection(COLLECTION_LOG).close();

            // Set dynamic logging to MongoDB
            MongoDbAppender mongoAppender = new MongoDbAppender();
            mongoAppender.setDatabaseName(repo_db);
            mongoAppender.setCollectionName(COLLECTION_LOG);
            mongoAppender.setHostname(repo_hostname);
            mongoAppender.setPort(repo_port);
            mongoAppender.activateOptions();
            Logger.getRootLogger().addAppender(mongoAppender);

            LOG.info(DFAPIMessage.logResponseMessage(1029, "Mongo Client & Log Shipping Setup Complete"));

            // Non-blocking Rest API Client to talk to Kafka Connect when needed
            if (this.kafka_connect_enabled) {
                this.wc_connect = WebClient.create(vertx);
                this.wc_schema = WebClient.create(vertx);
            }

            // Import from remote server. It is blocking at this point.
            if (this.kafka_connect_enabled && this.kafka_connect_import_start) {
                importAllFromKafkaConnect();
                // importAllFromFlinkTransform();
                startMetadataSink();
            }

            // Non-blocking Rest API Client to talk to Flink Rest when needed
            if (this.transform_engine_spark_enabled) {
                this.wc_spark = WebClient.create(vertx);
            }

            // Non-blocking Rest API Client to talk to Flink Rest when needed
            if (this.transform_engine_flink_enabled) {
                this.wc_flink = WebClient.create(vertx);
                // Delete all df jars already uploaded
                wc_flink.get(flink_rest_server_port, flink_server_host, ConstantApp.FLINK_REST_URL_JARS)
                        .send(ar -> {
                            if (ar.succeeded()) {
                                // Obtain response
                                JsonArray jarArray = ar.result().bodyAsJsonObject().getJsonArray("files");
                                for (int i = 0; i < jarArray.size(); i++) {
                                    if(jarArray.getJsonObject(i).getString("name").equalsIgnoreCase(df_jar_name)) {
                                        // delete it
                                        wc_flink.delete(flink_rest_server_port, flink_server_host,
                                                ConstantApp.FLINK_REST_URL_JARS + "/" +
                                                        jarArray.getJsonObject(i).getString("id"))
                                                .send(dar -> {});
                                    }
                                }

                                // The outer layer executeBlocking does not work when inside a web client api
                                // TODO Add another executeBlocking for now until Web Client does not support muti file
                                vertx.executeBlocking(jarfuture -> {
                                    // Web Client does not support muti file yet, blocking inside
                                    this.flink_jar_id = HelpFunc.uploadJar(
                                            flink_rest_server_host_port + ConstantApp.FLINK_REST_URL_JARS_UPLOAD,
                                            this.df_jar_path
                                    );
                                    if (flink_jar_id.isEmpty()) {
                                        LOG.error(DFAPIMessage.logResponseMessage(9035, flink_jar_id));
                                    } else {
                                        LOG.info(DFAPIMessage.logResponseMessage(1028, flink_jar_id));
                                        LOG.info("********* DataFibers Services is started :) *********");
                                    }
                                }, res -> {});

                            } else {
                                LOG.error(DFAPIMessage.logResponseMessage(9035, flink_jar_id));
                            }
                        });
            }

            if (!this.transform_engine_flink_enabled)
                LOG.info("********* DataFibers Services is started :) *********");

        }, res -> {});

        // Regular update Kafka connects/Flink transform status through unblocking api
        this.wc_refresh = WebClient.create(vertx);
        this.wc_streamback = WebClient.create(vertx);

        vertx.setPeriodic(ConstantApp.REGULAR_REFRESH_STATUS_TO_REPO, id -> {
            if(this.kafka_connect_enabled) updateKafkaConnectorStatus();
            if(this.transform_engine_flink_enabled) updateFlinkJobStatus();
            if(this.transform_engine_spark_enabled) updateSparkJobStatus();
        });

        // Start Core application
        startWebApp((http) -> completeStartup(http, fut));
    }

    private void startWebApp(Handler<AsyncResult<HttpServer>> next) {

        // Create a router object for rest.
        Router router = Router.router(vertx);

        // Job including both Connects and Transforms Rest API definition
        router.options(ConstantApp.DF_PROCESSOR_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_PROCESSOR_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_PROCESSOR_REST_URL).handler(this::getAllProcessor);
        router.get(ConstantApp.DF_PROCESSOR_REST_URL_WITH_ID).handler(this::getOne);
        router.route(ConstantApp.DF_PROCESSOR_REST_URL_WILD).handler(BodyHandler.create());

        // Connects Rest API definition
        router.options(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_CONNECTS_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_CONNECTS_REST_URL).handler(this::getAllConnects);
        router.get(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::getOne);
        router.route(ConstantApp.DF_CONNECTS_REST_URL_WILD).handler(BodyHandler.create());

        router.post(ConstantApp.DF_CONNECTS_REST_URL).handler(this::addOneConnects); // Kafka Connect Forward
        router.put(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::putOneConnect); // Kafka Connect Forward
        router.delete(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::deleteOneConnects); // Kafka Connect Forward

        // Transforms Rest API definition
        router.options(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::corsHandle);
        router.options(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::getAllTransforms);
        router.get(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::getOne);
        router.route(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL_WILD).handler(BodyHandler.create());
        router.post(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL).handler(this::uploadFiles);
        router.route(ConstantApp.DF_TRANSFORMS_REST_URL_WILD).handler(BodyHandler.create());

        router.post(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::addOneTransforms); // Flink Forward
        router.put(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::updateOneTransforms); // Flink Forward
        router.delete(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::deleteOneTransforms); // Flink Forward

        // Model Rest API definition
        router.options(ConstantApp.DF_MODEL_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_MODEL_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_MODEL_REST_URL).handler(this::getAllModels);
        router.get(ConstantApp.DF_MODEL_REST_URL_WITH_ID).handler(this::getOneModel);
        router.route(ConstantApp.DF_MODEL_REST_URL_WILD).handler(BodyHandler.create());

        router.post(ConstantApp.DF_MODEL_REST_URL).handler(this::addOneModel);
        router.put(ConstantApp.DF_MODEL_REST_URL_WITH_ID).handler(this::updateOneModel);
        router.delete(ConstantApp.DF_MODEL_REST_URL_WITH_ID).handler(this::deleteOneModel);

        // Schema Registry
        router.options(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_SCHEMA_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_SCHEMA_REST_URL).handler(this::getAllSchemas); // Schema Registry Forward
        router.get(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::getOneSchema); // Schema Registry Forward
        router.route(ConstantApp.DF_SCHEMA_REST_URL_WILD).handler(BodyHandler.create()); // Schema Registry Forward
        
        router.post(ConstantApp.DF_SCHEMA_REST_URL).handler(this::addOneSchema); // Schema Registry Forward
        router.put(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::updateOneSchema); // Schema Registry Forward
        router.delete(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::deleteOneSchema); // Schema Registry Forward

        // Logging Rest API definition
        router.options(ConstantApp.DF_LOGGING_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_LOGGING_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_LOGGING_REST_URL).handler(this::getAllLogs);
        router.get(ConstantApp.DF_LOGGING_REST_URL_WITH_ID).handler(this::getOneLogs);
        router.route(ConstantApp.DF_LOGGING_REST_URL_WILD).handler(BodyHandler.create());

        // Status Rest API definition
        router.options(ConstantApp.DF_TASK_STATUS_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_TASK_STATUS_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_TASK_STATUS_REST_URL_WITH_ID).handler(this::getOneStatus);
        router.get(ConstantApp.DF_TASK_STATUS_REST_URL_WILD).handler(this::getOneStatus);

        // Subject to Task Rest API definition
        router.options(ConstantApp.DF_SUBJECT_TO_TASK_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_SUBJECT_TO_TASK_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_SUBJECT_TO_TASK_REST_URL_WITH_ID).handler(this::getAllTasksOneTopic);
        router.get(ConstantApp.DF_SUBJECT_TO_TASK_REST_URL_WILD).handler(this::getAllTasksOneTopic);

        // Avro Consumer Rest API definition and set timeout to 10s
        router.route(ConstantApp.DF_AVRO_CONSUMER_REST_URL).handler(TimeoutHandler.create(10000));
        router.options(ConstantApp.DF_AVRO_CONSUMER_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_AVRO_CONSUMER_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_AVRO_CONSUMER_REST_URL_WITH_ID).handler(this::pollAllFromTopic);
        router.get(ConstantApp.DF_AVRO_CONSUMER_REST_URL_WILD).handler(this::pollAllFromTopic);

        // Subject to partition info. Rest API definition
        router.options(ConstantApp.DF_SUBJECT_TO_PAR_REST_URL).handler(this::corsHandle);
        router.options(ConstantApp.DF_SUBJECT_TO_PAR_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_SUBJECT_TO_PAR_REST_URL_WITH_ID).handler(this::getAllTopicPartitions);
        router.get(ConstantApp.DF_SUBJECT_TO_PAR_REST_URL_WILD).handler(this::getAllTopicPartitions);

        // Get all installed connect or transform
        router.options(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL).handler(this::getAllProcessorConfigs);
        router.get(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL_WITH_ID).handler(this::getOneProcessorConfig);
        router.route(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL_WILD).handler(BodyHandler.create());

        // Process History
        router.options(ConstantApp.DF_PROCESS_HIST_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_PROCESS_HIST_REST_URL).handler(this::getAllProcessHistory);
        
        // Create the HTTP server and pass the "accept" method to the request handler.
        vertx.createHttpServer().requestHandler(router::accept)
                                .listen(config().getInteger("rest.port.df.processor", 8080), next::handle);
    }

    private void completeStartup(AsyncResult<HttpServer> http, Future<Void> fut) {
        if (http.succeeded()) {
            fut.complete();
        } else {
            fut.fail(http.cause());
        }
    }

    @Override
    public void stop() throws Exception {
        this.mongo.close();
        this.mongoDFInstalled.close();
        this.wc_connect.close();
        this.wc_schema.close();
        this.wc_flink.close();
    }

    /**
     * This is mainly to bypass security control for local API testing.
     * @param routingContext
     */
    public void corsHandle(RoutingContext routingContext) {
        routingContext.response()
                .putHeader("Access-Control-Allow-Origin", "*")
                .putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
                .putHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, X-Total-Count")
                .putHeader("Access-Control-Expose-Headers", "X-Total-Count")
                .putHeader("Access-Control-Max-Age", "60").end();
    }

    /**
     * Handle upload UDF Jar form for Flink UDF Transformation
     * @param routingContext
     *
     * @api {post} /uploaded_files 7.Upload file
     * @apiVersion 0.1.1
     * @apiName uploadFiles
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is triggered through "ADD File" in the create transform view of Web Admin Console.
     * @apiParam	{binary}	None        Binary String of file content.
     * @apiSuccess {JsonObject[]} uploaded_file_name     The name of the file uploaded.
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {
     *       "code" : "200",
     *       "uploaded_file_name": "/home/vagrant/flink_word_count.jar",
     *     }
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {
     *       "code" : "400",
     *       "uploaded_file_name" : "failed"
     *     }
     *
     */
    private void uploadFiles (RoutingContext routingContext) {
        Set<FileUpload> fileUploadSet = routingContext.fileUploads();

        Iterator<FileUpload> fileUploadIterator = fileUploadSet.iterator();
        String fileName;
        
        while (fileUploadIterator.hasNext()) {
            FileUpload fileUpload = fileUploadIterator.next();
            
			try {
				fileName = URLDecoder.decode(fileUpload.fileName(), "UTF-8");
            
				String jarPath = new HelpFunc().getCurrentJarRunnigFolder();
				String currentDir = config().getString("upload.dest", jarPath);
				String fileToBeCopied = currentDir + HelpFunc.generateUniqueFileName(fileName);
	            LOG.debug("===== fileToBeCopied: " + fileToBeCopied);
	            
	            vertx.fileSystem().copy(fileUpload.uploadedFileName(), fileToBeCopied, res -> {
	                if (res.succeeded()) {
	                    LOG.info("FILE COPIED GOOD ==> " + fileToBeCopied);
	                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                .end(DFAPIMessage.getCustomizedResponseMessage("uploaded_file_name", fileToBeCopied));
	                } else {
	                    // Something went wrong
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                .end( DFAPIMessage.getCustomizedResponseMessage("uploaded_file_name", "Failed"));
	                }
	            });
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
        }
    }

    /**
     * Generic getAll method for REST API End Point
     * @param routingContext
     */
    private void getAll(RoutingContext routingContext, String connectorCategoryFilter) {
        JsonObject searchCondition;
        if (connectorCategoryFilter.equalsIgnoreCase("all")) {
            searchCondition = new JsonObject();
        } else {

            String searchKeywords = routingContext.request().getParam("q");
            searchCondition = new JsonObject().put("$and",
                    new JsonArray()
                            .add(new JsonObject().put("$where", "JSON.stringify(this).indexOf('" + searchKeywords + "') != -1"))
                            .add(new JsonObject().put("connectorCategory", connectorCategoryFilter))
            );
        }

        mongo.findWithOptions(COLLECTION, searchCondition, HelpFunc.getMongoSortFindOption(routingContext),
                results -> {
                    List<JsonObject> objects = results.result();
                    List<DFJobPOPJ> jobs = objects.stream().map(DFJobPOPJ::new).collect(Collectors.toList());
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .putHeader("X-Total-Count", jobs.size() + "" )
                            .end(Json.encodePrettily(jobs));
        });
    }

    /**
     * This is for fetch both connects and transforms
     * @param routingContext
     *
     * @api {get} /processor 1.List all tasks
     * @apiVersion 0.1.1
     * @apiName getAll
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get list of all connects and transforms.
     * @apiSuccess	{JsonObject[]}	connects    List of connect and transform task profiles.
     * @apiSampleRequest http://localhost:8080/api/df/processor
     */
    private void getAllProcessor(RoutingContext routingContext) {
        getAll(routingContext, "ALL");
    }

    /**
     * Get all DF connects
     *
     * @param routingContext
     *
     * @api {get} /ps 1.List all connects task
     * @apiVersion 0.1.1
     * @apiName getAllConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is where we get data for all active connects.
     * @apiSuccess	{JsonObject[]}	connects    List of connect task profiles.
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     [ {
     *          "id" : "58471d13bba4a429f8a272b6",
     *          "taskSeq" : "1",
     *          "name" : "tesavro",
     *          "connectUid" : "58471d13bba4a429f8a272b6",
     *          "jobUid" : "reserved for job level tracking",
     *          "connectorType" : "CONNECT_KAFKA_SOURCE_AVRO",
     *          "connectorCategory" : "CONNECT",
     *          "description" : "task description",
     *          "status" : "LOST",
     *          "udfUpload" : null,
     *          "jobConfig" : null,
     *          "connectorConfig" : {
     *              "connector.class" : "com.datafibers.kafka.connect.FileGenericSourceConnector",
     *              "schema.registry.uri" : "http://localhost:8081",
     *              "cuid" : "58471d13bba4a429f8a272b6",
     *              "file.location" : "/home/vagrant/df_data/",
     *              "tasks.max" : "1",
     *              "file.glob" : "*.{json,csv}",
     *              "file.overwrite" : "true",
     *              "schema.subject" : "test-value",
     *              "topic" : "testavro"
     *          }
     *       }
     *     ]
     * @apiSampleRequest http://localhost:8080/api/df/ps
     */
    private void getAllConnects(RoutingContext routingContext) {
        getAll(routingContext,"CONNECT");
    }

    /**
     * Get all DF transforms
     *
     * @param routingContext
     *
     * @api {get} /tr 1.List all transforms task
     * @apiVersion 0.1.1
     * @apiName getAllConnects
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is where get data for all active transforms.
     * @apiSuccess	{JsonObject[]}	connects    List of transform task profiles.
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     [ {
     *          "id" : "58471d13bba4a429f8a272b6",
     *          "taskSeq" : "1",
     *          "name" : "tesavro",
     *          "connectUid" : "58471d13bba4a429f8a272b6",
     *          "jobUid" : "reserved for job level tracking",
     *          "connectorType" : "TRANSFORM_FLINK_SQL_A2J",
     *          "connectorCategory" : "TRANSFORM",
     *          "description" : "task description",
     *          "status" : "LOST",
     *          "udfUpload" : null,
     *          "jobConfig" : null,
     *          "connectorConfig" : {
     *              "cuid" : "58471d13bba4a429f8a272b0",
     *              "trans.sql":"SELECT STREAM symbol, name FROM finance"
     *              "group.id":"consumer3",
     *              "topic.for.query":"finance",
     *              "topic.for.result":"stock",
     *              "schema.subject" : "test-value"
     *          }
     *       }
     *     ]
     * @apiSampleRequest http://localhost:8080/api/df/tr
     */
    private void getAllTransforms(RoutingContext routingContext) {
        getAll(routingContext,"TRANSFORM");
    }

    /**
     * Get all ML models for REST API End Point
     * @param routingContext
     */
    private void getAllModels(RoutingContext routingContext) {
        mongo.findWithOptions(COLLECTION_MODEL, new JsonObject(), HelpFunc.getMongoSortFindOption(routingContext),
                results -> {
                    List<JsonObject> objects = results.result();
                    List<DFModelPOPJ> jobs = objects.stream().map(DFModelPOPJ::new).collect(Collectors.toList());
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .putHeader("X-Total-Count", jobs.size() + "" )
                            .end(Json.encodePrettily(jobs));
                });
    }

    /**
     * Get all schema from schema registry
     * @param routingContext
     *
     * @api {get} /schema 1.List all schema
     * @apiVersion 0.1.1
     * @apiName getAllSchemas
     * @apiGroup Schema
     * @apiPermission none
     * @apiDescription This is where we get list of available schema data from schema registry.
     * @apiSuccess	{JsonObject[]}	connects    List of schemas added in schema registry.
     * @apiSampleRequest http://localhost:8080/api/df/schema
     */
    public void getAllSchemas(RoutingContext routingContext) {
        ProcessorTopicSchemaRegistry.forwardGetAllSchemas(vertx, routingContext, schema_registry_host_and_port);
    }

    /**
     * List all configurations for Connect or Transforms
     * @param routingContext
     *
     * @api {get} /config 4.List processor lib
     * @apiVersion 0.1.1
     * @apiName getAllProcessorConfigs
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where get list of configured or installed connect/transform jar or libraries.
     * @apiSuccess	{JsonObject[]}	config    List of processors' configuration.
     * @apiSampleRequest http://localhost:8080/api/df/config
     */
    private void getAllProcessorConfigs(RoutingContext routingContext) {
        ProcessorConnectKafka.forwardGetAsGetConfig(routingContext, wc_connect, mongo, COLLECTION_INSTALLED,
                kafka_connect_rest_host, kafka_connect_rest_port);
    }

    /**
     * Get all connector process history from df_meta topic sinked into Mongo
     * @param routingContext
     *
     * @api {get} /hist 2.List all processed history
     * @apiVersion 0.1.1
     * @apiName getAllProcessHistory
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where get history of processed files in tasks or jobs.
     * @apiSuccess	{JsonObject[]}	history    List of processed history.
     * @apiSampleRequest http://localhost:8080/api/df/hist
     */
    private void getAllProcessHistory(RoutingContext routingContext) {

        String sortName = HelpFunc.coalesce(routingContext.request().getParam("_sortField"), "name");
        int sortOrder = HelpFunc.strCompare(
                HelpFunc.coalesce(routingContext.request().getParam("_sortDir"), "ASC"), "ASC", 1, -1);

        JsonObject command = new JsonObject()
                .put("aggregate", config().getString("db.metadata.collection.name", this.COLLECTION_META))
                .put("pipeline", new JsonArray().add(
                        new JsonObject().put("$group",
                                new JsonObject()
                                        .put("_id", new JsonObject().put("cuid", "$cuid").put("file_name", "$file_name"))
                                        .put("cuid", new JsonObject().put("$first", "$cuid"))
                                        .put("file_name", new JsonObject().put("$first", "$file_name"))
                                        .put("schema_version", new JsonObject().put("$first", "$schema_version"))
                                        .put("last_modified_timestamp", new JsonObject().put("$first", "$last_modified_timestamp"))
                                        .put("file_size", new JsonObject().put("$first", "$file_size"))
                                        .put("topic_sent", new JsonObject().put("$first", "$topic_sent"))
                                        .put("schema_subject", new JsonObject().put("$first", "$schema_subject"))
                                        .put("start_time", new JsonObject().put("$min", "$current_timemillis"))
                                        .put("end_time", new JsonObject().put("$max", "$current_timemillis"))
                                        .put("status", new JsonObject().put("$min", "$status"))
                        )).add(
                        new JsonObject().put("$project", new JsonObject()
                                .put("_id", 1)
                                .put("id", new JsonObject().put("$concat",
                                        new JsonArray().add("$cuid").add("$file_name")))
                                .put("cuid", 1)
                                .put("file_name", 1)
                                .put("schema_version", 1)
                                .put("schema_subject", 1)
                                .put("last_modified_timestamp", 1)
                                .put("file_owner", 1)
                                .put("file_size", 1)
                                .put("topic_sent", 1)
                                .put("status", 1)
                                .put("process_milliseconds",
                                        new JsonObject().put("$subtract",
                                                new JsonArray().add("$end_time").add("$start_time")))

                        )
                        ).add( new JsonObject().put("$sort", new JsonObject().put(sortName, sortOrder)))
                );

        mongo.runCommand("aggregate", command, res -> {
            if (res.succeeded()) {
                JsonArray resArr = res.result().getJsonArray("result");
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_OK)
                        .putHeader(ConstantApp.HTTP_HEADER_TOTAL_COUNT, resArr.size() + "")
                        .end(Json.encodePrettily(resArr));
            } else {
                res.cause().printStackTrace();
            }

        });
    }

    /**
     * Get all logging information from df repo
     * @param routingContext
     *
     * @api {get} /logs 3.List all df logs in the current run
     * @apiVersion 0.1.1
     * @apiName getAllLogs
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where get df logs.
     * @apiSuccess	{JsonObject[]}	logs    List of processed logs.
     * @apiSampleRequest http://localhost:8080/api/df/logs
     */
    private void getAllLogs(RoutingContext routingContext) {

        JsonObject searchCondition;

        String searchKeywords = routingContext.request().getParam("q");
        if(searchKeywords == null || searchKeywords.isEmpty()) {
            searchCondition = new JsonObject().put("level", new JsonObject().put("$ne", "DEBUG"));
        } else {
            searchCondition = new JsonObject().put("$and",
                    new JsonArray()
                            .add(new JsonObject().put("$where", "JSON.stringify(this).indexOf('" + searchKeywords + "') != -1"))
                            .add(new JsonObject().put("level", new JsonObject().put("$ne", "DEBUG")))
            );
        }


        mongo.findWithOptions(COLLECTION_LOG, searchCondition, HelpFunc.getMongoSortFindOption(routingContext),
                results -> {
                    List<JsonObject> objects = results.result();
                    List<DFLogPOPJ> jobs = objects.stream().map(DFLogPOPJ::new).collect(Collectors.toList());
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .putHeader("X-Total-Count", jobs.size() + "" )
                            .end(Json.encodePrettily(jobs));
                });
    }
    /**
     * Get all tasks information using specific topic
     * @param routingContext
     *
     * @api {get} /s2t 5.List all df tasks using specific topic
     * @apiVersion 0.1.1
     * @apiName getAllTasksOneTopic
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where get list of tasks using specific topic.
     * @apiSuccess	{JsonObject[]}	topic    List of tasks related to the topic.
     * @apiSampleRequest http://localhost:8080/api/df/s2t
     */
    private void getAllTasksOneTopic(RoutingContext routingContext) {
        final String topic = routingContext.request().getParam("id");
        JsonObject searchCondition =
                HelpFunc.getContainsTopics("connectorConfig", ConstantApp.PK_DF_ALL_TOPIC_ALIAS, topic);
        if (topic == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "TOPIC_IS_NULL"));
        } else {
            mongo.find(COLLECTION, searchCondition,
                    results -> {
                        List<JsonObject> objects = results.result();
                        List<DFJobPOPJ> jobs = objects.stream().map(DFJobPOPJ::new).collect(Collectors.toList());
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .putHeader("X-Total-Count", jobs.size() + "" )
                                .end(Json.encodePrettily(jobs));
                    });
        }
    }

    /**
     * Describe topic with topic specified
     *
     * @api {get} /s2p/:taskId   6. Get partition information for the specific subject/topic
     * @apiVersion 0.1.1
     * @apiName getAllTopicPartitions
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get partition information for the subject/topic.
     * @apiParam {String}   topic      topic name.
     * @apiSuccess	{JsonObject[]}	info.    partition info.
     * @apiSampleRequest http://localhost:8080/api/df/s2p/:taskId
     */
    private void getAllTopicPartitions(RoutingContext routingContext) {
        final String topic = routingContext.request().getParam("id");
        if (topic == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, topic));
        } else {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_server_host_and_port);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, ConstantApp.DF_CONNECT_KAFKA_CONSUMER_GROUP_ID);
            props.put(ConstantApp.SCHEMA_URI_KEY, "http://" + schema_registry_host_and_port);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

            KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
            ArrayList<JsonObject> responseList = new ArrayList<JsonObject>();

            // Subscribe to a single topic
            consumer.partitionsFor(topic, ar -> {
                if (ar.succeeded()) {
                    for (PartitionInfo partitionInfo : ar.result()) {
                        responseList.add(new JsonObject()
                                .put("id", partitionInfo.getTopic())
                                .put("partitionNumber", partitionInfo.getPartition())
                                .put("leader", partitionInfo.getLeader().getIdString())
                                .put("replicas", StringUtils.join(partitionInfo.getReplicas(), ','))
                                .put("insyncReplicas", StringUtils.join(partitionInfo.getInSyncReplicas(), ','))
                        );

                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .putHeader("X-Total-Count", responseList.size() + "")
                                .end(Json.encodePrettily(responseList));
                        consumer.close();
                    }
                } else {
                    LOG.error(DFAPIMessage.logResponseMessage(9030, topic + "-" +
                            ar.cause().getMessage()));
                }
            });

            consumer.exceptionHandler(e -> {
                LOG.error(DFAPIMessage.logResponseMessage(9031, topic + "-" + e.getMessage()));
            });
        }
    }

    /**
     * Poll all available information from specific topic
     * @param routingContext
     *
     * @api {get} /avroconsumer 7.List all df tasks using specific topic
     * @apiVersion 0.1.1
     * @apiName poolAllFromTopic
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where consume data from specific topic in one pool.
     * @apiSuccess	{JsonObject[]}	topic    Consumer from the topic.
     * @apiSampleRequest http://localhost:8080/api/df/avroconsumer
     */
    private void pollAllFromTopic(RoutingContext routingContext) {

        final String topic = routingContext.request().getParam("id");
        if (topic == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "TOPIC_IS_NULL"));
        } else {
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_server_host_and_port);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, ConstantApp.DF_CONNECT_KAFKA_CONSUMER_GROUP_ID);
                props.put(ConstantApp.SCHEMA_URI_KEY, "http://" + schema_registry_host_and_port);
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
                props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

                KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
                ArrayList<JsonObject> responseList = new ArrayList<JsonObject>();

                consumer.handler(record -> {
                    //LOG.debug("Processing value=" + record.record().value() + ",offset=" + record.record().offset());
                    responseList.add(new JsonObject()
                            .put("id", record.record().offset())
                            .put("value", new JsonObject(record.record().value().toString()))
                            .put("valueString", Json.encodePrettily(new JsonObject(record.record().value().toString())))
                    );
                    if(responseList.size() >= ConstantApp.AVRO_CONSUMER_BATCH_SIE ) {
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .putHeader("X-Total-Count", responseList.size() + "")
                                .end(Json.encodePrettily(responseList));
                        consumer.pause();
                        consumer.commit();
                        consumer.close();
                    }
                });
                consumer.exceptionHandler(e -> {
                    LOG.error(DFAPIMessage.logResponseMessage(9031, topic + "-" + e.getMessage()));
                });

                // Subscribe to a single topic
                consumer.subscribe(topic, ar -> {
                    if (ar.succeeded()) {
                        LOG.info(DFAPIMessage.logResponseMessage(1027, "topic = " + topic));
                    } else {
                        LOG.error(DFAPIMessage.logResponseMessage(9030, topic + "-" + ar.cause().getMessage()));
                    }
                });
        }
    }

    /**
     * Generic getOne method for REST API End Point.
     * @param routingContext
     *
     * @api {get} /ps/:id    3. Get a connect task
     * @apiVersion 0.1.1
     * @apiName getOne
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is where we get data for one task with specified id.
     * @apiParam {String}   id      task Id (_id in mongodb).
     * @apiSuccess	{JsonObject[]}	connects    One connect task profiles.
     * @apiSampleRequest http://localhost:8080/api/df/ps/:id
     */
    /**
     * @api {get} /tr/:id    3. Get a transform task
     * @apiVersion 0.1.1
     * @apiName getOne
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is where we get data for one task with specified id.
     * @apiParam {String}   id      task Id (_id in mongodb).
     * @apiSuccess	{JsonObject[]}	transforms    One transform task profiles.
     * @apiSampleRequest http://localhost:8080/api/df/tr/:id
     */
    private void getOne(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "id=null"));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response()
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9001));
                        LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .end(Json.encodePrettily(dfJob));
                    LOG.info(DFAPIMessage.logResponseMessage(1003, id));

                } else {
                    routingContext.response()
                            .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                            .end(DFAPIMessage.getResponseMessage(9002));
                    LOG.error(DFAPIMessage.logResponseMessage(9002, id));
                }
            });
        }
    }

    /**
     * Get one model from repository
     */
    private void getOneModel(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "id=null"));
        } else {
            mongo.findOne(COLLECTION_MODEL, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response()
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9001));
                        LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                        return;
                    } else {
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                .end(Json.encodePrettily(ar.result()));
                        LOG.info(DFAPIMessage.logResponseMessage(1003, id));
                    }
                } else {
                    routingContext.response()
                            .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                            .end(DFAPIMessage.getResponseMessage(9002));
                    LOG.error(DFAPIMessage.logResponseMessage(9002, id));
                }
            });
        }
    }

    /**
     * @api {get} /config/:id    5. Get a config info.
     * @apiVersion 0.1.1
     * @apiName getOneProcessorConfig
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get config for one specific processor type.
     * @apiParam {String}   id      config Id (aka. connectorType).
     * @apiSuccess	{JsonObject[]}	all    One processor config.
     * @apiSampleRequest http://localhost:8080/api/df/config/:id
     */
    private void getOneProcessorConfig(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "id=null"));
        } else {
            mongo.findOne(COLLECTION_INSTALLED, new JsonObject().put("connectorType", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response()
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9001));
                        LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                        return;
                    }
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .end(Json.encodePrettily(ar.result()));
                    LOG.info(DFAPIMessage.logResponseMessage(1003, id));

                } else {
                    routingContext.response()
                            .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                            .end(DFAPIMessage.getResponseMessage(9002));
                    LOG.error(DFAPIMessage.logResponseMessage(9002, id));
                }
            });
        }
    }

    /**
     * Get one schema with schema subject specified
     * 1) Retrieve a specific subject latest information:
     * curl -X GET -i http://localhost:8081/subjects/Kafka-value/versions/latest
     *
     * 2) Retrieve a specific subject compatibility:
     * curl -X GET -i http://localhost:8081/config/finance-value
     *
     * @api {get} /schema/:subject   2. Get a schema
     * @apiVersion 0.1.1
     * @apiName getOneSchema
     * @apiGroup Schema
     * @apiPermission none
     * @apiDescription This is where we get schema with specified schema subject.
     * @apiParam {String}   subject      schema subject name in schema registry.
     * @apiSuccess	{JsonObject[]}	schema    One schema object.
     * @apiSampleRequest http://localhost:8080/api/df/schema/:subject
     */
    private void getOneSchema(RoutingContext routingContext) {
        ProcessorTopicSchemaRegistry.forwardGetOneSchema(routingContext, wc_schema,
                kafka_server_host, schema_registry_rest_port);
    }

    /**
     * Get one task logs with task id specified from repo
     *
     * @api {get} /logs/:taskId   2. Get a task status
     * @apiVersion 0.1.1
     * @apiName getOneLogs
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get task logs with specified task id.
     * @apiParam {String}   taskId      taskId for connect or transform.
     * @apiSuccess	{JsonObject[]}	logs    logs of the task.
     * @apiSampleRequest http://localhost:8080/api/df/logs/:taskId
     *
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     [ {
     *          "_id" : ObjectId("59827ca37985374c290c0bfd"),
     *          "timestamp" : ISODate("2017-08-03T01:30:11.720Z"),
     *          "level" : "INFO",
     *          "thread" : "vert.x-eventloop-thread-0",
     *          "message" : "{\"code\":\"1015\",\"message\":\"INFO - IMPORT_ACTIVE_CONNECTS_STARTED_AT_STARTUP\",\"comments\":\"CONNECT_IMPORT\"}",
     *          "loggerName" : {
     *          "fullyQualifiedClassName" : "com.datafibers.service.DFDataProcessor",
     *          "package" : [
     *          "com",
     *          "datafibers",
     *          "service",
     *          "DFDataProcessor"
     *          ],
     *          "className" : "DFDataProcessor"
     *          },
     *          "fileName" : "DFDataProcessor.java",
     *          "method" : "importAllFromKafkaConnect",
     *          "lineNumber" : "1279",
     *          "class" : {
     *          "fullyQualifiedClassName" : "com.datafibers.service.DFDataProcessor",
     *          "package" : [
     *          "com",
     *          "datafibers",
     *          "service",
     *          "DFDataProcessor"
     *          ],
     *          "className" : "DFDataProcessor"
     *          },
     *          "host" : {
     *          "process" : "19497@vagrant",
     *          "name" : "vagrant",
     *          "ip" : "127.0.1.1"
     *          }
     *          }
     *     ]
     * @apiSampleRequest http://localhost:8080/api/logs/:id
     */
    private void getOneLogs(RoutingContext routingContext) {

        final String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, id));
        } else {
            JsonObject searchCondition = new JsonObject().put("message", new JsonObject().put("$regex", id));
            mongo.findWithOptions(COLLECTION_LOG, searchCondition, HelpFunc.getMongoSortFindOption(routingContext),
                    results -> {
                        List<JsonObject> jobs = results.result();
                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                .putHeader("X-Total-Count", jobs.size() + "" )
                                .end(Json.encodePrettily(jobs));
                    }
            );
        }

    }

    /**
     * Get one task LIVE status with task id specified
     *
     * @api {get} /status/:taskId   2. Get a task status
     * @apiVersion 0.1.1
     * @apiName getOneStatus
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get task status with specified task id.
     * @apiParam {String}   taskId      taskId for connect or transform.
     * @apiSuccess	{JsonObject[]}	status    status of the task.
     * @apiSampleRequest http://localhost:8080/api/df/status/:taskId
     */
    private void getOneStatus(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, id));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response()
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9001));
                        LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    if(dfJob.getConnectorCategory().equalsIgnoreCase("CONNECT")) {
                        // Find status from Kafka Connect
                        ProcessorConnectKafka.forwardGetAsGetOne(routingContext, wc_connect,
                                kafka_connect_rest_host, kafka_connect_rest_port, id);
                    }
                    if(dfJob.getConnectorCategory().equalsIgnoreCase("TRANSFORM")) {

                        if(dfJob.getConnectorType().contains("FLINK"))
                            ProcessorTransformFlink.forwardGetAsJobStatus(
                                    routingContext, wc_flink,
                                    flink_server_host,
                                    flink_rest_server_port,
                                    id, dfJob.getFlinkIDFromJobConfig()
                            );

                        if(dfJob.getConnectorType().contains("SPARK"))
                            ProcessorTransformSpark.forwardGetAsJobStatus(
                                    routingContext, wc_spark, dfJob,
                                    spark_livy_server_host, spark_livy_server_port
                            );

                    }
                } else {
                    routingContext.response()
                            .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                            .end(DFAPIMessage.getResponseMessage(9002));
                    LOG.error(DFAPIMessage.logResponseMessage(9002, id));
                }
            });
        }
    }

    /**
     * Connects specific addOne End Point for Rest API
     * @param routingContext
     *
     * @api {post} /ps 4.Add a connect task
     * @apiVersion 0.1.1
     * @apiName addOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how we add or submit a connect to DataFibers.
     * @apiParam    {String}  None        Json String of task as message body.
     * @apiSuccess (201) {JsonObject[]} connect     The newly added connect task.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 409 Conflict
     *     {
     *       "code" : "409",
     *       "message" : "POST Request exception - Conflict"
     *     }
     */
    private void addOneConnects(RoutingContext routingContext) {
        final DFJobPOPJ dfJob = Json.decodeValue(
                HelpFunc.cleanJsonConfig(routingContext.getBodyAsString()), DFJobPOPJ.class);
        // Set initial status for the job
        dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());
        String mongoId = (dfJob.getId() != null && !dfJob.getId().isEmpty())? dfJob.getId() : new ObjectId().toString();
        dfJob.setConnectUid(mongoId).setId(mongoId).getConnectorConfig().put("cuid", mongoId);
        LOG.debug("newly added connect is " + dfJob.toJson());
        // Find and set default connector.class
        if(!dfJob.getConnectorConfig().containsKey(ConstantApp.PK_KAFKA_CONNECTOR_CLASS) ||
                dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_CONNECTOR_CLASS) == null) {
            String connectorClass =
                    mongoDFInstalled.lkpCollection("connectorType", dfJob.getConnectorType(), "class");
            if(connectorClass == null || connectorClass.isEmpty()) {
                LOG.info(DFAPIMessage.logResponseMessage(9024, mongoId + " - " + connectorClass));
            } else {
                dfJob.getConnectorConfig().put(ConstantApp.PK_KAFKA_CONNECTOR_CLASS, connectorClass);
                LOG.info(DFAPIMessage.logResponseMessage(1018, mongoId + " - " + connectorClass));
            }
        } else {
            LOG.debug("Use connector.class in the config received");
        }
        // Start Kafka Connect REST API Forward only if Kafka is enabled and Connector type is Kafka Connect
        if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("CONNECT")) {
            // Auto fix "name" in Connect Config when mismatch with Connector Name
            if (!dfJob.getConnectUid().equalsIgnoreCase(dfJob.getConnectorConfig().get("name")) &&
                    dfJob.getConnectorConfig().get("name") != null) {
                dfJob.getConnectorConfig().put("name", dfJob.getConnectUid());
                LOG.info(DFAPIMessage.logResponseMessage(1004, dfJob.getId()));
            }
            ProcessorConnectKafka.forwardPOSTAsAddOne(routingContext, wc_connect, mongo, COLLECTION,
                    kafka_connect_rest_host, kafka_connect_rest_port, dfJob);
        } else {
            mongo.insert(COLLECTION, dfJob.toJson(), r ->
                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                            .setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                            .end(Json.encodePrettily(dfJob)));
            LOG.warn(DFAPIMessage.logResponseMessage(9008, mongoId));
        }
    }

    /**
     * Transforms specific addOne End Point for Rest API
     * @param routingContext
     *
     * @api {post} /tr 4.Add a transform task
     * @apiVersion 0.1.1
     * @apiName addOneTransforms
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is how we submit or add a transform task to DataFibers.
     * @apiParam   {String}	 None        Json String of task as message body.
     * @apiSuccess (201) {JsonObject[]} connect     The newly added connect task.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 409 Conflict
     *     {
     *       "code" : "409",
     *       "message" : "POST Request exception - Conflict."
     *     }
     */
    private void addOneTransforms(RoutingContext routingContext) {
        String rawResBody = HelpFunc.cleanJsonConfig(routingContext.getBodyAsString());
        final DFJobPOPJ dfJob = Json.decodeValue(rawResBody, DFJobPOPJ.class);

        dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());
        String mongoId = (dfJob.getId() != null && !dfJob.getId().isEmpty())? dfJob.getId() : new ObjectId().toString();
        dfJob.setConnectUid(mongoId).setId(mongoId).getConnectorConfig().put(ConstantApp.PK_TRANSFORM_CUID, mongoId);

        if(dfJob.getConnectorType().contains("SPARK") && dfJob.getConnectorType().contains("TRANSFORM")) {
            LOG.info("calling spark engine with body = " + dfJob.toJson());
            ProcessorTransformSpark.forwardPostAsAddOne(vertx, wc_spark, dfJob, mongo, COLLECTION,
                    spark_livy_server_host, spark_livy_server_port, rawResBody
            );
        } else {
            // Flink refers to KafkaServerHostPort.java
            JsonObject para = HelpFunc.getFlinkJarPara(dfJob,
                    this.kafka_server_host_and_port,
                    this.schema_registry_host_and_port);

            ProcessorTransformFlink.forwardPostAsSubmitJar(wc_flink, dfJob, mongo, COLLECTION,
                    flink_server_host, flink_rest_server_port, flink_jar_id,
                    para.getString("allowNonRestoredState"),
                    para.getString("savepointPath"),
                    para.getString("entryClass"),
                    para.getString("parallelism"),
                    para.getString("programArgs"));
        }

        mongo.insert(COLLECTION, dfJob.toJson(), r ->
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                        .end(Json.encodePrettily(dfJob)));
        LOG.info(DFAPIMessage.logResponseMessage(1000, dfJob.getId()));
    }

    /**
     * Add one model's meta information to repository
     */
    private void addOneModel(RoutingContext routingContext) {
        String rawResBody = HelpFunc.cleanJsonConfig(routingContext.getBodyAsString());
        LOG.debug("TESTING addOneModel - resRaw = " + rawResBody);
        final DFModelPOPJ dfModel = Json.decodeValue(rawResBody, DFModelPOPJ.class);
        String mongoId = (dfModel.getId() != null && !dfModel.getId().isEmpty())? dfModel.getId() : new ObjectId().toString();
        dfModel.setId(mongoId);

        mongo.insert(COLLECTION_MODEL, dfModel.toJson(), r ->
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                        .end(Json.encodePrettily(dfModel)));
        LOG.info(DFAPIMessage.logResponseMessage(1000, dfModel.getId()));
    }

    /** Add one schema to schema registry
     *
     * @api {post} /schema 3.Add a Schema
     * @apiVersion 0.1.1
     * @apiName addOneSchema
     * @apiGroup Schema
     * @apiPermission none
     * @apiDescription This is how we add a new schema to schema registry service
     * @apiParam   {String}  None        Json String of Schema as message body.
     * @apiSuccess (201) {JsonObject[]} connect     The newly added connect task.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 409 Conflict
     *     {
     *       "code" : "409",
     *       "message" : "POST Request exception - Conflict"
     *     }
     */
    private void addOneSchema(RoutingContext routingContext) {
        ProcessorTopicSchemaRegistry.forwardAddOneSchema(routingContext, wc_schema,
                kafka_server_host, schema_registry_rest_port);

        JsonObject jsonObj = routingContext.getBodyAsJson();
        // Since Vertx kafka admin still need zookeeper, we now use kafka native admin api until vertx version get updated
        KafkaAdminClient.createTopic(kafka_server_host_and_port,
                jsonObj.getString("id"),
                jsonObj.containsKey(ConstantApp.TOPIC_KEY_PARTITIONS)?
                        jsonObj.getInteger(ConstantApp.TOPIC_KEY_PARTITIONS) : 1,
                jsonObj.containsKey(ConstantApp.TOPIC_KEY_REPLICATION_FACTOR)?
                        jsonObj.getInteger(ConstantApp.TOPIC_KEY_REPLICATION_FACTOR) : 1
        );
    }

    /**
     * Connects specific pause or resume End Point for Rest API
     * @param routingContext
     *
     * @api {put} /ps/:id   5.Pause or resume a connect task
     * @apiVersion 0.1.1
     * @apiName updateOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how we pause or resume a connect task.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 404 Not Found
     *     {
     *       "code" : "409",
     *       "message" : "PUT Request exception - Not Found."
     *     }
     */
    private void putOneConnect(RoutingContext routingContext) {
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(),DFJobPOPJ.class);
        String status = dfJob.getStatus();
        if(status.equalsIgnoreCase(ConstantApp.KAFKA_CONNECT_ACTION_PAUSE) ||
                status.equalsIgnoreCase(ConstantApp.KAFKA_CONNECT_ACTION_RESUME)) {
            ProcessorConnectKafka.forwardPUTAsPauseOrResumeOne(routingContext, wc_connect, mongo, COLLECTION,
                    kafka_connect_rest_host, kafka_connect_rest_port, dfJob, status);
        } else {
            updateOneConnects(routingContext, status.equalsIgnoreCase("restart") ? true : false);
        }
    }

    /**
     * Connects specific updateOne End Point for Rest API
     * @param routingContext
     *
     * @api {put} /ps/:id   5.Update a connect task
     * @apiVersion 0.1.1
     * @apiName updateOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how we update the connect configuration to DataFibers.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 404 Not Found
     *     {
     *       "code" : "409",
     *       "message" : "PUT Request exception - Not Found."
     *     }
     */
    private void updateOneConnects(RoutingContext routingContext, Boolean enforceSubmit) {
        final String id = routingContext.request().getParam("id");
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(),DFJobPOPJ.class);

        String connectorConfigString = HelpFunc.mapToJsonStringFromHashMapD2U(dfJob.getConnectorConfig());
        JsonObject json = dfJob.toJson();

        if (id == null || json == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            // Implement connectConfig change detection to decide if we need REST API forwarding
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id),
                    new JsonObject().put("connectorConfig", 1), res -> {
                if (res.succeeded()) {
                    String before_update_connectorConfigString = res.result().getJsonObject("connectorConfig").toString();
                    // Detect changes in connectConfig
                    if (enforceSubmit || (this.kafka_connect_enabled && dfJob.getConnectorType().contains("CONNECT") &&
                            connectorConfigString.compareTo(before_update_connectorConfigString) != 0)) {
                       ProcessorConnectKafka.forwardPUTAsUpdateOne(routingContext, wc_connect,
                               mongo, COLLECTION, kafka_connect_rest_host, kafka_connect_rest_port ,dfJob);
                    } else { // Where there is no change detected
                        LOG.info(DFAPIMessage.logResponseMessage(1007, id));
                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id), // Select a unique document
                                // The update syntax: {$set, the json object containing the fields to update}
                                new JsonObject().put("$set", dfJob.toJson()), v -> {
                                    if (v.failed()) {
                                        routingContext.response()
                                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                                .end(DFAPIMessage.getResponseMessage(9003));
                                        LOG.error(DFAPIMessage.logResponseMessage(9003, id));
                                    } else {
                                        HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                                .end(DFAPIMessage.getResponseMessage(1001));
                                        LOG.info(DFAPIMessage.logResponseMessage(1001, id));
                                    }
                                }
                        );
                    }
                } else {
                    LOG.error(DFAPIMessage.
                            logResponseMessage(9014, id + " details - " + res.cause()));
                }
            });
        }
    }

    /**
     * Transforms specific updateOne End Point for Rest API
     * @param routingContext
     *
     * @api {put} /tr/:id   5.Update a transform task
     * @apiVersion 0.1.1
     * @apiName updateOneConnects
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is how we update the transform configuration to DataFibers.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 404 Not Found
     *     {
     *       "code" : "409",
     *       "message" : "PUT Request exception - Not Found."
     *     }
     */
    private void updateOneTransforms(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        String connectorConfigString = HelpFunc.mapToJsonStringFromHashMapD2U(dfJob.getConnectorConfig());
        JsonObject json = dfJob.toJson();

        if (id == null || json == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            // Implement connectConfig change detection to decide if we need REST API forwarding
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id),
                    new JsonObject().put("connectorConfig", 1), res -> {
                        if (res.succeeded()) {
                            String before_update_connectorConfigString = res.result()
                                    .getJsonObject("connectorConfig").toString();
                            // Detect changes in connectConfig
                            if (this.transform_engine_flink_enabled &&
                                    dfJob.getConnectorType().contains("FLINK") &&
                                    connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {

                                JsonObject para = HelpFunc.getFlinkJarPara(dfJob,
                                        this.kafka_server_host_and_port,
                                        this.schema_registry_host_and_port
                                );

                                //if task is running then cancel and resubmit, else resubmit
                                if(dfJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())) {
                                    ProcessorTransformFlink.forwardPutAsRestartJob(
                                            routingContext,
                                            wc_flink,
                                            mongo, COLLECTION_INSTALLED, COLLECTION,
                                            flink_server_host, flink_rest_server_port, flink_jar_id,
                                            dfJob.getJobConfig().get(ConstantApp.PK_FLINK_SUBMIT_JOB_ID), dfJob,
                                            para.getString("allowNonRestoredState"),
                                            para.getString("savepointPath"),
                                            para.getString("entryClass"),
                                            para.getString("parallelism"),
                                            para.getString("programArgs")
                                    );
                                } else {
                                    ProcessorTransformFlink.forwardPostAsSubmitJar(
                                            wc_flink, dfJob, mongo, COLLECTION,
                                            flink_server_host, flink_rest_server_port, flink_jar_id,
                                            para.getString("allowNonRestoredState"),
                                            para.getString("savepointPath"),
                                            para.getString("entryClass"),
                                            para.getString("parallelism"),
                                            para.getString("programArgs"));
                                }

                                //update df status properly before response
                                dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());

                            } else if (this.transform_engine_spark_enabled &&
                                        dfJob.getConnectorType().contains("SPARK") &&
                                        connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {

                                //update df status properly before response
                                dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());

                                if(dfJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_FLAG)
                                        .equalsIgnoreCase("true")) {
                                    dfJob.setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
                                            ConstantApp.DF_STATUS.UNASSIGNED.name());
                                }

                                ProcessorTransformSpark.forwardPutAsUpdateOne(
                                        vertx, wc_spark,
                                        dfJob, mongo, COLLECTION,
                                        spark_livy_server_host, spark_livy_server_port
                                );

                            } else {
                                LOG.info(DFAPIMessage.logResponseMessage(1007, id));
                            }
                            // Where there is no change detected or detected changes, always update repo right away
                            // The only difference is whether to update status. Either way, do not remove tasks.
                            mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id), // Select a unique document
                                    // The update syntax: {$set, the json object containing the fields to update}
                                    new JsonObject().put("$set", dfJob.toJson()), v -> {
                                        if (v.failed()) {
                                            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                                    .end(DFAPIMessage.getResponseMessage(9003));
                                            LOG.error(DFAPIMessage.logResponseMessage(9003, id));
                                        } else {
                                            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                                    .end(DFAPIMessage.getResponseMessage(1001));
                                            LOG.info(DFAPIMessage.logResponseMessage(1001, id));
                                        }
                                    }
                            );
                        } else {
                            LOG.error(DFAPIMessage.
                                    logResponseMessage(9014, id + " details - " + res.cause()));
                        }
                    });
        }
    }

    /**
     * Update one model information
     */
    private void updateOneModel(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        final DFModelPOPJ dfModel = Json.decodeValue(routingContext.getBodyAsString(),DFModelPOPJ.class);

        if (id == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            mongo.updateCollection(COLLECTION_MODEL, new JsonObject().put("_id", id), // Select a unique document
                    // The update syntax: {$set, the json object containing the fields to update}
                    new JsonObject().put("$set", dfModel.toJson()), v -> {
                        if (v.failed()) {
                            routingContext.response()
                                    .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                    .end(DFAPIMessage.getResponseMessage(9003));
                            LOG.error(DFAPIMessage.logResponseMessage(9003, id));
                        } else {
                            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                    .end(DFAPIMessage.getResponseMessage(1001));
                            LOG.info(DFAPIMessage.logResponseMessage(1001, id));
                        }
                    }
            );
        }
    }

    /**
     * Update specified schema in schema registry
     * @api {put} /schema/:id   4.Update a schema
     * @apiVersion 0.1.1
     * @apiName updateOneSchema
     * @apiGroup Schema
     * @apiPermission none
     * @apiDescription This is how we update specified schema information in schema registry.
     * @apiParam    {String}    subject  schema subject in schema registry.
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 404 Not Found
     *     {
     *       "code" : "409",
     *       "message" : "PUT Request exception - Not Found."
     *     }
     */
    private void updateOneSchema(RoutingContext routingContext) {
        ProcessorTopicSchemaRegistry.forwardUpdateOneSchema(routingContext, wc_schema,
                kafka_server_host, schema_registry_rest_port);
    }

    /**
     * Connects specific deleteOne End Point for Rest API
     * @param routingContext
     *
     * @api {delete} /ps/:id   6.Delete a connect task
     * @apiVersion 0.1.1
     * @apiName deleteOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how to delete a specific connect.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {
     *       "code" : "400",
     *       "message" : "Delete Request exception - Bad Request."
     *     }
     */
    private void deleteOneConnects(RoutingContext routingContext) {
        String id = routingContext.request().getParam("id");
        
        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9001));
                        LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    if (this.kafka_connect_enabled &&
                            (dfJob.getConnectorType().contains("CONNECT"))){
                        ProcessorConnectKafka.forwardDELETEAsDeleteOne(routingContext, wc_connect, mongo, COLLECTION,
                                kafka_connect_rest_host, kafka_connect_rest_port, dfJob);
                    } else {
                        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                remove -> routingContext.response()
                                        .end(DFAPIMessage.getResponseMessage(1002, id)));
                        LOG.info(DFAPIMessage.logResponseMessage(1002, id));
                    }
                }
            });
        }
    }
	
    /**
     * Transforms specific deleteOne End Point for Rest API
     * @param routingContext
     *
     * @api {delete} /tr/:id   6.Delete a transform task
     * @apiVersion 0.1.1
     * @apiName deleteOneTransforms
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is how to delete a specific transform.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {
     *       "code" : "400",
     *       "message" : "Delete Request exception - Bad Request."
     *     }
     */
    private void deleteOneTransforms(RoutingContext routingContext) {
        String id = routingContext.request().getParam("id");

        if (id == null) {
            routingContext.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {

                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    String jobId;
                    // delete stream back files if exist
                    if(dfJob.getConnectorConfig().containsKey(ConstantApp.PK_TRANSFORM_STREAM_BACK_PATH)) {
                        File streamBackFolder =
                                new File(dfJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_PATH));
                        try {
                            FileUtils.deleteDirectory(streamBackFolder);
                            LOG.info("STREAM_BACK_FOLDER_DELETED " + streamBackFolder.toString());
                        } catch (IOException ioe) {
                            LOG.error("DELETE_STREAM_BACK_FOLDER_FAILED " + ioe.getCause());
                        }
                    }

                    // When jobConfig is null, we can remove task with canceling the underlying service job
                    if (dfJob.getJobConfig() == null) {
                        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                remove -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                        .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                        .end(DFAPIMessage.getResponseMessage(1002)));
                        LOG.info(DFAPIMessage.logResponseMessage(1002, id + "- service job info not found"));

                    } else if (dfJob.getJobConfig() != null &&
                            this.transform_engine_flink_enabled &&
                            dfJob.getConnectorType().contains("FLINK") &&
                            dfJob.getJobConfig().containsKey(ConstantApp.PK_FLINK_SUBMIT_JOB_ID)) {

                        jobId = dfJob.getJobConfig().get(ConstantApp.PK_FLINK_SUBMIT_JOB_ID);
                        if (dfJob.getStatus().equalsIgnoreCase("RUNNING")) {
                            // For cancel a running job, we want remove tasks from repo only when cancel is done
                            ProcessorTransformFlink.forwardDeleteAsCancelJob(
                                    routingContext, wc_flink,
                                    mongo, COLLECTION,
                                    this.flink_server_host, this.flink_rest_server_port,
                                    jobId);
                            LOG.info(DFAPIMessage.logResponseMessage(1006, id));
                        } else {
                            mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                    remove -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .end(DFAPIMessage.getResponseMessage(1002)));
                            LOG.info(DFAPIMessage.logResponseMessage(1002,
                                    id + "- FLINK_JOB_NOT_RUNNING"));
                        }

                    } else if (dfJob.getJobConfig() != null &&
                            this.transform_engine_spark_enabled &&
                            dfJob.getConnectorType().contains("SPARK") &&
                            dfJob.getJobConfig().containsKey(ConstantApp.PK_LIVY_SESSION_ID)) {

                        jobId = dfJob.getJobConfig().get(ConstantApp.PK_LIVY_SESSION_ID);
                        if (dfJob.getStatus().equalsIgnoreCase("RUNNING")) {
                            // For cancel a running job, we want remove tasks from repo only when cancel is done
                            ProcessorTransformSpark.forwardDeleteAsCancelOne(
                                    routingContext, wc_spark,
                                    mongo, COLLECTION,
                                    this.spark_livy_server_host, this.spark_livy_server_port,
                                    jobId);
                            LOG.info(DFAPIMessage.logResponseMessage(1006, id));
                        } else {
                            mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                    remove -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .end(DFAPIMessage.getResponseMessage(1002)));
                            LOG.info(DFAPIMessage.logResponseMessage(1002,
                                    id + "- SPARK_JOB_NOT_RUNNING"));
                        }
                    } else {
                        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                remove -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                        .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                        .end(DFAPIMessage.getResponseMessage(1002)));
                        LOG.info(DFAPIMessage.logResponseMessage(1002, id + "- Has jobConfig but missing service job id."));
                    }
                } else {
                    routingContext.response()
                            .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                            .end(DFAPIMessage.getResponseMessage(9001));
                    LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                }
            });
        }
    }

    /**
     * Delete model information as well as mode it's self. Now, we use hdfs cml for instead
     */
    private void deleteOneModel(RoutingContext routingContext) {
        String id = routingContext.request().getParam("id");

        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
            mongo.findOne(COLLECTION_MODEL, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9001));
                        LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                        return;
                    } else {
                        // TODO call hdfs command or rest api to delete the model
                        DFModelPOPJ dfModelPOPJ = new DFModelPOPJ(ar.result());
                        wc_connect.delete(webhdfs_rest_port, webhdfs_rest_hostname,
                                ConstantApp.WEBHDFS_REST_URL + "/" + dfModelPOPJ.getPath() + ConstantApp.WEBHDFS_REST_DELETE_PARA)
                                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                .sendJsonObject(DFAPIMessage.getResponseJsonObj(1002),
                                        ard -> {
                                            if (ard.succeeded()) {
                                                mongo.removeDocument(COLLECTION_MODEL, new JsonObject().put("_id", id),
                                                        remove -> routingContext.response()
                                                                .end(DFAPIMessage.getResponseMessage(1002, id)));
                                                LOG.info(DFAPIMessage.logResponseMessage(1002, id));
                                            } else {
                                                routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                                        .end(DFAPIMessage.getResponseMessage(9001));
                                                LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                                            }
                                        });
                    }
                }
            });
        }
    }

    /**
     * Schema specific deleteOne End Point for Rest API
     * @param routingContext
     *
     * @api {delete} /schema/:id   6.Delete a schema/topic
     * @apiVersion 0.1.1
     * @apiName deleteOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how to delete a specific schema/topic.
     * @apiParam    {String}    id  schema subject(or topic).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {
     *       "code" : "400",
     *       "message" : "Delete Request exception - Bad Request."
     *     }
     */
    private void deleteOneSchema(RoutingContext routingContext) {
        ProcessorTopicSchemaRegistry.forwardDELETEAsDeleteOne(routingContext, wc_schema,
                kafka_server_host, schema_registry_rest_port);
        KafkaAdminClient.deleteTopics(kafka_server_host_and_port, routingContext.request().getParam("id"));
    }

    /**
     * Start a Kafka connect in background to keep sinking meta data to mongodb
     * This is blocking process since it is a part of application initialization.
     */
    private void startMetadataSink() {

        // Check if the sink is already started. If yes, do not start
        String restURI = "http://" + this.kafka_connect_rest_host + ":" + this.kafka_connect_rest_port +
                ConstantApp.KAFKA_CONNECT_REST_URL;
        String metaDBHost = config().getString("repo.connection.string", "mongodb://localhost:27017")
                .replace("//", "").split(":")[1];
        String metaDBPort = config().getString("repo.connection.string", "mongodb://localhost:27017")
                .replace("//", "").split(":")[2];
        String metaDBName = config().getString("db.name", "DEFAULT_DB");

        // Create meta-database if it is not exist
        new MongoAdminClient(metaDBHost, Integer.parseInt(metaDBPort), metaDBName)
                .createCollection(this.COLLECTION_META)
                .close();

        String metaSinkConnect = new JSONObject().put("name", "metadata_sink_connect").put("config",
                new JSONObject().put("connector.class", "org.apache.kafka.connect.mongodb.MongodbSinkConnector")
                        .put("tasks.max", "2")
                        .put("host", metaDBHost)
                        .put("port", metaDBPort)
                        .put("bulk.size", "1")
                        .put("mongodb.database", metaDBName)
                        .put("mongodb.collections", config().getString("db.metadata.collection.name", this.COLLECTION_META))
                        .put("topics", config().getString("kafka.topic.df.metadata", "df_meta"))).toString();
        try {
            HttpResponse<String> res = Unirest.get(restURI + "/metadata_sink_connect/status")
                    .header("accept", ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .asString();

            if(res.getStatus() == ConstantApp.STATUS_CODE_NOT_FOUND) { // Add the meta sink
                Unirest.post(restURI)
                        .header("accept", "application/json").header("Content-Type", "application/json")
                        .body(metaSinkConnect).asString();
            }

            // Add the avro schema for metadata as well since df_meta-value maybe added only
            String dfMetaSchemaSubject = config().getString("kafka.topic.df.metadata", "df_meta");
            String schemaRegistryRestURL = "http://" + this.schema_registry_host_and_port + "/subjects/" +
                    dfMetaSchemaSubject + "/versions";

            HttpResponse<String> schmeaRes = Unirest.get(schemaRegistryRestURL + "/latest")
                    .header("accept", ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .asString();

            if(schmeaRes.getStatus() == ConstantApp.STATUS_CODE_NOT_FOUND) { // Add the meta sink schema
                Unirest.post(schemaRegistryRestURL)
                        .header("accept", ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                        .header("Content-Type", ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                        .body(new JSONObject().put("schema", config().getString("df.metadata.schema",
                                "{\"type\":\"record\"," +
                                        "\"name\": \"df_meta\"," +
                                        "\"fields\":[" +
                                        "{\"name\": \"cuid\", \"type\": \"string\"}," +
                                        "{\"name\": \"file_name\", \"type\": \"string\"}," +
                                        "{\"name\": \"file_size\", \"type\": \"string\"}, " +
                                        "{\"name\": \"file_owner\", \"type\": \"string\"}," +
                                        "{\"name\": \"last_modified_timestamp\", \"type\": \"string\"}," +
                                        "{\"name\": \"current_timestamp\", \"type\": \"string\"}," +
                                        "{\"name\": \"current_timemillis\", \"type\": \"long\"}," +
                                        "{\"name\": \"stream_offset\", \"type\": \"string\"}," +
                                        "{\"name\": \"topic_sent\", \"type\": \"string\"}," +
                                        "{\"name\": \"schema_subject\", \"type\": \"string\"}," +
                                        "{\"name\": \"schema_version\", \"type\": \"string\"}," +
                                        "{\"name\": \"status\", \"type\": \"string\"}]}"
                                )).toString()
                        ).asString();
                LOG.info(DFAPIMessage.logResponseMessage(1008, "META_DATA_SCHEMA_REGISTRATION"));
            } else {
                LOG.info(DFAPIMessage.logResponseMessage(1009, "META_DATA_SCHEMA_REGISTRATION"));
            }

            JSONObject resObj = new JSONObject(res.getBody());
            String status = "Unknown";
            if (resObj.has("connector")) {
                status = resObj.getJSONObject("connector").get("state").toString();
            }

            LOG.info(DFAPIMessage.logResponseMessage(1010, "topic:df_meta, status:" + status));

        } catch (UnirestException ue) {
            LOG.error(DFAPIMessage
                    .logResponseMessage(9015, "exception details - " + ue.getCause()));
        }
    }

    /**
     * Get initial method to import all active connectors from Kafka connect to mongodb repo
     * The same function does not apply to the transforms since we are not able to rebuild their configs
     */
    private void importAllFromKafkaConnect() {
        LOG.info(DFAPIMessage.logResponseMessage(1015, "CONNECT_IMPORT"));
        String restURI = "http://" + this.kafka_connect_rest_host+ ":" + this.kafka_connect_rest_port +
                ConstantApp.KAFKA_CONNECT_REST_URL;
        try {
            HttpResponse<String> res = Unirest.get(restURI)
                    .header("accept", "application/json").asString();
            String resStr = res.getBody();
            //LOG.debug(DFAPIMessage.logResponseMessage(1011, resStr));
            if (resStr.compareToIgnoreCase("[]") != 0 && !resStr.equalsIgnoreCase("[null]")) { //Has active connectors
                for (String connectName: resStr.substring(2,resStr.length()-2).split("\",\"")) {
                    if (connectName.equalsIgnoreCase("null")) continue;
                    // Get connector config
                    HttpResponse<JsonNode> resConnector = Unirest.get(restURI + "/" + connectName + "/config")
                            .header("accept", "application/json").asJson();
                    JsonNode resConfig = resConnector.getBody();
                    String resConnectTypeTmp = ConstantApp.DF_CONNECT_TYPE.NONE.name();
                    if(resConfig.getObject().has("connector.class"))
                        resConnectTypeTmp = resConfig.getObject().getString("connector.class");
                    String resConnectName = resConfig.getObject().getString("name");
                    String resConnectType;

                    if (resConnectName.equalsIgnoreCase("metadata_sink_connect")) {
                        resConnectType = ConstantApp.DF_CONNECT_TYPE.INTERNAL_METADATA_COLLECT.name();
                    } else {
                        resConnectType = mongoDFInstalled.lkpCollection("class", resConnectTypeTmp, "connectorType");
                    }

                    // Get task status
                    HttpResponse<JsonNode> resConnectorStatus = Unirest.get(restURI + "/" + connectName + "/status")
                            .header("accept", "application/json").asJson();

                    // Sometime, the status is still missing for active connectors (active <> running)
                    String resStatus = (resConnectorStatus.getStatus() == 200) ?
                            HelpFunc.getTaskStatusKafka(resConnectorStatus.getBody().getObject()) :
                            ConstantApp.DF_STATUS.LOST.name();

                    mongo.count(COLLECTION, new JsonObject().put("connectUid", connectName), count -> {
                        if (count.succeeded()) {
                            if (count.result() == 0) {
                                // No jobs found, then insert json data
                                DFJobPOPJ insertJob = new DFJobPOPJ (
                                        new JsonObject()
                                                .put("_id", connectName)
                                                .put("name", "imported " + connectName)
                                                .put("taskSeq", "0")
                                                .put("connectUid", connectName)
                                                .put("connectorType", resConnectType)
                                                .put("connectorCategory", "CONNECT")
                                                .put("status", resStatus)
                                                .put("jobConfig", new JsonObject()
                                                .put("comments", "This is imported from Kafka Connect."))
                                                .put("connectorConfig", new JsonObject(resConfig.getObject().toString()))
                                );
                                mongo.insert(COLLECTION, insertJob.toJson(), ar -> {
                                    if (ar.failed()) {
                                        LOG.error(DFAPIMessage
                                                .logResponseMessage(9016,
                                                        "exception_details - " + ar.cause()));
                                    } else {
                                        LOG.debug(DFAPIMessage
                                                .logResponseMessage(1012, "CONNECT_IMPORT"));
                                    }
                                });
                            } else { // Update the connectConfig portion from Kafka import
                                mongo.findOne(COLLECTION, new JsonObject().put("connectUid", connectName), null,
                                        findidRes -> {
                                        if (findidRes.succeeded()) {
                                            DFJobPOPJ updateJob = new DFJobPOPJ(findidRes.result());
                                            try {
                                                updateJob.setStatus(resStatus).setConnectorConfig(
                                                        new ObjectMapper().readValue(resConfig.getObject().toString(),
                                                                new TypeReference<HashMap<String, String>>(){}));

                                            } catch (IOException ioe) {
                                                LOG.error(DFAPIMessage.logResponseMessage(9017,
                                                                        "CONNECT_IMPORT - " + ioe.getCause()));
                                            }

                                        mongo.updateCollection(COLLECTION,
                                                new JsonObject().put("_id", updateJob.getId()),
                                                // The update syntax: {$set, json object containing fields to update}
                                                new JsonObject().put("$set", updateJob.toJson()), v -> {
                                                    if (v.failed()) {
                                                        LOG.error(DFAPIMessage.logResponseMessage(9003,
                                                                "CONNECT_IMPORT - " + updateJob.getId()
                                                                        + "-" + v.cause()));
                                                    } else {
                                                        LOG.debug(DFAPIMessage.logResponseMessage(1001,
                                                                "CONNECT_IMPORT - " + updateJob.getId()));
                                                    }
                                                }
                                        );

                                    } else {
                                            LOG.error(DFAPIMessage
                                                    .logResponseMessage(9002,
                                                            "CONNECT_IMPORT - " + findidRes.cause()));
                                    }
                                });
                            }
                        } else {
                            // report the error
                            LOG.error(DFAPIMessage
                                    .logResponseMessage(9018,
                                            "exception_details - " + connectName + " - " + count.cause()));
                        }
                    });
                }
            } else {
                LOG.info(DFAPIMessage.logResponseMessage(1013, "CONNECT_IMPORT"));
            }
        } catch (UnirestException ue) {
            LOG.error(DFAPIMessage.logResponseMessage(9006, "CONNECT_IMPORT - " + ue.getCause()));
        }
        LOG.info(DFAPIMessage.logResponseMessage(1014, "CONNECT_IMPORT"));
    }

    /**
     * Keep refreshing the active Kafka connectors' status in repository against remote Kafka REST Server
     */
    private void updateKafkaConnectorStatus() {
        List<String> list = new ArrayList<>();
        // Add all Kafka connect
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*connect.*)"); // case insensitive matching
        list.add(ConstantApp.DF_CONNECT_TYPE.INTERNAL_METADATA_COLLECT.name()); // update metadata sink as well

        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
            if (result.succeeded()) {
                for (JsonObject json : result.result()) {
                    String connectName = json.getString("connectUid");
                    String statusRepo = json.getString("status");
                    String taskId = json.getString("_id");

                    wc_refresh.get(kafka_connect_rest_port, kafka_connect_rest_host,
                            ConstantApp.KAFKA_CONNECT_REST_URL + "/" + connectName + "/status")
                            .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                            .send(ar -> {
                                        if (ar.succeeded()) {
                                            String resStatus =
                                                    (ar.result().statusCode() == ConstantApp.STATUS_CODE_NOT_FOUND) ?
                                                    ConstantApp.DF_STATUS.LOST.name():// Not find - Mark status as LOST
                                                    HelpFunc.getTaskStatusKafka(ar.result().bodyAsJsonObject());

                                            // Do change detection on status
                                            /*
                                            We detect that when adding new connect, some exiting running connect will
                                            become UNASSIGNED within short time from RUNNING, then it goes back to normal.
                                            In this case, we do not want to show it if the coming status is UNASSIGNED and
                                            status in repo is RUNNING.
                                             */
                                            if (statusRepo.compareToIgnoreCase(resStatus) != 0 &&
                                                    !(resStatus.equalsIgnoreCase(ConstantApp.DF_STATUS.UNASSIGNED.name())
                                                    && statusRepo.equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()))
                                                ) { //status changes
                                                DFJobPOPJ updateJob = new DFJobPOPJ(json);
                                                updateJob.setStatus(resStatus);
                                                mongo.updateCollection(COLLECTION, new JsonObject().put("_id", taskId),
                                                        new JsonObject().put("$set", updateJob.toJson()), v -> {
                                                            if (v.failed()) {
                                                                LOG.error(DFAPIMessage.
                                                                        logResponseMessage(9003,
                                                                                taskId+ "cause:" + v.cause()));
                                                            } else {
                                                                LOG.info(DFAPIMessage.logResponseMessage(1019, taskId));
                                                            }
                                                        }
                                                );
                                            } else {
                                                LOG.debug(DFAPIMessage.logResponseMessage(1020, taskId));
                                            }

                                        } else {
                                            LOG.error(DFAPIMessage.logResponseMessage(9006, ar.cause().getMessage()));
                                        }
                                    }
                            );
                }
            } else {
                LOG.error(DFAPIMessage.logResponseMessage(9002, result.cause().getMessage()));
            }
        });
    }

    /**
     * Keep refreshing the active Flink transforms/jobs' status in repository against remote Flink REST Server
     */
    private void updateFlinkJobStatus() {
        List<String> list = new ArrayList<>();
        // Add all transform
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*transform_exchange_flink.*)") ;

        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
            if (result.succeeded()) {
                for (JsonObject json : result.result()) {
                    String statusRepo = json.getString("status");
                    String taskId = json.getString("_id");
                    String jobId = ConstantApp.FLINK_DUMMY_JOB_ID;
                    DFJobPOPJ updateJob = new DFJobPOPJ(json);

                    if (json.getValue("jobConfig") == null ||
                            !json.getJsonObject("jobConfig").containsKey(ConstantApp.PK_FLINK_SUBMIT_JOB_ID)) {
                        updateJob.setStatus(ConstantApp.DF_STATUS.LOST.name());
                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                                // The update syntax: {$set, the json object containing the fields to update}
                                new JsonObject().put("$set", updateJob.toJson()), v -> {
                                    if (v.failed()) {
                                        LOG.error(DFAPIMessage.logResponseMessage(9003, taskId + "cause:" + v.cause()));
                                    } else {
                                        LOG.info(DFAPIMessage.logResponseMessage(1022, taskId));
                                    }
                                }
                        );

                    } else {
                        jobId = json.getJsonObject("jobConfig").getString(ConstantApp.PK_FLINK_SUBMIT_JOB_ID);
                        wc_refresh.get(flink_rest_server_port, flink_server_host,
                                ConstantApp.FLINK_REST_URL + "/" + jobId)
                                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                .send(ar -> {
                                    if (ar.succeeded()) {
                                        String resStatus = ar.result().statusCode() == ConstantApp.STATUS_CODE_NOT_FOUND ?
                                                ConstantApp.DF_STATUS.LOST.name() :// Not find - Mark status as LOST
                                                HelpFunc.getTaskStatusFlink(ar.result().bodyAsJsonObject());

                                        // Do change detection on status
                                        if (statusRepo.compareToIgnoreCase(resStatus) != 0) { //status changes
                                            updateJob.setStatus(resStatus);
                                            LOG.info(DFAPIMessage.logResponseMessage(1021, "Flink " + taskId));
                                        } else {
                                            LOG.debug(DFAPIMessage.logResponseMessage(1022, "Flink " + taskId));
                                        }
                                    } else {
                                        // When jobId not found, set status LOST with error message.
                                        LOG.info(DFAPIMessage.logResponseMessage(9006,
                                                "TRANSFORM_STATUS_REFRESH_FOUND " + taskId + " LOST"));
                                        updateJob.setStatus(ConstantApp.DF_STATUS.LOST.name());
                                    }

                                    // update status finally
                                    mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                                            // The update syntax: {$set, the json object containing the fields to update}
                                            new JsonObject().put("$set", updateJob.toJson()), v -> {
                                                if (v.failed()) {
                                                    LOG.error(DFAPIMessage.logResponseMessage(9003, taskId + "cause:" + v.cause()));
                                                } else {
                                                    LOG.debug(DFAPIMessage.logResponseMessage(1001, taskId));
                                                }
                                            }
                                    );
                                });
                    }
                }
            } else {
                LOG.error(DFAPIMessage.logResponseMessage(9002, "TRANSFORM_STATUS_REFRESH:" + result.cause()));
            }
        });
    }

    /**
     * Keep refreshing the active Spark transforms/jobs' status in repository against remote Livy REST Server
     * We need to find out three type of information
     * * session status
     * * statement status
     * * last query result in rich text format at "livy_statement_output"
     *
     * In addition, this function will trigger batch spark sql result set stream back when it is needed.
     */
    private void updateSparkJobStatus() {
        List<String> list = new ArrayList<>();
        // Add all transform
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*transform_exchange_spark.*)") ;
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*transform_model_spark.*)") ;

        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
            if (result.succeeded()) {
                for (JsonObject json : result.result()) {
                    String repoStatus = json.getString("status");
                    String taskId = json.getString("_id");
                    DFJobPOPJ updateJob = new DFJobPOPJ(json);

                    if (json.getValue("jobConfig") == null ||
                            !json.getJsonObject("jobConfig").containsKey(ConstantApp.PK_LIVY_SESSION_ID) ||
                            !json.getJsonObject("jobConfig").containsKey(ConstantApp.PK_LIVY_STATEMENT_ID)) {
                        updateJob.setStatus(ConstantApp.DF_STATUS.LOST.name());
                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                                new JsonObject().put("$set", updateJob.toJson()), v -> {
                                    if (v.failed()) {
                                        LOG.error(DFAPIMessage.logResponseMessage(9003,
                                                taskId + "cause:" + v.cause()));
                                    } else {
                                        LOG.debug(DFAPIMessage
                                                .logResponseMessage(1001,
                                                        "Missing Livy session/statement with task Id " + taskId));
                                    }
                                }
                        );

                    } else if( // update status in repo when it is in running or unassigned
                        // TODO may need to remove LOST later
                            updateJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()) ||
                            updateJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.STREAMING.name()) ||
                            updateJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.UNASSIGNED.name()) ||
                            updateJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.LOST.name())) {
                        String sessionId = json.getJsonObject("jobConfig").getString(ConstantApp.PK_LIVY_SESSION_ID);
                        String statementId = json.getJsonObject("jobConfig").getString(ConstantApp.PK_LIVY_STATEMENT_ID);
                        String repoFullCode = json.getJsonObject("jobConfig").getString(ConstantApp.PK_LIVY_STATEMENT_CODE);

                        // 1. Check if session is available
                        wc_refresh.get(spark_livy_server_port, spark_livy_server_host,
                                ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + "/state")
                                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                                        ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                .send(sar -> {
                                    if (sar.succeeded() && sar.result().statusCode() == ConstantApp.STATUS_CODE_OK) {
                                        updateJob.setJobConfig(ConstantApp.PK_LIVY_SESSION_STATE, sar.result().bodyAsJsonObject().getString("state"));

                                        LOG.debug("session info response = " + sar.result().bodyAsJsonObject());

                                        // 1. Check if statement is available
                                        wc_refresh
                                                .get(spark_livy_server_port, spark_livy_server_host,
                                                        ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + ConstantApp.LIVY_REST_URL_STATEMENTS + "/" + statementId)
                                                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                                                        ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                                .send(ar -> {
                                                    if (ar.succeeded() &&
                                                            ar.result().statusCode() == ConstantApp.STATUS_CODE_OK) {

                                                        JsonObject resultJo = ar.result().bodyAsJsonObject();
                                                        LOG.debug("statement info response = " + resultJo);
                                                        String resStatus = ar.result().statusCode() == ConstantApp.STATUS_CODE_NOT_FOUND ?
                                                                ConstantApp.DF_STATUS.LOST.name() :// Not find - Mark status as LOST
                                                                HelpFunc.getTaskStatusSpark(resultJo);

                                                        String resFullCode = ar.result().statusCode() == ConstantApp.STATUS_CODE_NOT_FOUND ?
                                                                "":resultJo.getString("code");

                                                        LOG.debug("repoStatus = " + repoStatus + " resStatus = " + resStatus);
                                                        LOG.debug("repoFullCode = " + repoFullCode + " resFullCode = " + resFullCode);

                                                        // Do change detection on status, but code snip must same
                                                        // Because the livy session/statement id could be reset
                                                        if (repoStatus.compareToIgnoreCase(resStatus) != 0 &&
                                                                repoFullCode.equalsIgnoreCase(resFullCode)) { //status changes

                                                            LOG.debug("Change Detected");

                                                            // Set df job status from statement state
                                                            updateJob
                                                                    .setStatus(resStatus)
                                                                    // Set statement state and progress
                                                                    .setJobConfig(
                                                                            ConstantApp.PK_LIVY_STATEMENT_STATE,
                                                                            resultJo.getString("state"))
                                                                    .setJobConfig(
                                                                            ConstantApp.PK_LIVY_STATEMENT_PROGRESS,
                                                                            resultJo.getValue("progress").toString())
                                                                    // Set statement status
                                                                    .setJobConfig(
                                                                            ConstantApp.PK_LIVY_STATEMENT_STATUS,
                                                                            resultJo.getJsonObject("output")
                                                                            .getString("status").toUpperCase())
                                                                    // Set detailed error and traceback in case of failed
                                                                    .setJobConfig(ConstantApp.PK_LIVY_STATEMENT_TRACEBACK,
                                                                            resultJo.getJsonObject("output")
                                                                            .containsKey("traceback")?
                                                                            resultJo.getJsonObject("output")
                                                                                    .getJsonArray("traceback")
                                                                                    .toString():"")
                                                                    .setJobConfig(ConstantApp.PK_LIVY_STATEMENT_EXCEPTION,
                                                                            resultJo.getJsonObject("output")
                                                                                    .containsKey("evalue")?
                                                                                    resultJo.getJsonObject("output")
                                                                                            .getString("evalue"):"")
                                                                    // Also set result
                                                                    .setJobConfig(ConstantApp.PK_LIVY_STATEMENT_OUTPUT, HelpFunc.livyTableResultToRichText(resultJo));

                                                            // Check if stream back is needed. If yes, enable it when batch is FINISHED.
                                                            // This has to come after above update dfJob since we do not want loose dfJob status
                                                            // when update stream back status in separate thread
                                                            if(resStatus.equalsIgnoreCase(ConstantApp.DF_STATUS.FINISHED.name()) &&
                                                                    updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_FLAG)
                                                                            .equalsIgnoreCase("true")) {
                                                                LOG.debug("Stream Back Enabled");
                                                                // Update repo for stream back job
                                                                ProcessorStreamBack.enableStreamBack(
                                                                        wc_streamback,
                                                                        updateJob,
                                                                        mongo, COLLECTION,
                                                                        schema_registry_rest_port, schema_registry_rest_hostname,
                                                                        df_rest_port, "localhost",
                                                                        HelpFunc.livyTableResultToAvroFields(
                                                                                resultJo,
                                                                                updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TOPIC)
                                                                        )
                                                                );

                                                            } else {
                                                                // Update repo for regular job
                                                                HelpFunc.updateRepoWithLogging(mongo, COLLECTION, updateJob, LOG);
                                                                LOG.debug("PK_TRANSFORM_STREAM_BACK_FLAG Not Found or equal to FALSE or Master Status Not FINISHED, so update as regular job.");
                                                            }

                                                        } else {
                                                            // No status changes, do nothing
                                                            LOG.debug(DFAPIMessage.logResponseMessage(1022, taskId));
                                                        }
                                                    } else {
                                                        // When statement not found, set status LOST with error message.
                                                        HelpFunc.updateRepoWithLogging(mongo, COLLECTION, updateJob.setStatus(ConstantApp.DF_STATUS.LOST.name()), LOG);
                                                        LOG.info(DFAPIMessage.logResponseMessage(9006,"TRANSFORM_STATUS_REFRESH_FOUND SPARK " + taskId + " LOST STATEMENT"));
                                                    }
                                                });

                                    } else {
                                        // When session not found, set status LOST with error message.
                                        HelpFunc.updateRepoWithLogging(mongo, COLLECTION, updateJob.setJobConfig(ConstantApp.PK_LIVY_SESSION_STATE, ConstantApp.DF_STATUS.LOST.name()), LOG);
                                        LOG.info(DFAPIMessage.logResponseMessage(9006,"TRANSFORM_STATUS_REFRESH_FOUND SPARK " + taskId + " LOST SESSION"));
                                    }
                                });
                    } else {
                        LOG.debug(DFAPIMessage.logResponseMessage(1022, taskId));
                    }
                }
            } else {
                LOG.error(DFAPIMessage.logResponseMessage(9002, "TRANSFORM_STATUS_REFRESH:" + result.cause()));
            }
        });
    }
}


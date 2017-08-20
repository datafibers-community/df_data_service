package com.datafibers.service;

import com.datafibers.model.DFLogPOPJ;
import com.datafibers.processor.SchemaRegisterProcessor;
import com.datafibers.util.DFAPIMessage;
import com.datafibers.util.MongoAdminClient;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.apache.log4j.Logger;
import com.datafibers.flinknext.DFRemoteStreamEnvironment;
import com.datafibers.model.DFJobPOPJ;
import com.datafibers.processor.FlinkTransformProcessor;
import com.datafibers.processor.KafkaConnectProcessor;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.hubrick.vertx.rest.MediaType;
import com.hubrick.vertx.rest.RestClient;
import com.hubrick.vertx.rest.RestClientOptions;
import com.hubrick.vertx.rest.RestClientRequest;
import com.hubrick.vertx.rest.converter.FormHttpMessageConverter;
import com.hubrick.vertx.rest.converter.HttpMessageConverter;
import com.hubrick.vertx.rest.converter.JacksonJsonHttpMessageConverter;
import com.hubrick.vertx.rest.converter.StringHttpMessageConverter;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.log4mongo.MongoDbAppender;
import org.log4mongo.MongoDbPatternLayout;
import scala.collection.immutable.Stream;

/**
 * DF Producer is used to route producer service to kafka connect rest or lunch locally.
 * The overall status is maintained in the its local database - mongodb
 */

public class DFDataProcessor extends AbstractVerticle {

    // Generic attributes
    public static String COLLECTION;
    public static String COLLECTION_INSTALLED;
    public static String COLLECTION_META;
    public static String COLLECTION_LOG;
    private static String repo_conn_str;
    private static String repo_hostname;
    private static String repo_port;
    private static String repo_db;
    private MongoClient mongo;
    private MongoAdminClient mongoDFInstalled;
    private RestClient rc;
    private RestClient rc_schema;
    private RestClient rc_flink;

    // Connects attributes
    private static Boolean kafka_connect_enabled;
    private static String kafka_connect_rest_host;
    private static Integer kafka_connect_rest_port;
    private static Boolean kafka_connect_import_start;

    // Transforms attributes
    public static Boolean transform_engine_flink_enabled;
    private static String flink_server_host;
    private static Integer flink_server_port;
    private static Integer flink_rest_server_port;
    public static DFRemoteStreamEnvironment env;

    // Kafka attributes
    private static String kafka_server_host;
    private static Integer kafka_server_port;
    public static String kafka_server_host_and_port;

    // Schema Registry attributes
    private static String schema_registry_host_and_port;
    private static Integer schema_registry_rest_port;

    private static final Logger LOG = Logger.getLogger(DFDataProcessor.class);

    @Override
    public void start(Future<Void> fut) {
        // Turn off
        // Logger.getLogger("io.vertx.core.impl.BlockedThreadChecker").setLevel(Level.OFF);

        /**
         * Get all application configurations
         **/
        // Get generic variables
        this.COLLECTION = config().getString("db.collection.name", "df_processor");
        this.COLLECTION_INSTALLED = config().getString("db.collection_installed.name", "df_installed");
        this.COLLECTION_META = config().getString("db.metadata.collection.name", "df_meta");
        this.COLLECTION_LOG = config().getString("db.log.collection.name", "df_log");
        this.repo_db = config().getString("db.name", "DEFAULT_DB");
        this.repo_conn_str = config().getString("repo.connection.string", "mongodb://localhost:27017");
        this.repo_hostname = repo_conn_str.replace("//", "").split(":")[1];
        this.repo_port = repo_conn_str.replace("//", "").split(":")[2];

        // Get Connects config
        this.kafka_connect_enabled = config().getBoolean("kafka.connect.enable", Boolean.TRUE);
        this.kafka_connect_rest_host = config().getString("kafka.connect.rest.host", "localhost");
        this.kafka_connect_rest_port = config().getInteger("kafka.connect.rest.port", 8083);
        this.kafka_connect_import_start = config().getBoolean("kafka.connect.import.start", Boolean.TRUE);

        // Check Transforms config
        this.transform_engine_flink_enabled = config().getBoolean("transform.engine.flink.enable", Boolean.TRUE);
        this.flink_server_host = config().getString("flink.servers.host", "localhost");
        this.flink_server_port = config().getInteger("flink.servers.port", 6123);
        this.flink_rest_server_port = config().getInteger("flink.rest.server.port", 8001); // Same to Flink Web Dashboard

        // Kafka config
        this.kafka_server_host = this.kafka_connect_rest_host;
        this.kafka_server_port = config().getInteger("kafka.server.port", 9092);
        this.kafka_server_host_and_port = this.kafka_server_host + ":" + this.kafka_server_port.toString();

        // Schema Registry
        this.schema_registry_rest_port = config().getInteger("kafka.schema.registry.rest.port", 8081);
        this.schema_registry_host_and_port = this.kafka_server_host + ":" + this.schema_registry_rest_port;

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

        // Common rest client properties
        final List<HttpMessageConverter> httpMessageConverters = ImmutableList.of(
                new FormHttpMessageConverter(),
                new StringHttpMessageConverter(),
                new JacksonJsonHttpMessageConverter(new ObjectMapper())
        );

        final RestClientOptions restClientOptions = new RestClientOptions()
                .setConnectTimeout(ConstantApp.REST_CLIENT_CONNECT_TIMEOUT)
                .setGlobalRequestTimeout(ConstantApp.REST_CLIENT_GLOBAL_REQUEST_TIMEOUT)
                .setKeepAlive(ConstantApp.REST_CLIENT_KEEP_LIVE)
                .setMaxPoolSize(ConstantApp.REST_CLIENT_MAX_POOL_SIZE);

        // Non-blocking Vertx Rest API Client to talk to Kafka Connect when needed
        if (this.kafka_connect_enabled) {

            this.rc = RestClient.create(vertx, restClientOptions.setDefaultHost(this.kafka_connect_rest_host)
                    .setDefaultPort(this.kafka_connect_rest_port), httpMessageConverters);

            this.rc_schema = RestClient.create(vertx, restClientOptions.setDefaultHost(this.kafka_connect_rest_host)
                    .setDefaultPort(this.schema_registry_rest_port), httpMessageConverters);

        }
        // Non-blocking Vertx Rest API Client to talk toFlink Rest when needed
        if (this.transform_engine_flink_enabled) {
            this.rc_flink = RestClient.create(vertx, restClientOptions.setDefaultHost(this.flink_server_host)
                    .setDefaultPort(this.flink_rest_server_port), httpMessageConverters);
        }

        // Flink stream environment for data transformation
        if(transform_engine_flink_enabled) {
            if (config().getBoolean("debug.mode", Boolean.FALSE)) {
                // TODO Add DF LocalExecutionEnvironment Support
//                env = StreamExecutionEnvironment.getExecutionEnvironment()
//                        .setParallelism(config().getInteger("flink.job.parallelism", 1));
            } else {
                String jarPath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                LOG.debug("Flink resource manager is started at " + this.flink_server_host + ":" + this.flink_server_port);
                LOG.debug("Distributed below Jar to above Flink resource manager.");
                LOG.debug(jarPath);
                env = new DFRemoteStreamEnvironment(this.flink_server_host, this.flink_server_port, jarPath)
                        .setParallelism(config().getInteger("flink.job.parallelism", 1));
//                env = StreamExecutionEnvironment.createRemoteEnvironment(this.flink_server_host,
//                        this.flink_server_port, jarPath)
//                        .setParallelism(config().getInteger("flink.job.parallelism", 1));
            }
        }

        // Import from remote server. It is blocking at this point.
        if (this.kafka_connect_enabled && this.kafka_connect_import_start) {
            importAllFromKafkaConnect();
            // importAllFromFlinkTransform();
            startMetadataSink();
        }

        // Regular update Kafka connects/Flink transform status
        vertx.setPeriodic(ConstantApp.REGULAR_REFRESH_STATUS_TO_REPO, id -> {
            if(this.kafka_connect_enabled) updateKafkaConnectorStatus();
            if(this.transform_engine_flink_enabled) updateFlinkJobStatus();
        });

        // Start Core application
        startWebApp((http) -> completeStartup(http, fut));
        LOG.info("********* DataFibers Services is started :) *********");
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
        router.put(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::updateOneConnects); // Kafka Connect Forward
        router.delete(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::deleteOneConnects); // Kafka Connect Forward

        // Transforms Rest API definition
        router.options(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::corsHandle);
        router.options(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::getAllTransforms);
        router.get(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::getOne);
        router.route(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL_WILD).handler(BodyHandler.create()); //TODO - can we delete this line?
        router.post(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL).handler(this::uploadFiles);
        router.route(ConstantApp.DF_TRANSFORMS_REST_URL_WILD).handler(BodyHandler.create());

        router.post(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::addOneTransforms); // Flink Forward
        router.put(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::updateOneTransforms); // Flink Forward
        router.delete(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::deleteOneTransforms); // Flink Forward

        // Schema Registry
        router.options(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_SCHEMA_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_SCHEMA_REST_URL).handler(this::getAllSchemas); // Schema Registry Forward
        router.get(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::getOneSchema); // Schema Registry Forward
        router.route(ConstantApp.DF_SCHEMA_REST_URL_WILD).handler(BodyHandler.create()); // Schema Registry Forward
        
        router.post(ConstantApp.DF_SCHEMA_REST_URL).handler(this::addOneSchema); // Schema Registry Forward
        router.put(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::updateOneSchema); // Schema Registry Forward
        router.delete(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::deleteOneConnects); // Schema Registry Forward

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
        this.rc.close();
        this.rc_schema.close();
        this.rc_flink.close();
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
        getAll(routingContext, "CONNECT");
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
        getAll(routingContext, "TRANSFORM");
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
        SchemaRegisterProcessor.forwardGetAllSchemas(vertx, routingContext, schema_registry_host_and_port);
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

        // TODO get all installed transforms as well
        final RestClientRequest postRestClientRequest =
                rc.get(
                        ConstantApp.KAFKA_CONNECT_PLUGIN_REST_URL, List.class, portRestResponse -> {
                            String search = portRestResponse.getBody().toString().replace("{", "\"").
                                    replace("}", "\"").replace("class=", "");


                            JsonObject query = new JsonObject().put("$and", new JsonArray()
                                    .add(new JsonObject().put("class",
                                            new JsonObject().put("$in", new JsonArray(search))))
                                    .add(new JsonObject().put("meta_type", "installed_connect"))
                            );

                            mongo.findWithOptions(COLLECTION_INSTALLED, query,
                                    HelpFunc.getMongoSortFindOption(routingContext), res -> {
                                if(res.result().toString().equalsIgnoreCase("[]")) {
                                    LOG.warn("RUN_BELOW_CMD_TO_SEE_FULL_METADATA");
                                    LOG.warn("mongoimport -c df_installed -d DEFAULT_DB --file df_installed.json");
                                }

                                if (res.succeeded()) {
                                    HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .end(Json.encodePrettily(
                                                    res.result().toString()=="[]"?
                                                            portRestResponse.getBody():res.result()
                                                    )
                                            );
                                }
                            });
                        });

        postRestClientRequest.exceptionHandler(exception -> {
            HelpFunc.responseCorsHandleAddOn(routingContext.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .end(DFAPIMessage.getResponseMessage(9006));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end();
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
                                .put("_id", 0)
                                .put("uid", new JsonObject().put("$concat",
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
        LOG.debug("SEARCH = " + searchCondition);
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
        SchemaRegisterProcessor.forwardGetOneSchema(vertx, routingContext, schema_registry_host_and_port);
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
     * Get one task status with task id specified
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
                        KafkaConnectProcessor.forwardGetAsGetOne(routingContext, rc, id);
                    }
                    if(dfJob.getConnectorCategory().equalsIgnoreCase("TRANSFORM")) {
                        // Find status from Flink API
                        FlinkTransformProcessor.forwardGetAsGetOne(routingContext, rc_flink, id,
                                dfJob.getFlinkIDFromJobConfig());
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
        // TODO Set default registry url for DF Generic connect for Avro
        // Set MongoId to _id, connect, cid in connectConfig
        String mongoId = new ObjectId().toString();
        dfJob.setConnectUid(mongoId).setId(mongoId).getConnectorConfig().put("cuid", mongoId);

        // Find and set default connector.class
        if(!dfJob.getConnectorConfig().containsKey(ConstantApp.PK_KAFKA_CONNECTOR_CLASS) ||
                dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_CONNECTOR_CLASS) == null) {
            String connectorClass = mongoDFInstalled.findConnectorClassName(dfJob.getConnectorType());
            if(connectorClass == null || connectorClass.isEmpty()) {
                LOG.info(DFAPIMessage.logResponseMessage(9024, mongoId + " - " + connectorClass));
            } else {
                dfJob.getConnectorConfig().put(ConstantApp.PK_KAFKA_CONNECTOR_CLASS, connectorClass);
                LOG.info(DFAPIMessage.logResponseMessage(1018, mongoId + " - " + connectorClass));
            }
        }

        // Start Kafka Connect REST API Forward only if Kafka is enabled and Connector type is Kafka Connect
        if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("CONNECT")) {
            // Auto fix "name" in Connect Config when mismatch with Connector Name
            if (!dfJob.getConnectUid().equalsIgnoreCase(dfJob.getConnectorConfig().get("name")) &&
                    dfJob.getConnectorConfig().get("name") != null) {
                dfJob.getConnectorConfig().put("name", dfJob.getConnectUid());
                LOG.info(DFAPIMessage.logResponseMessage(1004, dfJob.getId()));
            }
            KafkaConnectProcessor.forwardPOSTAsAddOne(routingContext, rc, mongo, COLLECTION, dfJob);
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
        final DFJobPOPJ dfJob = Json.decodeValue(
                HelpFunc.cleanJsonConfig(routingContext.getBodyAsString()), DFJobPOPJ.class);
        dfJob.setStatus(ConstantApp.DF_STATUS.RUNNING.name());
        String mongoId = new ObjectId().toString();
        dfJob.setConnectUid(mongoId).setId(mongoId).getConnectorConfig().put(ConstantApp.PK_TRANSFORM_CUID, mongoId);

        if (this.transform_engine_flink_enabled) {
            // Submit Flink UDF
            if(dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_UDF.name()) {
                FlinkTransformProcessor.runFlinkJar(dfJob.getUdfUpload(),
                        this.flink_server_host + ":" + this.flink_server_port);
            } else {
                String engine = "";
                if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_SQLA2A.name()) {
                    engine = "SQL_API";
                }

                if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_Script.name()) {
                    engine = "TABLE_API";
                }

                if (dfJob.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_UDF.name()) {
                    FlinkTransformProcessor.runFlinkJar(dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_JAR),
                            this.flink_server_host + this.flink_server_port);
                }

                // When schema name are not provided, got them from topic
                FlinkTransformProcessor.submitFlinkJobA2A(dfJob, vertx,
                        config().getInteger("flink.trans.client.timeout", 8000), env,
                        this.kafka_server_host_and_port,
                        this.schema_registry_host_and_port,
                        HelpFunc.coalesce(dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_CONSUMER_GROURP),
                                ConstantApp.DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK),
                        dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_INPUT),
                        dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_OUTPUT),
                        dfJob.getConnectorConfig().get(ConstantApp.PK_FLINK_TABLE_SINK_KEYS),
                        dfJob.getConnectorConfig().get(engine == "SQL_API" ?
                                ConstantApp.PK_TRANSFORM_SQL:ConstantApp.PK_TRANSFORM_SCRIPT),
                        HelpFunc.coalesce(dfJob.getConnectorConfig().get(ConstantApp.PK_SCHEMA_SUB_INPUT),
                                dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_INPUT)),
                        HelpFunc.coalesce(dfJob.getConnectorConfig().get(ConstantApp.PK_SCHEMA_SUB_OUTPUT),
                                dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_OUTPUT)),
                        mongo, COLLECTION, engine);
            }
        }

        mongo.insert(COLLECTION, dfJob.toJson(), r ->
                HelpFunc.responseCorsHandleAddOn(routingContext.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                        .end(Json.encodePrettily(dfJob)));
        LOG.info(DFAPIMessage.logResponseMessage(1000, dfJob.getId()));
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
        SchemaRegisterProcessor.forwardAddOneSchema(routingContext, rc_schema, schema_registry_host_and_port);
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
    private void updateOneConnects(RoutingContext routingContext) {
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
                    if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("CONNECT") &&
                            connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {
                       KafkaConnectProcessor.forwardPUTAsUpdateOne(routingContext, rc, mongo, COLLECTION, dfJob);
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
                            if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK") &&
                                    connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {

                                // TODO - if running then cancel and resubmit, else resubmit for flink UDF/JAR

                                //here update is to cancel exiting job and submit a new one
                                FlinkTransformProcessor.updateFlinkSQL(dfJob, vertx,
                                        config().getInteger("flink.trans.client.timeout", 8000), env,
                                        this.kafka_server_host_and_port,
                                        HelpFunc.coalesce(dfJob.getConnectorConfig()
                                                .get(ConstantApp.PK_KAFKA_CONSUMER_GROURP),
                                                ConstantApp.DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK),
                                        dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_INPUT),
                                        dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_OUTPUT),
                                        dfJob.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_SQL),
                                        mongo, COLLECTION, this.flink_server_host + ":" + this.flink_server_port,
                                        routingContext, this.schema_registry_host_and_port,
                                        HelpFunc.coalesce(dfJob.getConnectorConfig().get(ConstantApp.PK_SCHEMA_SUB_INPUT),
                                                dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_INPUT)),
                                        HelpFunc.coalesce(dfJob.getConnectorConfig().get(ConstantApp.PK_SCHEMA_SUB_OUTPUT),
                                                dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_OUTPUT)),
                                        dfJob.getConnectorConfig().get(ConstantApp.PK_FLINK_TABLE_SINK_KEYS),
                                        rc_flink);

                            } else { // Where there is no change detected
                                LOG.info(DFAPIMessage.logResponseMessage(1007, id));
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
                            }
                        } else {
                            LOG.error(DFAPIMessage.
                                    logResponseMessage(9014, id + " details - " + res.cause()));
                        }
                    });
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
        SchemaRegisterProcessor.forwardUpdateOneSchema(routingContext, rc_schema, schema_registry_host_and_port);
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
                        KafkaConnectProcessor.forwardDELETEAsDeleteOne(routingContext, rc, mongo, COLLECTION, dfJob);
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
                    if (ar.result() == null) {
                        routingContext.response()
                                .setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(DFAPIMessage.getResponseMessage(9001));
                        LOG.error(DFAPIMessage.logResponseMessage(9001, id));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());

                    String jobId = null;
                    if(dfJob.getJobConfig() != null &&
                            dfJob.getJobConfig().containsKey(ConstantApp.PK_FLINK_SUBMIT_JOB_ID)) {
                        jobId = dfJob.getJobConfig().get(ConstantApp.PK_FLINK_SUBMIT_JOB_ID);
                        if (this.transform_engine_flink_enabled &&
                                dfJob.getConnectorType().contains("FLINK") &&
                                dfJob.getStatus().equalsIgnoreCase("RUNNING")) {
                            FlinkTransformProcessor.cancelFlinkJob(jobId, mongo, COLLECTION, routingContext, rc_flink);
                            LOG.info(DFAPIMessage.logResponseMessage(1006, id));
                        } else {
                            mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                    remove -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                            .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                            .end(DFAPIMessage.getResponseMessage(1002)));
                            LOG.info(DFAPIMessage.logResponseMessage(1002, id + "- FLINK_JOB_NOT_RUNNING"));
                        }
                    } else {
                        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                remove -> HelpFunc.responseCorsHandleAddOn(routingContext.response())
                                        .setStatusCode(ConstantApp.STATUS_CODE_OK)
                                        .end(DFAPIMessage.getResponseMessage(1002)));
                        LOG.info(DFAPIMessage.logResponseMessage(1002, id + "- FLINK_JOB_ID_NULL"));
                    }
                }
            });
        }
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
        new MongoAdminClient(metaDBHost, Integer.parseInt(metaDBPort), metaDBName).createCollection(this.COLLECTION_META);

        String metaSinkConnect = new JSONObject().put("name", "metadata_sink_connect").put("config",
                new JSONObject().put("connector.class", "org.apache.kafka.connect.mongodb.MongodbSinkConnector")
                        .put("tasks.max", "2").put("host", metaDBHost)
                        .put("port", metaDBPort).put("bulk.size", "1")
                        .put("mongodb.database", config().getString("db.name", metaDBName))
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

            // Add the avro schema for metadata as well
            String dfMetaSchemaSubject = config().getString("kafka.topic.df.metadata", "df_meta") + "-value";
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
            LOG.debug(DFAPIMessage.logResponseMessage(1011, resStr));

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
                    } else if (resConnectTypeTmp.toUpperCase().contains("SOURCE")) {
                        resConnectType = ConstantApp.DF_CONNECT_TYPE.CONNECT_SOURCE_KAFKA_AvroFile.name();
                    } else if (resConnectTypeTmp.toUpperCase().contains("SINK")) {
                        resConnectType = ConstantApp.DF_CONNECT_TYPE.CONNECT_SINK_KAFKA_AvroFile.name();
                    } else {
                        resConnectType = resConnectTypeTmp;
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
                                                        LOG.info(DFAPIMessage.logResponseMessage(1001,
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

        String restURI = "http://" + this.kafka_connect_rest_host+ ":" + this.kafka_connect_rest_port +
                ConstantApp.KAFKA_CONNECT_REST_URL;

        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
            if (result.succeeded()) {
                for (JsonObject json : result.result()) {
                    String connectName = json.getString("connectUid");
                    String statusRepo = json.getString("status");
                    String taskId = json.getString("_id");
                    // Get task status
                    try {
                        String resStatus;
                        HttpResponse<JsonNode> resConnectorStatus =
                                Unirest.get(restURI + "/" + connectName + "/status")
                                        .header("accept", "application/json").asJson();
                        resStatus = resConnectorStatus.getStatus() == ConstantApp.STATUS_CODE_NOT_FOUND ?
                                ConstantApp.DF_STATUS.LOST.name():// Not find - Mark status as LOST
                                HelpFunc.getTaskStatusKafka(resConnectorStatus.getBody().getObject());

                        // Do change detection on status
                        if (statusRepo.compareToIgnoreCase(resStatus) != 0) { //status changes
                            DFJobPOPJ updateJob = new DFJobPOPJ(json);
                            updateJob.setStatus(resStatus);

                            mongo.updateCollection(COLLECTION, new JsonObject().put("_id", taskId),
                                    // The update syntax: {$set, the json object containing the fields to update}
                                    new JsonObject().put("$set", updateJob.toJson()), v -> {
                                        if (v.failed()) {
                                            LOG.error(DFAPIMessage.logResponseMessage(9003, taskId+ "cause:" + v.cause()));
                                        } else {
                                            LOG.info(DFAPIMessage.logResponseMessage(1019, taskId));
                                        }
                                    }
                            );
                        } else {
                            LOG.debug(DFAPIMessage.logResponseMessage(1020, taskId));
                        }
                    } catch (UnirestException ue) {
                        LOG.error(DFAPIMessage.logResponseMessage(9006, ue.getCause().getMessage()));
                    }
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
        // Add all Kafka connect
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*transform.*)") ;
        String restURI = "http://" + this.flink_server_host + ":" + this.flink_rest_server_port +
                ConstantApp.FLINK_REST_URL;

        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
            if (result.succeeded()) {
                for (JsonObject json : result.result()) {
                    String statusRepo = json.getString("status");
                    String taskId = json.getString("_id");
                    String jobId = ConstantApp.FLINK_DUMMY_JOB_ID;
                    String resStatus;
                    if(json.getValue("jobConfig") == null) {
                        resStatus = ConstantApp.DF_STATUS.LOST.name();
                    } else if (json.getJsonObject("jobConfig").containsKey(ConstantApp.PK_FLINK_SUBMIT_JOB_ID)) {
                        jobId = json.getJsonObject("jobConfig").getString(ConstantApp.PK_FLINK_SUBMIT_JOB_ID);
                    } else {
                        resStatus = ConstantApp.DF_STATUS.LOST.name();
                    }

                    // Get task status

                    try {
                        HttpResponse<JsonNode> resConnectorStatus =
                                Unirest.get(restURI + "/" + jobId).header("accept", "application/json").asJson();
                        resStatus = resConnectorStatus.getStatus() == ConstantApp.STATUS_CODE_NOT_FOUND ?
                            ConstantApp.DF_STATUS.LOST.name():// Not find - Mark status as LOST
                                HelpFunc.getTaskStatusFlink(resConnectorStatus.getBody().getObject());
                    } catch (UnirestException ue) {
                        // When jobId not found, set status LOST with error message.
                        LOG.info(DFAPIMessage.logResponseMessage(9006, "TRANSFORM_STATUS_REFRESH:" + "DELETE_LOST_TRANSFORMS"));
                        resStatus = ConstantApp.DF_STATUS.LOST.name();
                    }

                    // Do change detection on status
                    if (statusRepo.compareToIgnoreCase(resStatus) != 0) { //status changes
                        DFJobPOPJ updateJob = new DFJobPOPJ(json);
                        updateJob.setStatus(resStatus);

                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                                // The update syntax: {$set, the json object containing the fields to update}
                                new JsonObject().put("$set", updateJob.toJson()), v -> {
                                    if (v.failed()) {
                                        LOG.error(DFAPIMessage.logResponseMessage(9003, taskId + "cause:" + v.cause()));
                                    } else {
                                        LOG.info(DFAPIMessage.logResponseMessage(1021, taskId));
                                    }
                                }
                        );
                    } else {
                        LOG.debug(DFAPIMessage.logResponseMessage(1022, taskId));
                    }
                }
            } else {
                LOG.error(DFAPIMessage.logResponseMessage(9002, "TRANSFORM_STATUS_REFRESH:" +
                        result.cause()));
            }
        });
    }
}


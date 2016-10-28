package com.datafibers.service;

import com.datafibers.flinknext.Kafka09JsonTableSink;
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
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.sinks.CsvTableSink;
import org.apache.flink.api.table.sinks.TableSink;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DF Producer is used to route producer service to kafka connect rest or lunch locally
 * The overall status is maintained in the its local database - mongo
 */

public class DFDataProcessor extends AbstractVerticle {

    // Generic attributes
    public static String COLLECTION;
    private MongoClient mongo;
    private RestClient rc;

    // Connects attributes
    private static Boolean kafka_connect_enabled;
    private static String kafka_connect_rest_host;
    private static Integer kafka_connect_rest_port;
    private static Boolean kafka_connect_import_start;

    // Transforms attributes
    public static Boolean transform_engine_flink_enabled;
    private static String flink_server_host;
    private static Integer flink_server_port;
    private static String zookeeper_server_host;
    private static Integer zookeeper_server_port;
    private static String zookeeper_server_host_and_port;
    private static String kafka_server_host;
    private static Integer kafka_server_port;
    public static String kafka_server_host_and_port;
    public static StreamExecutionEnvironment env;

    private static final Logger LOG = LoggerFactory.getLogger(DFDataProcessor.class);

    @Override
    public void start(Future<Void> fut) {

        /**
         * Get all application configurations
         **/
        // Get generic variables
        this.COLLECTION = config().getString("db.collection.name", "df_processor");
        // Get Connects config
        this.kafka_connect_enabled = config().getBoolean("kafka.connect.enable", Boolean.TRUE);
        this.kafka_connect_rest_host = config().getString("kafka.connect.rest.host", "localhost");
        this.kafka_connect_rest_port = config().getInteger("kafka.connect.rest.port", 8083);
        this.kafka_connect_import_start = config().getBoolean("kafka.connect.import.start", Boolean.TRUE);
        // Check Transforms config
        this.transform_engine_flink_enabled = config().getBoolean("transform.engine.flink.enable", Boolean.TRUE);
        this.flink_server_host = config().getString("flink.servers.host", "localhost");
        this.flink_server_port = config().getInteger("flink.servers.port", 6123);
        this.zookeeper_server_host = config().getString("zookeeper.server.host", "localhost");
        this.zookeeper_server_port = config().getInteger("zookeeper.server.port", 2181);
        this.zookeeper_server_host_and_port = this.zookeeper_server_host + ":" + this.zookeeper_server_port.toString();
        this.kafka_server_host = this.kafka_connect_rest_host;
        this.kafka_server_port = config().getInteger("kafka.server.port", 9092);
        this.kafka_server_host_and_port = this.kafka_server_host + ":" + this.kafka_server_port.toString();

        /**
         * Create all application client
         **/
        // Mongo client for metadata repository
        mongo = MongoClient.createShared(vertx, config());
        // Non-blocking Vertx Rest API Client to talk to Kafka Connect when needed
        if (this.kafka_connect_enabled) {
            final ObjectMapper objectMapper = new ObjectMapper();
            final List<HttpMessageConverter> httpMessageConverters = ImmutableList.of(
                    new FormHttpMessageConverter(),
                    new StringHttpMessageConverter(),
                    new JacksonJsonHttpMessageConverter(objectMapper)
            );
            final RestClientOptions restClientOptions = new RestClientOptions()
                    .setConnectTimeout(ConstantApp.REST_CLIENT_CONNECT_TIMEOUT)
                    .setGlobalRequestTimeout(ConstantApp.REST_CLIENT_GLOBAL_REQUEST_TIMEOUT)
                    .setDefaultHost(this.kafka_connect_rest_host)
                    .setDefaultPort(this.kafka_connect_rest_port)
                    .setKeepAlive(ConstantApp.REST_CLIENT_KEEP_LIVE)
                    .setMaxPoolSize(ConstantApp.REST_CLIENT_MAX_POOL_SIZE);

            this.rc = RestClient.create(vertx, restClientOptions, httpMessageConverters);
        }
        // Flink stream environment for data transformation
        if(transform_engine_flink_enabled) {
            if (config().getBoolean("debug.mode", Boolean.FALSE)) {
                env = StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(config().getInteger("flink.job.parallelism", 1));
            } else {
                String jarPath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                LOG.debug("Distribute " + jarPath + " to Apache Flink cluster at " +
                        this.flink_server_host + ":" + this.flink_server_port);
                env = StreamExecutionEnvironment.createRemoteEnvironment(this.flink_server_host,
                        this.flink_server_port, jarPath)
                        .setParallelism(config().getInteger("flink.job.parallelism", 1));
            }
        }

        /**
         * Create all application client
         **/
        // Import from remote server. It is blocking at this point.
        if (this.kafka_connect_enabled && this.kafka_connect_import_start) {
           importAllFromKafkaConnect();
        }

        // Start Core application
        startWebApp((http) -> completeStartup(http, fut));

        // Regular update Kafka connects status
        if(this.kafka_connect_enabled) {
            long timerID = vertx.setPeriodic(ConstantApp.REGULAR_REFRESH_STATUS_TO_REPO, id -> {
                updateKafkaConnectorStatus();
            });
        }
    }

    private void startWebApp(Handler<AsyncResult<HttpServer>> next) {

        // Create a router object for rest.
        Router router = Router.router(vertx);

        // Job including both Connects and Transforms Rest API definition
        router.options(ConstantApp.DF_PROCESSOR_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_PROCESSOR_REST_URL).handler(this::getAllProcessor);

        // Connects Rest API definition
        router.options(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_CONNECTS_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_CONNECTS_REST_URL).handler(this::getAllConnects);
        router.get(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::getOne);
        router.route(ConstantApp.DF_CONNECTS_REST_URL_WILD).handler(BodyHandler.create());

        router.get(ConstantApp.DF_CONNECTS_INSTALLED_CONNECTS_REST_URL).handler(this::getAllInstalledConnects);
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
        this.rc.close();
    }

    /**
     * This is mainly to bypass security control for local API testing
     * @param routingContext
     */
    public void corsHandle(RoutingContext routingContext) {
        routingContext.response().putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
                .putHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type")
                .putHeader("Access-Control-Max-Age", "60").end();
    }

    /**
     * Generic getOne method for REST API End Point
     * @param routingContext
     */
    private void getOne(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(HelpFunc.errorMsg(20, "id is null in your request."));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(HelpFunc.errorMsg(21, "id cannot find in repository."));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                            .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                            .end(Json.encodePrettily(dfJob));
                } else {
                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                            .end(HelpFunc.errorMsg(22, "Search id in repository failed."));
                }
            });
        }
    }

   
    
    private void uploadFiles (RoutingContext routingContext) {
        Set<FileUpload> fileUploadSet = routingContext.fileUploads();

        Iterator<FileUpload> fileUploadIterator = fileUploadSet.iterator();
        String fileName = "";
        
        while (fileUploadIterator.hasNext()) {
            FileUpload fileUpload = fileUploadIterator.next();
            // String fileName = fileUpload.uploadedFileName();
            
			try {
				fileName = URLDecoder.decode(fileUpload.fileName(), "UTF-8");
            
				String jarPath = new HelpFunc().getCurrentJarRunnigFolder();
				String currentDir = config().getString("upload.dest", jarPath);
				String fileToBeCopied = currentDir + HelpFunc.generateUniqueFileName(fileName);
	            LOG.debug("UPLOADED FILE NAME (decode): " + currentDir + fileName + ", fileUpload.uploadedFileName(): " + fileUpload.uploadedFileName());
	            LOG.debug("===== fileToBeCopied: " + fileToBeCopied);
	            
	            vertx.fileSystem().copy(fileUpload.uploadedFileName(), fileToBeCopied, res -> {
	                if (res.succeeded()) {
	                    LOG.info("FILE COPIED GOOD ==> " + fileToBeCopied);
	                    
	                    routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_OK)
	                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
	                    .end(Json.encodePrettily(new JsonObject().put("uploaded_file_name", fileToBeCopied)));
	                } else {
	                    // Something went wrong
	                	routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
	                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
	                    .end(Json.encodePrettily(new JsonObject().put("uploaded_file_name", "Failed")));
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
        mongo.find(COLLECTION, new JsonObject().put("connectorCategory", connectorCategoryFilter), results -> {
            List<JsonObject> objects = results.result();
            List<DFJobPOPJ> jobs = objects.stream().map(DFJobPOPJ::new).collect(Collectors.toList());
            routingContext.response()
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(Json.encodePrettily(jobs));
        });
    }

    /**
     * This is for fetch both connects and transforms
     * @param routingContext
     */
    private void getAllProcessor(RoutingContext routingContext) {
        mongo.find(COLLECTION, new JsonObject(), results -> {
            List<JsonObject> objects = results.result();
            List<DFJobPOPJ> jobs = objects.stream().map(DFJobPOPJ::new).collect(Collectors.toList());
            routingContext.response()
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(Json.encodePrettily(jobs));
        });
    }

    private void getAllConnects(RoutingContext routingContext) {
        this.getAll(routingContext, "CONNECT");
    }

    private void getAllTransforms(RoutingContext routingContext) {
        this.getAll(routingContext, "TRANSFORM");
    }

    /**
     * Connects specific addOne End Point for Rest API
     * @param routingContext
     */
    private void addOneConnects(RoutingContext routingContext) {
        // Get request as object
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        // Set initial status for the job
        dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());
        LOG.info("received the body is:" + routingContext.getBodyAsString());
        LOG.info("repack for kafka is:" + dfJob.toKafkaConnectJson().toString());

        // Start Kafka Connect REST API Forward only if Kafka is enabled and Connector type is Kafka Connect
        if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("KAFKA")) {
            // Auto fix "name" in Connect Config when mismatch with Connector Name
            if (!dfJob.getConnector().equalsIgnoreCase(dfJob.getConnectorConfig().get("name")) &&
                    dfJob.getConnectorConfig().get("name") != null) {
                dfJob.getConnectorConfig().put("name", dfJob.getConnector());
            }
            KafkaConnectProcessor.forwardPOSTAsAddOne(routingContext, rc, mongo, COLLECTION, dfJob);
        } else {
            mongo.insert(COLLECTION, dfJob.toJson(), r -> routingContext
                    .response().setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(Json.encodePrettily(dfJob.setId(r.result()))));
        }
    }

    /**
     * Transforms specific addOne End Point for Rest API
     * @param routingContext
     */
    private void addOneTransforms(RoutingContext routingContext) {
        // Get request as object
        LOG.info("received the body is:" + routingContext.getBodyAsString());
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        // Set initial status for the job
        LOG.info("rebuilt object as:" + dfJob.toJson());
        dfJob.setStatus(ConstantApp.DF_STATUS.RUNNING.name());

        // Start Flink job Forward only if it is enabled and Connector type is FLINK_TRANS
        if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK_TRANS")) {
            // Submit flink sql
            FlinkTransformProcessor.submitFlinkSQL(dfJob, vertx,
                    config().getInteger("flink.trans.client.timeout", 8000), env,
                    this.zookeeper_server_host_and_port,
                    this.kafka_server_host_and_port,
                    HelpFunc.coalesce(dfJob.getConnectorConfig().get("group.id"),
                            ConstantApp.DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK),
                    dfJob.getConnectorConfig().get("column.name.list"),
                    dfJob.getConnectorConfig().get("column.schema.list"),
                    dfJob.getConnectorConfig().get("topic.for.query"),
                    dfJob.getConnectorConfig().get("topic.for.result"),
                    dfJob.getConnectorConfig().get("trans.sql"),
                    mongo, COLLECTION);
        } // Mongo updated is delayed inside of above block

        if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK_UDF")) {
            // Submit flink sql
            FlinkTransformProcessor.runFlinkJar(dfJob.getUdfUpload(),
                    this.flink_server_host + ":" + this.flink_server_port);
        }
            mongo.insert(COLLECTION, dfJob.toJson(), r -> routingContext
                .response().setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                .end(Json.encodePrettily(dfJob.setId(r.result()))));
    }

    /**
     * Connects specific updateOne End Point for Rest API
     * @param routingContext
     */
    private void updateOneConnects(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        LOG.debug("received the body is from updateOne:" + routingContext.getBodyAsString());
        String connectorConfigString = dfJob.mapToJsonString(dfJob.getConnectorConfig());
        JsonObject json = dfJob.toJson();

        if (id == null || json == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(HelpFunc.errorMsg(30, "id is null in your request."));
        } else {
            // Implement connectConfig change detection to decide if we need REST API forwarding
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id),
                    new JsonObject().put("connectorConfig", 1), res -> {
                if (res.succeeded()) {
                    String before_update_connectorConfigString = res.result().getString("connectorConfig");
                    // Detect changes in connectConfig
                    if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("KAFKA") &&
                            connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {
                       KafkaConnectProcessor.forwardPUTAsUpdateOne(routingContext, rc, mongo, COLLECTION, dfJob);
                    } else { // Where there is no change detected
                        LOG.info("connectorConfig has NO change. Update in local repository only.");
                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id), // Select a unique document
                                // The update syntax: {$set, the json object containing the fields to update}
                                new JsonObject().put("$set", dfJob.toJson()), v -> {
                                    if (v.failed()) {
                                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                                .end(HelpFunc.errorMsg(33, "updateOne to repository is failed."));
                                    } else {
                                        routingContext.response().putHeader(ConstantApp.CONTENT_TYPE,
                                                        ConstantApp.APPLICATION_JSON_CHARSET_UTF_8).end();
                                    }
                                }
                        );
                    }
                } else {
                    LOG.error("Mongo client response exception", res.cause());
                }
            });
        }
    }

    /**
     * Transforms specific updateOne End Point for Rest API
     * @param routingContext
     */
    private void updateOneTransforms(RoutingContext routingContext) {
        final String id = routingContext.request().getParam("id");
        final DFJobPOPJ dfJob = Json.decodeValue(routingContext.getBodyAsString(), DFJobPOPJ.class);
        LOG.debug("received the body is from updateOne:" + routingContext.getBodyAsString());
        String connectorConfigString = dfJob.mapToJsonString(dfJob.getConnectorConfig());
        JsonObject json = dfJob.toJson();

        if (id == null || json == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(HelpFunc.errorMsg(130, "id is null in your request."));
        } else {
            // Implement connectConfig change detection to decide if we need REST API forwarding
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id),
                    new JsonObject().put("connectorConfig", 1), res -> {
                        if (res.succeeded()) {
                            String before_update_connectorConfigString = res.result().getString("connectorConfig");
                            // Detect changes in connectConfig
                            if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK") &&
                                    connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {
                                //here update is to cancel exiting job and submit a new one
                                FlinkTransformProcessor.updateFlinkSQL(dfJob, vertx,
                                        config().getInteger("flink.trans.client.timeout", 8000), env,
                                        this.zookeeper_server_host_and_port,
                                        this.kafka_server_host_and_port,
                                        HelpFunc.coalesce(dfJob.getConnectorConfig().get("group.id"),
                                                ConstantApp.DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK),
                                        dfJob.getConnectorConfig().get("column.name.list"),
                                        dfJob.getConnectorConfig().get("column.schema.list"),
                                        dfJob.getConnectorConfig().get("topic.for.query"),
                                        dfJob.getConnectorConfig().get("topic.for.result"),
                                        dfJob.getConnectorConfig().get("trans.sql"),
                                        mongo, COLLECTION, this.flink_server_host + ":" + this.flink_server_port,
                                        routingContext);

                            } else { // Where there is no change detected
                                LOG.info("connectorConfig has NO change. Update in local repository only.");
                                mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id), // Select a unique document
                                        // The update syntax: {$set, the json object containing the fields to update}
                                        new JsonObject().put("$set", dfJob.toJson()), v -> {
                                            if (v.failed()) {
                                                routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                                        .end(HelpFunc.errorMsg(133, "updateOne to repository is failed."));
                                            } else {
                                                routingContext.response().putHeader(ConstantApp.CONTENT_TYPE,
                                                        ConstantApp.APPLICATION_JSON_CHARSET_UTF_8).end();
                                            }
                                        }
                                );
                            }
                        } else {
                            LOG.error("Mongo client response exception", res.cause());
                        }
                    });
        }
    }

    /**
     * Connects specific deleteOne End Point for Rest API
     * @param routingContext
     */
    private void deleteOneConnects(RoutingContext routingContext) {
        String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(HelpFunc.errorMsg(40, "id is null in your request."));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(HelpFunc.errorMsg(41, "id cannot find in repository."));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    System.out.println("DELETE OBJ" + dfJob.toJson());
                    if (this.kafka_connect_enabled && dfJob.getConnectorType().contains("KAFKA")) {
                        KafkaConnectProcessor.forwardDELETEAsDeleteOne(routingContext, rc, mongo, COLLECTION, dfJob);
                    } else {
                        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                remove -> routingContext.response().end(id + " is deleted from repository."));
                    }
                }
            });
        }
    }

    /**
     * Transforms specific deleteOne End Point for Rest API
     * @param routingContext
     */
    private void deleteOneTransforms(RoutingContext routingContext) {
        String id = routingContext.request().getParam("id");
        if (id == null) {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(HelpFunc.errorMsg(140, "id is null in your request."));
        } else {
            mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
                if (ar.succeeded()) {
                    if (ar.result() == null) {
                        routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
                                .end(HelpFunc.errorMsg(141, "id cannot find in repository."));
                        return;
                    }
                    DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
                    if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK")) {
                        FlinkTransformProcessor.cancelFlinkSQL(this.flink_server_host + ":" + this.flink_server_port,
                                dfJob.getJobConfig().get("flink.submit.job.id"),
                                mongo, COLLECTION, routingContext);
                    } else {
                        mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
                                remove -> routingContext.response().end(id + " is deleted from repository."));
                    }
                }
            });
        }
    }

    /**
     * Get initial method to import all available|paused|running connectors from Kafka connect.
     */
    private void importAllFromKafkaConnect() {
        LOG.info("Starting initial import data from Kafka Connect REST Server.");
        String restURI = "http://" + this.kafka_connect_rest_host+ ":" + this.kafka_connect_rest_port +
                ConstantApp.KAFKA_CONNECT_REST_URL;
        try {
            HttpResponse<String> res = Unirest.get(restURI)
                    .header("accept", "application/json")
                    .asString();
            String resStr = res.getBody();
            if (resStr.compareToIgnoreCase("[]") != 0) { //Has active connectors
                for (String connectName: resStr.substring(2,resStr.length()-2).split("\",\"")) {
                    // Get connector config
                    HttpResponse<JsonNode> resConnector = Unirest.get(restURI + "/" + connectName + "/config")
                            .header("accept", "application/json").asJson();
                    JsonNode resConfig = resConnector.getBody();
                    String resConnectTypeTmp = resConfig.getObject().getString("connector.class");
                    String resConnectType;
                    if (resConnectTypeTmp.toUpperCase().contains("SOURCE")) {
                        resConnectType = ConstantApp.DF_CONNECT_TYPE.KAFKA_SOURCE.name();
                    } else if (resConnectTypeTmp.toUpperCase().contains("SINK")) {
                        resConnectType = ConstantApp.DF_CONNECT_TYPE.KAFKA_SINK.name();
                    } else {
                        resConnectType = ConstantApp.DF_CONNECT_TYPE.NONE.name();
                    }
                    // Get task status
                    HttpResponse<JsonNode> resConnectorStatus = Unirest.get(restURI + "/" + connectName + "/status")
                            .header("accept", "application/json").asJson();
                    String resStatus = resConnectorStatus.getBody().getObject().getJSONObject("connector").getString("state");

                    mongo.count(COLLECTION, new JsonObject().put("connector", connectName), count -> {
                        if (count.succeeded()) {
                            if (count.result() == 0) {
                                // No jobs found, then insert json data
                                DFJobPOPJ insertJob = new DFJobPOPJ (
                                        new JsonObject().put("name", "imported " + connectName)
                                                .put("taskId", "0")
                                                .put("connector", connectName)
                                                .put("connectorType", resConnectType)
                                                .put("connectorCategory", "CONNECT")
                                                .put("status", resStatus)
                                                .put("jobConfig", new JsonObject().put("comments", "This is imported from Kafka Connect.").toString())
                                                .put("connectorConfig", resConfig.getObject().toString())
                                );
                                mongo.insert(COLLECTION, insertJob.toJson(), ar -> {
                                    if (ar.failed()) {
                                        LOG.error("IMPORT Status - failed", ar);
                                    } else {
                                        LOG.debug("IMPORT Status - successfully", ar);
                                    }
                                });
                            } else { // Update the connectConfig portion from Kafka import
                                mongo.findOne(COLLECTION, new JsonObject().put("connector", connectName), null, findidRes -> {
                                    if (findidRes.succeeded()) {
                                        DFJobPOPJ updateJob = new DFJobPOPJ(findidRes.result());
                                        try {
                                            updateJob.setStatus(resStatus).setConnectorConfig(
                                                    new ObjectMapper().readValue(resConfig.getObject().toString(),
                                                            new TypeReference<HashMap<String, String>>(){}));

                                        } catch (IOException ioe) {
                                            LOG.error("IMPORT Status - Read Connector Config as Map failed", ioe.getCause());
                                        }

                                        mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                                                // The update syntax: {$set, the json object containing the fields to update}
                                                new JsonObject().put("$set", updateJob.toJson()), v -> {
                                                    if (v.failed()) {
                                                        LOG.error("IMPORT Status - Update Connector Config as Map failed",
                                                                v.cause());
                                                    } else {
                                                        LOG.debug("IMPORT Status - Update Connector Config as Map Successfully");
                                                    }
                                                }
                                        );

                                    } else {
                                        LOG.error("IMPORT-UPDATE failed", findidRes.cause());
                                    }
                                });
                            }
                        } else {
                            // report the error
                            LOG.error("count failed",count.cause());
                        }
                    });
                }
            } else {
                LOG.info("There is no active connects in Kafka Connect REST Server.");
            }
        } catch (UnirestException ue) {
            LOG.error("Importing from Kafka Connect Server exception", ue);
        }
        LOG.info("Completed initial import data from Kafka Connect REST Server.");
    }

    /**
     * Keep refreshing the active Kafka connector status against remote Kafka REST Server
     */
    private void updateKafkaConnectorStatus() {
        // Loop existing KAFKA connectors in repository and fetch their latest status from Kafka Server
        LOG.info("Refreshing Connects status from Kafka Connect REST Server - Start.");
        List<String> list = new ArrayList<String>();
        list.add(ConstantApp.DF_CONNECT_TYPE.KAFKA_SINK.name());
        list.add(ConstantApp.DF_CONNECT_TYPE.KAFKA_SOURCE.name());

        String restURI = "http://" + this.kafka_connect_rest_host+ ":" + this.kafka_connect_rest_port +
                ConstantApp.KAFKA_CONNECT_REST_URL;
        // Container reused for keeping refreshing list of active Kafka jobs
        ArrayList<String> activeKafkaConnector = new ArrayList<String>();
        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
                    if (result.succeeded()) {
                        for (JsonObject json : result.result()) {
                            String connectName = json.getString("connector");
                            String statusRepo = json.getString("status");
                            // Get task status
                            try {
                                String resStatus;
                                HttpResponse<JsonNode> resConnectorStatus =
                                        Unirest.get(restURI + "/" + connectName + "/status")
                                                .header("accept", "application/json").asJson();
                                if(resConnectorStatus.getStatus() == 404) {
                                    // Not find - Mark status as LOST
                                    resStatus = ConstantApp.DF_STATUS.LOST.name();
                                } else {
                                    resStatus = resConnectorStatus.getBody().getObject()
                                            .getJSONObject("connector").getString("state");
                                }

                                // Do change detection on status
                                if (statusRepo.compareToIgnoreCase(resStatus) != 0) { //status changes
                                    DFJobPOPJ updateJob = new DFJobPOPJ(json);
                                    updateJob.setStatus(resStatus);

                                    mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                                            // The update syntax: {$set, the json object containing the fields to update}
                                            new JsonObject().put("$set", updateJob.toJson()), v -> {
                                                if (v.failed()) {
                                                    LOG.error("Update Status - Update status failed", v.cause());
                                                } else {
                                                    LOG.debug("Update Status - Update status Successfully");
                                                }
                                            }
                                    );
                                } else {
                                    LOG.info("Refreshing Connects status from Kafka Connect REST Server - No Changes.");
                                }
                            } catch (UnirestException ue) {
                                LOG.error("Refreshing status REST client exception", ue.getCause());
                            }
                        }
                    } else {
                        LOG.error("Refreshing status Mongo client find active connectors exception", result.cause());
                    }
        });
        LOG.info("Refreshing Connects status from Kafka Connect REST Server - Complete.");
    }

    /**
     * Keep refreshing the active Flink transforms/job status against remote Flink REST Server since v1.2
     */
    private void updateFlinkJobStatus() {
        // TODO add implementation against Flink new REST API
    }

    /**
     * List all installed Connects
     * @param routingContext
     */
    private void getAllInstalledConnects(RoutingContext routingContext) {
        // TODO get all installed transforms as well
        final RestClientRequest postRestClientRequest =
                rc.get(
                        ConstantApp.KAFKA_CONNECT_PLUGIN_REST_URL,
                        List.class, portRestResponse -> {
                            routingContext
                                    .response().setStatusCode(ConstantApp.STATUS_CODE_OK)
                                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                                    .end(Json.encodePrettily(portRestResponse.getBody()));
                        });
        postRestClientRequest.exceptionHandler(exception -> {
            routingContext.response().setStatusCode(ConstantApp.STATUS_CODE_CONFLICT)
                    .putHeader(ConstantApp.CONTENT_TYPE, ConstantApp.APPLICATION_JSON_CHARSET_UTF_8)
                    .end(HelpFunc.errorMsg(31, "POST Request exception - " + exception.toString()));
        });

        postRestClientRequest.setContentType(MediaType.APPLICATION_JSON);
        postRestClientRequest.setAcceptHeader(Arrays.asList(MediaType.APPLICATION_JSON));
        postRestClientRequest.end();
    }
}


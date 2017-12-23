package com.datafibers.processor;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.DFAPIMessage;
import com.datafibers.util.HelpFunc;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import java.io.File;
import java.util.List;

/**
 * This is the utility class to communicate with Spark through Apache Livy Rest Service
 */

public class ProcessorStreamBack {

    private static final Logger LOG = Logger.getLogger(ProcessorStreamBack.class);

    /**
     * Add a stream back task (AVRO file source connect) directly to df rest service.
     * @param wc_streamback
     * @param mongo
     * @param COLLECTION
     * @param schema_registry_rest_port
     * @param schema_registry_rest_host
     * @param df_rest_port
     * @param df_rest_host
     * @param createNewSchema
     * @param subject
     * @param schemaFields
     * @param streamBackMaster
     * @param streamBackWorker
     * @param LOG
     */
    public static void addStreamBackTask(WebClient wc_streamback, MongoClient mongo, String COLLECTION,
                                         int schema_registry_rest_port, String schema_registry_rest_host,
                                         int df_rest_port, String df_rest_host,
                                         Boolean createNewSchema,
                                         String subject, String schemaFields,
                                         DFJobPOPJ streamBackMaster, DFJobPOPJ streamBackWorker, Logger LOG) {
        if(createNewSchema) {
            LOG.debug("create schema ...");
            wc_streamback.post(schema_registry_rest_port, schema_registry_rest_host,
                    ConstantApp.SR_REST_URL_SUBJECTS + "/" + subject + ConstantApp.SR_REST_URL_VERSIONS)
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
                    .sendJsonObject( new JsonObject().put(ConstantApp.SCHEMA_REGISTRY_KEY_SCHEMA, schemaFields), schemar -> {
                        if (schemar.succeeded()) {
                            // Use local client to create a stream back worker source file task from datafibers rest service.
                            // Update stream back master status to failed when submission failed
                            // Then, check if the taskId already in repo, if yes update instead of create
                            LOG.debug("Stream Back Schema is created with version " + schemar.result().bodyAsString());
                            LOG.debug("The schema fields are " + schemaFields);

                            mongo.findOne(COLLECTION, new JsonObject().put("_id", streamBackWorker.getId()),
                                    new JsonObject().put("connectorConfig", 1), res -> {
                                        if (res.succeeded() && res.result() != null) {
                                            // found in repo, update
                                            LOG.debug("found stream back task, update it");
                                            wc_streamback
                                                    .put(df_rest_port, df_rest_host, ConstantApp.DF_CONNECTS_REST_URL + "/" + streamBackWorker.getId())
                                                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                                    .sendJsonObject(streamBackWorker.toPostJson(),
                                                            war -> {
                                                                LOG.debug("rest put result = " + war.result().bodyAsString());
                                                                streamBackMaster.getConnectorConfig().put(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
                                                                        war.succeeded() ? ConstantApp.DF_STATUS.RUNNING.name() : ConstantApp.DF_STATUS.FAILED.name());
                                                                HelpFunc.updateRepoWithLogging(mongo, COLLECTION, streamBackMaster, LOG);
                                                            }
                                                    );
                                        } else {
                                            // create a new task
                                            LOG.debug("not found stram back task, create it");
                                            wc_streamback
                                                    .post(df_rest_port, df_rest_host, ConstantApp.DF_CONNECTS_REST_URL)
                                                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                                    .sendJsonObject(streamBackWorker.toPostJson(),
                                                            war -> {
                                                                LOG.debug("rest post result = " + war.result().bodyAsString());
                                                                streamBackMaster.getConnectorConfig().put(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
                                                                        war.succeeded() ? ConstantApp.DF_STATUS.RUNNING.name() : ConstantApp.DF_STATUS.FAILED.name());
                                                                LOG.debug("response from create stream back tsdk = " + war.result().bodyAsString());
                                                                HelpFunc.updateRepoWithLogging(mongo, COLLECTION, streamBackMaster, LOG);
                                                            }
                                                    );
                                        }
                                    });
                        } else {
                            LOG.error("Schema creation failed for streaming back worker");
                        }
                    });
        } else {
            LOG.debug("use old schema ...");
            mongo.findOne(COLLECTION, new JsonObject().put("_id", streamBackWorker.getId()),
                    new JsonObject().put("connectorConfig", 1), res -> {
                        LOG.debug("res.succeeded() = " + res.succeeded());
                        LOG.debug("res.result() = " + res.result());
                        if (res.succeeded() && res.result() != null) {
                            LOG.debug("Use old schema and update the stream back task");
                            // found in repo, update
                            wc_streamback
                                    .put(df_rest_port, df_rest_host, ConstantApp.DF_CONNECTS_REST_URL + "/" + streamBackWorker.getId())
                                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                    .sendJsonObject(streamBackWorker.toPostJson(),
                                            war -> {
                                                LOG.debug("rest put result = " + war.result().bodyAsString());
                                                streamBackMaster.getConnectorConfig().put(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
                                                        war.succeeded() ? ConstantApp.DF_STATUS.RUNNING.name() : ConstantApp.DF_STATUS.FAILED.name());
                                                HelpFunc.updateRepoWithLogging(mongo, COLLECTION, streamBackMaster, LOG);
                                            }
                                    );
                        } else {
                            // create a new task
                            LOG.debug("use old schema to create a new stream back task streamBackWorker.toJson() = " + streamBackWorker.toJson());
                            wc_streamback
                                    .post(df_rest_port, df_rest_host, ConstantApp.DF_CONNECTS_REST_URL)
                                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                    .sendJsonObject(streamBackWorker.toPostJson(),
                                            war -> {
                                                LOG.debug("rest post result = " + war.result().bodyAsString());
                                                streamBackMaster.getConnectorConfig().put(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
                                                        war.succeeded() ? ConstantApp.DF_STATUS.RUNNING.name() : ConstantApp.DF_STATUS.FAILED.name());
                                                HelpFunc.updateRepoWithLogging(mongo, COLLECTION, streamBackMaster, LOG);
                                            }
                                    );
                        }
                    });
        }
    }

    /**
     * enableStreamBack is a generic function to submit stream back task if requested. It also check the status of the
     * stream back worker and update proper status in the stream back master (the transform which triggered the stream
     * back task. Another important function is whether to overwrite the status in updateJob. The stream master's status
     * has to be overwritten when the stream back working is in progressing in order to keep master running.
     *
     * @param wc_streamback vertx web client for rest
     * @param updateJob dfpopj job about to update to repo
     * @param mongo mongodb client
     * @param COLLECTION mongo collection to keep the task
     * @param schema_registry_rest_host Schema registry rest hostname
     * @param schema_registry_rest_port Schema registry rest port number
     * @param df_rest_port datafibers rest hostname
     * @param df_rest_host datafibers rest port number
     * @param schemaFields if we create a new topic, this is list of fields and types
     */
    public static void enableStreamBack(WebClient wc_streamback, DFJobPOPJ updateJob,
                                        MongoClient mongo, String COLLECTION,
                                        int schema_registry_rest_port, String schema_registry_rest_host,
                                        int df_rest_port, String df_rest_host, String schemaFields) {
        LOG.debug("Enter stream back call");
        // When stream back is needed, we either monitoring status OR kicking off the job
        // If state is available, the stream back job is already started. Or else, start it.
        String streamBackTaskId = updateJob.getId() + "sw";
        String streamBackFilePath = updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_PATH);
        String streamBackTopic = updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TOPIC);

        if (!(updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE).equalsIgnoreCase("") ||
                updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE)
                        .equalsIgnoreCase(ConstantApp.DF_STATUS.UNASSIGNED.name()))) {

            String streamBackTaskState = updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE);
            LOG.debug("Found stream back state = " + streamBackTaskState);

            if (streamBackTaskState.equalsIgnoreCase(ConstantApp.DF_STATUS.FINISHED.name())) {
                // This is never reach. Since status = FINISH will not by pass regular status checker

            } else if (streamBackTaskState.equalsIgnoreCase(ConstantApp.DF_STATUS.FAILED.name())) {
                // TODO Overwrite transform task state as FAILED, but we keep the failed stream worker task for now
                HelpFunc.updateRepoWithLogging(
                        mongo, COLLECTION,
                        updateJob.setStatus(ConstantApp.DF_STATUS.FAILED.name()),
                        LOG
                );
            } else {
                // Check stream back worker status in repo is failed or lost. If yes, set Master PK_TRANSFORM_STREAM_BACK_TASK_STATE and master task status as failed
                // Or else, check stream back folder to see if all files are processed if yes set PK_TRANSFORM_STREAM_BACK_TASK_STATE and master task status as finished
                // Or else, set PK_TRANSFORM_STREAM_BACK_TASK_STATE and master task status as streaming and PK_TRANSFORM_STREAM_BACK_TASK_STATE to running
                mongo.findOne(COLLECTION, new JsonObject().put("_id", streamBackTaskId),
                        new JsonObject().put("status", 1), res -> {
                            if (res.succeeded()) {
                                String workerStatus = res.result().getString("status");
                                LOG.debug("Stream back worker status = " + workerStatus);

                                if (workerStatus.equalsIgnoreCase(ConstantApp.DF_STATUS.FAILED.name()) ||
                                        workerStatus.equalsIgnoreCase(ConstantApp.DF_STATUS.LOST.name())) {
                                    updateJob
                                            .setStatus(ConstantApp.DF_STATUS.FAILED.name())
                                            .setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE, ConstantApp.DF_STATUS.FAILED.name());
                                } else {
                                    // Check stream back path to see if all files are processed
                                    LOG.debug("Checking streaming process result ...");
                                    File dir = new File(streamBackFilePath);
                                    int jsonFileNumber = ((List<File>) FileUtils.listFiles(dir, new String[]{"json"}, false)).size();
                                    int processedFileNumber = ((List<File>) FileUtils.listFiles(dir, new String[]{"processed"}, false)).size();
                                    int processingFileNumber = ((List<File>) FileUtils.listFiles(dir, new String[]{"processing"}, false)).size();

                                    LOG.debug("jsonFileNumber = " + jsonFileNumber + " processedFileNumber = " + processedFileNumber);
                                    if (jsonFileNumber == 0 && processedFileNumber >= 0 && processingFileNumber == 0) {
                                        // When processedFileNumber = 0 means batch job does not produce any result
                                        // When processedFileNumber > 0 means stream back is finished
                                        updateJob
                                                .setStatus(ConstantApp.DF_STATUS.FINISHED.name())
                                                .setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE, ConstantApp.DF_STATUS.FINISHED.name());

                                        // Delete the stream back worker task. If not implement, the task will keep not impact
                                        // When stream worker is finished, we do not need to overwrite the master status anymore.
                                        wc_streamback.delete(df_rest_port, df_rest_host,
                                                ConstantApp.DF_CONNECTS_REST_URL + "/" + streamBackTaskId)
                                                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                                                .send(ar -> {
                                                    if (ar.succeeded()) {
                                                        LOG.info(DFAPIMessage.logResponseMessage(1031, updateJob.getId()));
                                                    } else {
                                                        LOG.error(DFAPIMessage.logResponseMessage(9043,
                                                                updateJob.getId() + " has error " + ar.result().bodyAsString()));
                                                    }
                                        });

                                    } else { // in progress when csvFileNumber > 0
                                        updateJob
                                                .setStatus(ConstantApp.DF_STATUS.STREAMING.name())
                                                .setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE, ConstantApp.DF_STATUS.RUNNING.name());
                                    }
                                }

                                HelpFunc.updateRepoWithLogging(mongo, COLLECTION, updateJob, LOG);

                            } else {
                                LOG.error("Stream Back Task with Id = " + streamBackTaskId + " Not Found.");
                            }
                        }
                );
            }
        } else {
            /*
            When start a stream back needed.
            1. Overwrite status to STREAMING
            2. Create a jobId and start a stream back source job (with new or existing schema)
            3. Set PK_TRANSFORM_STREAM_BACK_TASK_ID and PK_TRANSFORM_STREAM_BACK_TASK_STATE = RUNNING/FAILED

            Note: We have to start the stream back job here when we have batch sql result, because, we need to
            create new schema based on the result types when needed.
            */

            // First, set current job state and stream back state
            LOG.debug("Will create a new stream back job");
            updateJob
                    .setStatus(ConstantApp.DF_STATUS.STREAMING.name())
                    .setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE, ConstantApp.DF_STATUS.UNASSIGNED.name())
                    .setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_ID, streamBackTaskId);

            DFJobPOPJ streamBackTask = new DFJobPOPJ()
                    .setId(streamBackTaskId)
                    .setTaskSeq("1")
                    .setName("stream_worker")
                    .setDescription("stream_worker")
                    .setConnectorType(ConstantApp.DF_CONNECT_TYPE.CONNECT_SOURCE_KAFKA_AvroFile.name())
                    .setConnectorCategory("source")
                    .setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_TASK, "1")
                    .setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_SRURI, "http://" + schema_registry_rest_host + ":" + schema_registry_rest_port)
                    .setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_OW, "true")
                    .setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_LOC, streamBackFilePath)
                    .setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_GLOB, "*.json")
                    .setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_TOPIC, streamBackTopic)
                    ; // Populate file connect task config

            LOG.debug("Stream Back Connect POPJ = " + streamBackTask.toJson());

            // Create the topic if it asks to create a new topic from ui
            addStreamBackTask(
                    wc_streamback, mongo, COLLECTION,
                    schema_registry_rest_port, schema_registry_rest_host,
                    df_rest_port, df_rest_host,
                    Boolean.parseBoolean(updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TOPIC_CREATION)),
                    streamBackTopic, schemaFields,
                    updateJob, streamBackTask, LOG
            );
        }

    }
}

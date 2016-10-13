package com.datafibers.model;

import com.datafibers.util.ConstantApp;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.HashMap;

/**
 * Meta Objects Response for REST API
 */
public class DFJobPOPJ {

    private String id; // id as pk, which is also used as job id
    private String taskId; // Identify each task in a job
    private String name; // Name of the job
    private String connector; // Name of the connector used. This will maps to Kafka Connect name attribute.
    private ConstantApp.DF_CONNECT_TYPE connectorType; // Identify proper connector type from enum
    private String connectorCategory;
    private String description; // Description about job and connector
    private String status; // Job/Connector status

    /*
     * The reason we keep them as HashMap is because we do not want to SerDe all field (in that case, we have to define all attribute in
     * configuration file we may use. By using hashmap, we have such flexibility to have one attribute packs all possible configurations.
     * As result, this two below fields are saved as string instead object in mongo and lose native good format (not true nest json). But,
     * we keep flexibility to do any processing through hashmap. And, we can expect any configuration in the config file without changing
     * our code.
     */
    private HashMap<String, String> jobConfig; //configuration or metadata for the job
    private HashMap<String, String> connectorConfig; //configuration for the connector used. This will maps to Kafka Connect config attribute

    public DFJobPOPJ(String task_id, String name, String connector, String connector_type, String description,
                     String status, HashMap<String, String> job_config, HashMap<String, String> connector_config) {
        this.taskId = task_id;
        this.name = name;
        this.connector = connector;
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.valueOf(connector_type);
        this.connectorCategory = findConnectorCategory(connector_type);
        this.description = description;
        this.status = status;
        this.id = "";
        this.jobConfig = job_config;
        this.connectorConfig = connector_config;
    }

    public DFJobPOPJ(String name, String connector, String status,
                     HashMap<String, String> job_config, HashMap<String, String> connector_config) {
        this.taskId = "0";
        this.name = name;
        this.connector = connector;
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.NONE;
        this.connectorCategory = findConnectorCategory("");
        this.description = "";
        this.status = status;
        this.id = "";
        this.jobConfig = job_config;
        this.connectorConfig = connector_config;
    }

    public DFJobPOPJ(String name, String connector, String status) {
        this.name = name;
        this.connector = connector;
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.NONE;
        this.connectorCategory = findConnectorCategory("");
        this.description = "";
        this.status = status;
        this.id = "";
        this.taskId = "0";
        this.jobConfig = null;
        this.connectorConfig = null;
    }

    public DFJobPOPJ(JsonObject json) {
        this.taskId = json.getString("taskId");
        this.name = json.getString("name");
        this.connector = json.getString("connector");
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.valueOf(json.getString("connectorType"));
        this.connectorCategory = findConnectorCategory(json.getString("connectorType"));
        this.description = json.getString("description");
        this.status = json.getString("status");
        this.id = json.getString("_id");

        try {

            String jobConfig = json.getString("jobConfig");
            if (jobConfig == null) {
                this.jobConfig = null;
            } else {
                this.jobConfig = new ObjectMapper().readValue(jobConfig, new TypeReference<HashMap<String, String>>() {
                });
            }

            String connectorConfig = json.getString("connectorConfig");
            if (jobConfig == null) {
                this.connectorConfig = null;
            } else {
                this.connectorConfig = new ObjectMapper().readValue(connectorConfig,
                        new TypeReference<HashMap<String, String>>() {
                        });
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public DFJobPOPJ() {
        this.id = "";
    }

    public DFJobPOPJ(String id, String name, String connector, String status) {
        this.id = id;
        this.name = name;
        this.connector = connector;
        this.status = status;
    }

    public JsonObject toJson() {

        JsonObject json = new JsonObject()
                .put("name", name)
                .put("taskId", taskId)
                .put("connector", connector)
                .put("connectorType", connectorType)
                .put("connectorCategory", connectorCategory)
                .put("description", description)
                .put("status", status)
                .put("jobConfig", mapToJsonString(jobConfig))
                .put("connectorConfig", mapToJsonString(connectorConfig));

        if (id != null && !id.isEmpty()) {
            json.put("_id", id);
        }
        return json;
    }

    /**
     * Kafka connector use name and config as json attribute name. Which maps to connect and connectConfig
     * @return a json object
     */
    public JsonObject toKafkaConnectJson() {
        JsonObject json = new JsonObject()
                .put("name", connector)
                    .put("config", mapToJsonObj(connectorConfig));
            return json;
    }

    /*
     * All below get method is needed to render json to rest. Do not override.
     */
    public String getName() {
        return name;
    }

    public String getConnector() {
        return connector;
    }

    public String getConnectorCategory() {
        return findConnectorCategory(getConnectorType());
    }

    public String getConnectorType() {
        return connectorType.name();
    }

    public String findConnectorCategory(String ct) {
        if(StringUtils.indexOfAny(ct, new String[]{"TRANS", "JOINS"}) == -1) {
            return "CONNECT";
        }
        return "TRANSFORM";
    }

    public String getDescription() {
        return description;
    }

    public String getStatus() {
        return status;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getId() {
        return id;
    }

    public HashMap<String, String> getJobConfig() {
        return jobConfig;
    }

    public HashMap<String, String> getConnectorConfig() {
        return connectorConfig;
    }

    public DFJobPOPJ setName(String name) {
        this.name = name;
        return this;
    }

    public DFJobPOPJ setConnector(String connector) {
        this.connector = connector;
        return this;
    }

    public DFJobPOPJ setConnectorType(String connector_type) {
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.valueOf(connector_type);
        this.connectorCategory = findConnectorCategory(connector_type);
        return this;
    }

    public DFJobPOPJ setConnectorCategory(String cc) {
        this.connectorCategory = cc;
        return this;
    }

    public DFJobPOPJ setDescription(String description) {
        this.description = description;
        return this;
    }

    public DFJobPOPJ setStatus(String status) {
        this.status = status;
        return this;
    }

    public DFJobPOPJ setId(String id) {
        this.id = id;
        return this;
    }

    public DFJobPOPJ setTaskId(String task_id) {
        this.taskId = task_id;
        return this;
    }

    public DFJobPOPJ setConnectorConfig(HashMap<String, String> connector_config) {
        this.connectorConfig = connector_config;
        return this;
    }

    public DFJobPOPJ setJobConfig(HashMap<String, String> job_config) {
        this.jobConfig = job_config;
        return this;
    }

    public DFJobPOPJ setFlinkIDToJobConfig(String jobID) {
        this.jobConfig.put("flink.submit.job.id", jobID);
        return this;
    }

    public String mapToJsonString(HashMap<String, String> hm) {
        ObjectMapper mapperObj = new ObjectMapper();
        try {
            return mapperObj.writeValueAsString(hm);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public JsonObject mapToJsonObj(HashMap<String, String> hm) {
        JsonObject json = new JsonObject();
        for (String key : hm.keySet()) {
            json.put(key, hm.get(key));
        }
        return json;
    }


}
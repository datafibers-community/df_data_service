package com.datafibers.model;

import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;

/**
 * Meta Objects Response for REST API
 */
public class DFJobPOPJ {

    private String id; // id as pk, which is also used as task id
    private String taskSeq; // Identify each task order in a job
    private String name; // Name of the task
    private String connectUid; // Generate UID using Mongo API to identify connect name for Kafka connect = id
    private String jobUid; // UID for the Job for future usage.
    private ConstantApp.DF_CONNECT_TYPE connectorType; // Identify proper connectUid type from enum
    private String connectorCategory;
    private String description; // Description about job and connectUid
    private String status; // Job/Connector status
    private String udfUpload;

    /*
     * The reason we keep them as HashMap is because we do not want to SerDe all field (in that case, we have to define all attribute in
     * configuration file we may use. By using hashmap, we have such flexibility to have one attribute packs all possible configurations.
     * As result, this two below fields are saved as string instead object in mongo and lose native good format (not true nest json). But,
     * we keep flexibility to do any processing through hashmap. And, we can expect any configuration in the config file without changing
     * our code.
     */
    private HashMap<String, String> jobConfig; //configuration or metadata for the job
    private HashMap<String, String> connectorConfig; //configuration for the connectUid used. This will maps to Kafka Connect config attribute

    public DFJobPOPJ(String task_seq, String name, String connector_uid, String connector_type, String description,
                     String status, HashMap<String, String> job_config, HashMap<String, String> connector_config) {
        this.taskSeq = task_seq;
        this.name = name;
        this.connectUid = connector_uid;
        this.jobUid = "NOT_ASSIGNED";
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.valueOf(connector_type);
        this.connectorCategory = findConnectorCategory(connector_type);
        this.description = description;
        this.status = status;
        this.id = "";
        this.jobConfig = job_config;
        this.connectorConfig = connector_config;
    }

    public DFJobPOPJ(String name, String connector_uid, String status,
                     HashMap<String, String> job_config, HashMap<String, String> connector_config) {
        this.taskSeq = "0";
        this.name = name;
        this.connectUid = connector_uid;
        this.jobUid = "NOT_ASSIGNED";
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.NONE;
        this.connectorCategory = findConnectorCategory("");
        this.description = "";
        this.status = status;
        this.id = "";
        this.jobConfig = job_config;
        this.connectorConfig = connector_config;
    }

    public DFJobPOPJ(String name, String connector_uid, String status) {
        this.name = name;
        this.connectUid = connector_uid;
        this.jobUid = "NOT_ASSIGNED";
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.NONE;
        this.connectorCategory = findConnectorCategory("");
        this.description = "";
        this.status = status;
        this.id = "";
        this.taskSeq = "0";
        this.jobConfig = null;
        this.connectorConfig = null;
    }

    // Used by
    public DFJobPOPJ(JsonObject json) {
        this.taskSeq = json.getString("taskSeq");
        this.name = json.getString("name");
        this.connectUid = json.getString("connectUid");
        this.jobUid = json.getString("jobUid");
        this.connectorType = ConstantApp.DF_CONNECT_TYPE.valueOf(json.getString("connectorType"));
        this.connectorCategory = findConnectorCategory(json.getString("connectorType"));
        this.description = json.getString("description");
        this.status = json.getString("status");
        this.id = json.getString("_id");
        this.udfUpload = json.getString("udfUpload");
        this.jobConfig = (json.containsKey("jobConfig") && json.getValue("jobConfig") != null) ?
                HelpFunc.mapToHashMapFromJson(json.getJsonObject("jobConfig")) : null;
        this.connectorConfig = (json.containsKey("connectorConfig") && json.getValue("connectorConfig") != null) ?
                HelpFunc.mapToHashMapFromJson(json.getJsonObject("connectorConfig")) : null;
    }

    public DFJobPOPJ() {
        this.id = "";
    }

    public JsonObject toJson() {

        JsonObject json = new JsonObject()
                .put("name", name)
                .put("taskSeq", taskSeq)
                .put("connectUid", connectUid)
                .put("jobUid", jobUid)
                .put("connectorType", connectorType)
                .put("connectorCategory", connectorCategory)
                .put("description", description)
                .put("status", status)
                .put("jobConfig", jobConfig == null ? null : HelpFunc.mapToJsonFromHashMapD2U(jobConfig))
                .put("connectorConfig", HelpFunc.mapToJsonFromHashMapD2U(connectorConfig))
                .put("udfUpload", udfUpload);

        if (id != null && !id.isEmpty()) {
            json.put("_id", id);
        }
        return json;
    }

    public JsonObject toPostJson() {

        JsonObject json = new JsonObject()
                .put("name", name)
                .put("taskSeq", taskSeq)
                .put("connectUid", connectUid)
                .put("jobUid", jobUid)
                .put("connectorType", connectorType)
                .put("connectorCategory", connectorCategory)
                .put("description", description)
                .put("status", status)
                .put("jobConfig", jobConfig == null ? null : HelpFunc.mapToJsonFromHashMapD2U(jobConfig))
                .put("connectorConfig", HelpFunc.mapToJsonFromHashMapD2U(connectorConfig))
                .put("udfUpload", udfUpload);

        if (id != null && !id.isEmpty()) {
            json.put("id", id);
        }
        return json;
    }

    /**
     * Kafka connectUid use name and config as json attribute name. Which maps to connect and connectConfig
     *
     * @return a json object
     */
    public JsonObject toKafkaConnectJson() {
        return new JsonObject().put("name", connectUid).put("config", HelpFunc.mapToJsonFromHashMapU2D(connectorConfig));
    }

    public JsonObject toKafkaConnectJsonConfig() {
        return HelpFunc.mapToJsonFromHashMapU2D(connectorConfig);
    }

    /*
     * All below get method is needed to render json to rest. Do not override.
     */
    public String getName() {
        return name;
    }

    public String getConnectUid() {
        return connectUid;
    }

    public String getJobUid() {
        return jobUid;
    }

    public String getConnectorCategory() {
        return connectorCategory;
    }

    public String getConnectorType() {
        return connectorType.name();
    }

    public String findConnectorCategory(String ct) {
        return ct.split("_")[0];
    }

    public String getDescription() {
        return description;
    }

    public String getStatus() {
        return status;
    }

    public String getTaskSeq() {
        return taskSeq;
    }

    public String getId() {
        return id;
    }

    public HashMap<String, String> getJobConfig() {
        return jobConfig;
    }

    public String getJobConfig(String key) {
        return jobConfig.containsKey(key)? jobConfig.get(key) : "";
    }

    public HashMap<String, String> getConnectorConfig() {
        return connectorConfig;
    }

    public String getConnectorConfig(String key) {
        return connectorConfig.containsKey(key) ? connectorConfig.get(key) : "";
    }

    public DFJobPOPJ setName(String name) {
        this.name = name;
        return this;
    }

    public DFJobPOPJ setConnectUid(String connectUid) {
        this.connectUid = connectUid;
        return this;
    }

    public DFJobPOPJ setJobUid(String jobUid) {
        this.jobUid = jobUid;
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

    public DFJobPOPJ setTaskSeq(String task_id) {
        this.taskSeq = task_id;
        return this;
    }

    public DFJobPOPJ setConnectorConfig(HashMap<String, String> connector_config) {
        this.connectorConfig = connector_config;
        return this;
    }

    public DFJobPOPJ setConnectorConfig(String key, String value) {
        if (this.connectorConfig == null) this.connectorConfig = new HashMap<>();
        this.connectorConfig.put(key, value);
        return this;
    }

    public DFJobPOPJ setJobConfig(HashMap<String, String> job_config) {
        this.jobConfig = job_config;
        return this;
    }

    public DFJobPOPJ setJobConfig(String key, String value) {
        if (this.jobConfig == null) this.jobConfig = new HashMap<>();
        this.jobConfig.put(key, value);
        return this;
    }

    public DFJobPOPJ setFlinkIDToJobConfig(String jobID) {
        return setJobConfig(ConstantApp.PK_FLINK_SUBMIT_JOB_ID, jobID);
    }

    @JsonIgnore
    public String getFlinkIDFromJobConfig() {
        if (this.jobConfig != null && this.jobConfig.containsKey(ConstantApp.PK_FLINK_SUBMIT_JOB_ID))
            return this.jobConfig.get(ConstantApp.PK_FLINK_SUBMIT_JOB_ID);
        return "";
    }

    public void setUdfUpload(String tmpUDFUpload) {
        udfUpload = tmpUDFUpload;
    }

    public String getUdfUpload() {
        return udfUpload;
    }


}
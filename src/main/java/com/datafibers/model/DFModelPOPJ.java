package com.datafibers.model;

import com.datafibers.util.HelpFunc;
import io.vertx.core.json.JsonObject;
import java.time.LocalTime;
import java.util.HashMap;

/**
 * Meta Objects Response for REST API
 */
public class DFModelPOPJ {

    private String id; // id as pk, which is also used as model id
    private String name; // Name of the ml model
    private String type; // Type of the ml model, such as sparkml, scikit-learn, tensorflow, xgboost, dl4j
    private String category; // Model category, such as classification, recommendation, etc
    private String description; // Description about model
    private String path; // The model path in hdfs
    private String udf; // The spark sql/hive udf name for the model
    private String createDate; // The creation date for the model
    private String updateDate; // The creation date for the model
    private HashMap<String, String> modelInputPara; //ordered input parameters for the model // TODO check if useful
    private String modelOutputPara; //Output parameters for the model // TODO check if useful
    private String idTrained; // Job id which trains and persist the model if avaliable

    public DFModelPOPJ() {
        this.id = "";
    }

    public DFModelPOPJ(String id, String name, String type, String category, String description,
                       String path, String udf, String createDate, String updateDate,
                       HashMap<String, String> modelInputPara, String modelOutputPara, String idTrained) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.category = category;
        this.description = description;
        this.path = path;
        this.udf = udf;
        this.createDate = createDate.isEmpty() ? LocalTime.now().toString():createDate;
        this.updateDate = updateDate.isEmpty() ? LocalTime.now().toString():updateDate;
        this.modelInputPara = modelInputPara;
        this.modelOutputPara = modelOutputPara;
        this.idTrained = idTrained;
    }

    // Used by
    public DFModelPOPJ(JsonObject json) {
        this.id = json.getString("_id");
        this.name = json.getString("name");
        this.type = json.getString("type");
        this.category = json.getString("category");
        this.description = json.getString("description");
        this.path = json.getString("path");
        this.udf = json.getString("udf");
        this.createDate = json.getString("createDate");
        this.updateDate = json.getString("updateDate");
        this.modelInputPara = (json.containsKey("modelInputPara") && json.getValue("modelInputPara") != null) ?
                HelpFunc.mapToHashMapFromJson(json.getJsonObject("modelInputPara")) : null;
        this.modelOutputPara = json.getString("modelOutputPara");
        this.idTrained = json.getString("idTrained");
    }

    public JsonObject toJson() {

        JsonObject json = new JsonObject()
                .put("name", name)
                .put("type", type)
                .put("category", category)
                .put("description", description)
                .put("path", path)
                .put("udf", udf)
                .put("createDate", createDate)
                .put("updateDate", updateDate)
                .put("jobConfig", modelInputPara == null ? null : HelpFunc.mapToJsonFromHashMapD2U(modelInputPara))
                .put("modelOutputPara", modelOutputPara)
                .put("idTrained", idTrained)
                ;

        if (id != null && !id.isEmpty()) {
            json.put("_id", id);
        }
        return json;
    }

    public JsonObject toPostJson() {

        JsonObject json = new JsonObject()
                .put("name", name)
                .put("type", type)
                .put("category", category)
                .put("description", description)
                .put("path", path)
                .put("udf", udf)
                .put("createDate", createDate)
                .put("updateDate", updateDate)
                .put("jobConfig", modelInputPara == null ? null : HelpFunc.mapToJsonFromHashMapD2U(modelInputPara))
                .put("modelOutputPara", modelOutputPara)
                .put("idTrained", idTrained)
                ;

        if (id != null && !id.isEmpty()) {
            json.put("id", id);
        }
        return json;
    }

    public String getId() {
        return id;
    }

    public DFModelPOPJ setId(String id) {
        this.id = id;
        return this;
    }

    public String getType() {
        return type;
    }

    public DFModelPOPJ setType(String type) {
        this.type = type;
        return this;
    }

    public String getCategory() {
        return category;
    }

    public DFModelPOPJ setCategory(String category) {
        this.category = category;
        return this;
    }

    public String getPath() {
        return path;
    }

    public DFModelPOPJ setPath(String path) {
        this.path = path;
        return this;
    }

    public String getUdf() {
        return udf;
    }

    public DFModelPOPJ setUdf(String udf) {
        this.udf = udf;
        return this;
    }

    public String getCreateDate() {
        return createDate;
    }

    public DFModelPOPJ setCreateDate(String createDate) {
        this.createDate = createDate;
        return this;
    }

    public String getUpdateDate() {
        return updateDate;
    }

    public DFModelPOPJ setUpdateDate(String updateDate) {
        this.updateDate = updateDate;
        return this;
    }

    public HashMap<String, String> getModelInputPara() {
        return modelInputPara;
    }

    public DFModelPOPJ setModelInputPara(HashMap<String, String> modelInputPara) {
        this.modelInputPara = modelInputPara;
        return this;
    }

    public String getModelOutputPara() {
        return modelOutputPara;
    }

    public DFModelPOPJ setModelOutputPara(String modelOutputPara) {
        this.modelOutputPara = modelOutputPara;
        return this;
    }

    public String getIdTrained() {
        return idTrained;
    }

    public DFModelPOPJ setIdTrained(String idTrained) {
        this.idTrained = idTrained;
        return this;
    }
}
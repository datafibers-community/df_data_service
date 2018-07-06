package com.datafibers.model;

import io.vertx.core.json.JsonObject;

/**
 * Created by will on 2017-08-11.
 */
public class DFLogPOPJ {

    private String id; // id as pk, which is also used as task id
    private String timestamp; // Identify each task order in a job
    private String level; // Name of the task
    private String message;
    private String fileName;
    private String method;
    private String lineNumber;
    private String className;

    public DFLogPOPJ(String id, String timestamp, String level, String className, String fileName, String lineNumber, String method, String message) {
        this.id = id;
        this.timestamp = timestamp;
        this.level = level;
        this.className = className;
        this.fileName = fileName;
        this.lineNumber = lineNumber;
        this.method = method;
        this.message = message;
    }

    // Used by
    public DFLogPOPJ(JsonObject json) {
        this.id = json.getJsonObject("_id").getString("$oid");
        this.timestamp = json.getJsonObject("timestamp").getString("$date");
        this.level = json.getString("level");
        this.className = json.getJsonObject("class").getString("className");
        this.fileName = json.getString("fileName");
        this.lineNumber = json.getString("lineNumber");
        this.method = json.getString("method");
        this.message = json.getString("message");

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(String lineNumber) {
        this.lineNumber = lineNumber;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}

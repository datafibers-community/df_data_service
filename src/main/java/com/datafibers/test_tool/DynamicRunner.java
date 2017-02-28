package com.datafibers.test_tool;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.table.Table;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Interface for dynamic Flink stable api script generation and running
 */
public interface DynamicRunner {

    /*
     * Run Flink Table API Transformation
     */
    default void runTransform(DataSet<String> ds) {

    }

    default void runTransform(DataStream<String> ds) {

    }

    default Table transTableObj(Table tbl) {
        return tbl;
    }
}

package com.datafibers.test_tool;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.table.Table;

/**
 * Created by DUW3 on 2/24/2017.
 */
public interface DynamicRunner {

    /*
     * Run Flink Table API Transformation
     */
    default void runTransform(DataSet<String> ds) {

    };

    default Table getTableObj() {
        return null;
    };
}

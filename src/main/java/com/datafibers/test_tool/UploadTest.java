package com.datafibers.test_tool;

import com.datafibers.util.HelpFunc;

/**
 * Created by dadu on 24/10/2017.
 */
public class UploadTest {

    public static void main(String[] args)  {

        String jar_id = HelpFunc.uploadJar("http://localhost:8001/jars/upload",
                "C:/Users/dadu/Coding/df_data_service/target/df-data-service-1.1-SNAPSHOT-fat.jar");
        System.out.println(jar_id);
    }
}

package com.datafibers.test_tool;
import com.datafibers.service.DFInitService;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.datafibers.util.MongoAdminClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by DUW3 on 11/11/2016.
 */
public class JsonTest {

    public static String arrayToString(JsonArray ja) {
        String result = "";
        for (int i = 0; i < ja.size(); i++) {
            result = result + ja.getValue(i).toString() + ",";
        }
        return result.substring(0, result.length() - 1);
    }

    public static JsonArray livyTableResultToArray(JsonObject livyStatementResult) {
        JsonObject output = livyStatementResult
                .getJsonObject("output")
                .getJsonObject("data")
                .getJsonObject("application/vnd.livy.table.v1+json");

        JsonArray header = output.getJsonArray("headers");
        JsonArray data = output.getJsonArray("data");
        JsonArray result = new JsonArray();
        JsonObject headerRowJson = new JsonObject();
        String headerRow = "";

        if(header.size() == 0) return new JsonArray().add(new JsonObject().put("row", ""));

        for(int i = 0; i < header.size(); i++) {
            headerRow = headerRow + header.getJsonObject(i).getString("name") + ",";
        }

        result.add(headerRowJson.put("row", headerRow));

        for(int i = 0; i < data.size(); i++) {
            result.add(new JsonObject().put("row", arrayToString(data.getJsonArray(i))));
        }

        return result;
    }

    public static void main(String[] args) throws IOException, DecoderException {
        String mySb = "--comments \n" +
                "select * from test;--comments2\n" +
                "select * from\n" +
                "test2;";
         mySb = "--comments \n" +
                "select * from test;";
        Arrays.asList(HelpFunc.sqlCleaner(mySb)).forEach(System.out::println);

    }
}

package com.datafibers.test_tool;
import com.datafibers.util.HelpFunc;
import org.apache.commons.codec.DecoderException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by DUW3 on 11/11/2016.
 */
public class JsonTest {

    public static void main(String[] args) throws IOException, DecoderException {
        System.out.println("TestCase_Simple Avro Producer");

        String JSON_STRING = "{\"taskId\":1,\"name\":\"testtest\",\"connector\":\"testtest\",\"connectorType\":\"CONNECT_KAFKA_SOURCE\",\"status\":null,\"description\":null,\"jobConfig\":null,\"connectorConfig\":{\"connector.class\":\"org.apache.kafka.connect.file.FileStreamSourceConnector\",\"file\":\"a.dat\",\"tasks.max\":\"1\",\"name\":\"testtest\",\"topic\":\"testtest\"},\"connectorConfig_1\":{\"config_ignored\":\"template marker, remove it to make config effective\",\"connector.class\":\"org.apache.kafka.connect.file.FileStreamSinkConnector\",\"file\":\"File name to keep the data exported from Kafka.\",\"tasks.max\":\"Number of tasks in parallel.\",\"name\":\"Kafka Connect name.\",\"topics\":\"List of Kafka topics having data streamed out\"}}";

        JSONObject json = new JSONObject(JSON_STRING);
        int index = 0;
        int index_found = 0;
        String json_key_to_check;
        while (true) {
            if (index == 0) {
                json_key_to_check = "connectorConfig";
            } else json_key_to_check = "connectorConfig_" + index;
            if(json.has(json_key_to_check)) {
                if(json.getJSONObject(json_key_to_check).has("config_ignored")) {
                    json.remove(json_key_to_check);
                } else index_found = index;
            } else break;
            index ++;
        }

        if (index_found >0 )
            json.put("connectorConfig", json.getJSONObject("connectorConfig_" + (index_found))).remove("connectorConfig_" + (index_found));

        System.out.println(json.toString());
        System.out.println(HelpFunc.cleanJsonConfig(JSON_STRING, "connectorConfig_", "config_ignored"));

    }
}

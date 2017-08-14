package com.datafibers.test_tool;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import org.apache.commons.codec.DecoderException;
import java.io.IOException;

/**
 * Created by DUW3 on 11/11/2016.
 */
public class JsonTest {

    public static void main(String[] args) throws IOException, DecoderException {
        System.out.println("TestCase_Simple Avro Producer");
        String JSON_STRING = "{\"description\":\"This is default description.\",\"connectorConfig\":{\"tasks.max\":\"1\",\"topics\":[\"test_value\",\"output_value\"],\"file.overwrite\":true,\"file.location\":\"/home/vagrant/df_data\",\"file.glob\":\"*.json\"},\"connectorType\":\"CONNECT_KAFKA_SOURCE_AVRO\",\"name\":\"test\",\"taskSeq\":1}";
        System.out.println(HelpFunc.convertTopicsFromArrayToString(JSON_STRING, ConstantApp.PK_DF_TOPICS_ALIAS));

    }
}

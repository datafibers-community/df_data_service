package com.datafibers.test_tool;
import com.datafibers.service.DFInitService;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;
import com.datafibers.util.MongoAdminClient;
import io.vertx.core.json.JsonObject;
import org.apache.commons.codec.DecoderException;
import java.io.IOException;

/**
 * Created by DUW3 on 11/11/2016.
 */
public class JsonTest {

    public static void main(String[] args) throws IOException, DecoderException {
        new MongoAdminClient("localhost", 27017, "DEFAULT_DB")
                .useCollection("df_installed")
                .importJsonInputStream(DFInitService.class.getResourceAsStream("/import/df_installed.json"))
                .close();
    }
}

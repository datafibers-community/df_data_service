package com.datafibers.util;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

/**
 * List of help functions
 */
public class HelpFunc {

    public static <T> T coalesce(T a, T b) {
        return a == null ? b : a;
    }

    /**
     * Print error message in better JSON format
     *
     * @param error_code
     * @param msg
     * @return
     */
    public static String errorMsg(int error_code, String msg) {
        return Json.encodePrettily(new JsonObject()
                .put("code", String.format("%6d", error_code))
                .put("message", msg));
    }
}

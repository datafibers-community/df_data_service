package com.datafibers.util;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.json.JSONObject;

/**
 * List of help functions to be used for all DF classes.
 */
public class HelpFunc {
	
	final static int max = 1000;
	final static int min = 1;
	final static String underscore = "_";
	final static String period = ".";
	final static String space = " ";
	final static String colon = ":";
	
	public static String generateUniqueFileName(String inputName) {
		Date curDate = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String DateToStr = format.format(curDate);

		DateToStr = DateToStr.replaceAll(space, underscore);
		DateToStr = DateToStr.replaceAll(colon, underscore);
		

		// nextInt excludes the top value so we have to add 1 to include the top value
		Random rand = new Random();
	    int randomNum = rand.nextInt((max - min) + 1) + min;
	    
		if (inputName != null && inputName.indexOf(period) > 0) {
			int firstPart = inputName.indexOf(period);
			String fileName = inputName.substring(0, firstPart);
			String fileExtension = inputName.substring(firstPart + 1);
			DateToStr = fileName + underscore + DateToStr + underscore + randomNum + "." + fileExtension;
		} else {
			DateToStr = inputName + underscore + DateToStr + underscore + randomNum;
		}
		
		return DateToStr;
	}
	
	public String getCurrentJarRunnigFolder() {
		String jarPath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();  // "/home/vagrant/df-data-service-1.0-SNAPSHOT-fat.jar";
		int i = jarPath.lastIndexOf("/");
		
		if (i > 0) {
		    jarPath = jarPath.substring(0, i + 1);
		}
		
		return jarPath;
	}

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

	/**
	 * This function will search JSONSTRING to find patterned keys_1..n. If it has key_ignored_mark subkey, the element
	 * will be removed. For example {"connectorConfig_1":{"config_ignored":"test"}, "connectorConfig_2":{"test":"test"}}
	 * will be cleaned as {"connectorConfig":{"test":"test"}}
	 * @param JSON_STRING
	 * @param key_ingored_mark: If the
	 * @return json toString()
	 */
	public static String cleanJsonConfig(String JSON_STRING, String key_pattern, String key_ingored_mark ) {
		JSONObject json = new JSONObject(JSON_STRING);
		int index = 0;
		int index_found = 0;
		String json_key_to_check;
		while (true) {
			if (index == 0) {
				json_key_to_check = key_pattern.replace("_", "");
			} else json_key_to_check = key_pattern + index;

			if(json.has(json_key_to_check)) {
				if(json.getJSONObject(json_key_to_check).has(key_ingored_mark)) {
					json.remove(json_key_to_check);
				} else index_found = index;
			} else break;
			index ++;
		}
		if (index_found > 0)
			json.put(key_pattern.replace("_", ""), json.getJSONObject(key_pattern + index_found)).remove(key_pattern + index_found);
		return json.toString();
	}

	public static String cleanJsonConfig(String JSON_STRING) {
		return cleanJsonConfig(JSON_STRING, "connectorConfig_", "config_ignored");
	}

	/**
	 * Print list of Properties
	 * @param prop
	 * @return
	 */
	public static String getPropertyAsString(Properties prop) {
		StringWriter writer = new StringWriter();
		prop.list(new PrintWriter(writer));
		return writer.getBuffer().toString();
	}
	
	public static void main(String[] args) {
	}
}

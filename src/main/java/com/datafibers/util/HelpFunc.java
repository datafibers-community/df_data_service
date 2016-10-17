package com.datafibers.util;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * List of help functions
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
}

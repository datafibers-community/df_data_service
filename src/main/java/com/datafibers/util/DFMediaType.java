package com.datafibers.util;

import com.hubrick.vertx.rest.MediaType;

public class DFMediaType extends MediaType {
	public final static MediaType APPLICATION_SCHEMAREGISTRY_JSON;
	public final static String APPLICATION_SCHEMAREGISTRY_JSON_VALUE = "application/vnd.schemaregistry.v1+json";
	 
	static {
	    APPLICATION_SCHEMAREGISTRY_JSON = valueOf(APPLICATION_SCHEMAREGISTRY_JSON_VALUE);
	}
	 
	public DFMediaType(String type) {
		super(type);
	}

}

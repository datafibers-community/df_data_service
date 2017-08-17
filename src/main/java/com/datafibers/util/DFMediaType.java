package com.datafibers.util;

import com.hubrick.vertx.rest.MediaType;

public class DFMediaType extends MediaType {
	public final static MediaType APPLICATION_SCHEMA_REGISTRY_JSON;
	public final static String APPLICATION_SCHEMA_REGISTRY_JSON_VALUE = "application/vnd.schemaregistry.v1+json";
	 
	static {
	    APPLICATION_SCHEMA_REGISTRY_JSON = valueOf(APPLICATION_SCHEMA_REGISTRY_JSON_VALUE);
	}
	 
	public DFMediaType(String type) {
		super(type);
	}

}

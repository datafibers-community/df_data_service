define({ "api": [
  {
    "type": "get",
    "url": "/processor",
    "title": "1.List all tasks",
    "version": "0.1.1",
    "name": "getAll",
    "group": "All",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where we get list of all connects and transforms.</p>",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connects",
            "description": "<p>List of connect and transform task profiles.</p>"
          }
        ]
      }
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/processor"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "All"
  },
  {
    "type": "get",
    "url": "/hist",
    "title": "2.List all processed history",
    "version": "0.1.1",
    "name": "getAllProcessHistory",
    "group": "All",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where get history of processed files in tasks or jobs.</p>",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "history",
            "description": "<p>List of processed history.</p>"
          }
        ]
      }
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/hist"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "All"
  },
  {
    "type": "post",
    "url": "/ps",
    "title": "4.Add a connect task",
    "version": "0.1.1",
    "name": "addOneConnects",
    "group": "Connect",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is how we add or submit a connect to DataFibers.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "None",
            "description": "<p>Json String of task as message body.</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "201": [
          {
            "group": "201",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connect",
            "description": "<p>The newly added connect task.</p>"
          }
        ]
      }
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 409 Conflict\n{\n  \"code\" : \"409\",\n  \"message\" : \"POST Request exception - Conflict\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Connect"
  },
  {
    "type": "delete",
    "url": "/ps/:id",
    "title": "6.Delete a connect task",
    "version": "0.1.1",
    "name": "deleteOneConnects",
    "group": "Connect",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is how to delete a specific connect.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "id",
            "description": "<p>task Id (_id in mongodb).</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "String",
            "optional": false,
            "field": "message",
            "description": "<p>OK.</p>"
          }
        ]
      }
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 400 Bad Request\n{\n  \"code\" : \"400\",\n  \"message\" : \"Delete Request exception - Bad Request.\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Connect"
  },
  {
    "type": "get",
    "url": "/ps",
    "title": "1.List all connects task",
    "version": "0.1.1",
    "name": "getAllConnects",
    "group": "Connect",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where we get data for all active connects.</p>",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connects",
            "description": "<p>List of connect task profiles.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response:",
          "content": "HTTP/1.1 200 OK\n[ {\n     \"id\" : \"58471d13bba4a429f8a272b6\",\n     \"taskSeq\" : \"1\",\n     \"name\" : \"tesavro\",\n     \"connectUid\" : \"58471d13bba4a429f8a272b6\",\n     \"jobUid\" : \"reserved for job level tracking\",\n     \"connectorType\" : \"CONNECT_KAFKA_SOURCE_AVRO\",\n     \"connectorCategory\" : \"CONNECT\",\n     \"description\" : \"task description\",\n     \"status\" : \"LOST\",\n     \"udfUpload\" : null,\n     \"jobConfig\" : null,\n     \"connectorConfig\" : {\n         \"connector.class\" : \"com.datafibers.kafka.connect.FileGenericSourceConnector\",\n         \"schema.registry.uri\" : \"http://localhost:8081\",\n         \"cuid\" : \"58471d13bba4a429f8a272b6\",\n         \"file.location\" : \"/home/vagrant/df_data/\",\n         \"tasks.max\" : \"1\",\n         \"file.glob\" : \"*.{json,csv}\",\n         \"file.overwrite\" : \"true\",\n         \"schema.subject\" : \"test-value\",\n         \"topic\" : \"testavro\"\n     }\n  }\n]",
          "type": "json"
        }
      ]
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/ps"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Connect"
  },
  {
    "type": "get",
    "url": "/installed_connects",
    "title": "2.List installed connect lib",
    "version": "0.1.1",
    "name": "getAllInstalledConnects",
    "group": "Connect",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where get list of launched or installed connect jar or libraries.</p>",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connects",
            "description": "<p>List of connects installed and launched by DataFibers.</p>"
          }
        ]
      }
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/installed_connects"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Connect"
  },
  {
    "type": "get",
    "url": "/ps/:id",
    "title": "3.Get a connect task",
    "version": "0.1.1",
    "name": "getOne",
    "group": "Connect",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where we get data for one task with specified id.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "id",
            "description": "<p>task Id (_id in mongodb).</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connects",
            "description": "<p>One connect task profiles.</p>"
          }
        ]
      }
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/ps/:id"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Connect"
  },
  {
    "type": "put",
    "url": "/ps/:id",
    "title": "5.Update a connect task",
    "version": "0.1.1",
    "name": "updateOneConnects",
    "group": "Connect",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is how we update the connect configuration to DataFibers.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "id",
            "description": "<p>task Id (_id in mongodb).</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "String",
            "optional": false,
            "field": "message",
            "description": "<p>OK.</p>"
          }
        ]
      }
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 404 Not Found\n{\n  \"code\" : \"409\",\n  \"message\" : \"PUT Request exception - Not Found.\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Connect"
  },
  {
    "type": "post",
    "url": "/schema",
    "title": "3.Add a Schema",
    "version": "0.1.1",
    "name": "addOneSchema",
    "group": "Schema",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is how we add a new schema to schema registry service</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "None",
            "description": "<p>Json String of Schema as message body.</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "201": [
          {
            "group": "201",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connect",
            "description": "<p>The newly added connect task.</p>"
          }
        ]
      }
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 409 Conflict\n{\n  \"code\" : \"409\",\n  \"message\" : \"POST Request exception - Conflict\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Schema"
  },
  {
    "type": "get",
    "url": "/schema",
    "title": "1.List all schema",
    "version": "0.1.1",
    "name": "getAllSchemas",
    "group": "Schema",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where we get list of available schema data from schema registry.</p>",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connects",
            "description": "<p>List of schemas added in schema registry.</p>"
          }
        ]
      }
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/schema"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Schema"
  },
  {
    "type": "get",
    "url": "/schema/:subject",
    "title": "2.Get a schema",
    "version": "0.1.1",
    "name": "getOneSchema",
    "group": "Schema",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where we get schema with specified schema subject.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "subject",
            "description": "<p>schema subject name in schema registry.</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "schema",
            "description": "<p>One schema object.</p>"
          }
        ]
      }
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/schema/:subject"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Schema"
  },
  {
    "type": "put",
    "url": "/schema/:id",
    "title": "4.Update a schema",
    "version": "0.1.1",
    "name": "updateOneSchema",
    "group": "Schema",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is how we update specified schema information in schema registry.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "subject",
            "description": "<p>schema subject in schema registry.</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "String",
            "optional": false,
            "field": "message",
            "description": "<p>OK.</p>"
          }
        ]
      }
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 404 Not Found\n{\n  \"code\" : \"409\",\n  \"message\" : \"PUT Request exception - Not Found.\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Schema"
  },
  {
    "type": "post",
    "url": "/tr",
    "title": "4.Add a transform task",
    "version": "0.1.1",
    "name": "addOneTransforms",
    "group": "Transform",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is how we submit or add a transform task to DataFibers.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "None",
            "description": "<p>Json String of task as message body.</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "201": [
          {
            "group": "201",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connect",
            "description": "<p>The newly added connect task.</p>"
          }
        ]
      }
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 409 Conflict\n{\n  \"code\" : \"409\",\n  \"message\" : \"POST Request exception - Conflict.\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Transform"
  },
  {
    "type": "delete",
    "url": "/tr/:id",
    "title": "6.Delete a transform task",
    "version": "0.1.1",
    "name": "deleteOneTransforms",
    "group": "Transform",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is how to delete a specific transform.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "id",
            "description": "<p>task Id (_id in mongodb).</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "String",
            "optional": false,
            "field": "message",
            "description": "<p>OK.</p>"
          }
        ]
      }
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 400 Bad Request\n{\n  \"code\" : \"400\",\n  \"message\" : \"Delete Request exception - Bad Request.\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Transform"
  },
  {
    "type": "get",
    "url": "/tr",
    "title": "1.List all transforms task",
    "version": "0.1.1",
    "name": "getAllConnects",
    "group": "Transform",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where get data for all active transforms.</p>",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "connects",
            "description": "<p>List of transform task profiles.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response:",
          "content": "HTTP/1.1 200 OK\n[ {\n     \"id\" : \"58471d13bba4a429f8a272b6\",\n     \"taskSeq\" : \"1\",\n     \"name\" : \"tesavro\",\n     \"connectUid\" : \"58471d13bba4a429f8a272b6\",\n     \"jobUid\" : \"reserved for job level tracking\",\n     \"connectorType\" : \"TRANSFORM_FLINK_SQL_A2J\",\n     \"connectorCategory\" : \"TRANSFORM\",\n     \"description\" : \"task description\",\n     \"status\" : \"LOST\",\n     \"udfUpload\" : null,\n     \"jobConfig\" : null,\n     \"connectorConfig\" : {\n         \"cuid\" : \"58471d13bba4a429f8a272b0\",\n         \"trans.sql\":\"SELECT STREAM symbol, name FROM finance\"\n         \"group.id\":\"consumer3\",\n         \"topic.for.query\":\"finance\",\n         \"topic.for.result\":\"stock\",\n         \"schema.subject\" : \"test-value\"\n     }\n  }\n]",
          "type": "json"
        }
      ]
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/tr"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Transform"
  },
  {
    "type": "get",
    "url": "/installed_transforms",
    "title": "2.List installed transform lib",
    "version": "0.1.1",
    "name": "getAllInstalledTransforms",
    "group": "Transform",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where get list of launched or installed transform jar or libraries.</p>",
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "transforms",
            "description": "<p>List of transforms installed and launched by DataFibers.</p>"
          }
        ]
      }
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/installed_transforms"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Transform"
  },
  {
    "type": "get",
    "url": "/tr/:id",
    "title": "3. Get a transform task",
    "version": "0.1.1",
    "name": "getOne",
    "group": "Transform",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is where we get data for one task with specified id.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "id",
            "description": "<p>task Id (_id in mongodb).</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "transforms",
            "description": "<p>One transform task profiles.</p>"
          }
        ]
      }
    },
    "sampleRequest": [
      {
        "url": "http://localhost:8080/api/df/tr/:id"
      }
    ],
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Transform"
  },
  {
    "type": "put",
    "url": "/tr/:id",
    "title": "5.Update a transform task",
    "version": "0.1.1",
    "name": "updateOneConnects",
    "group": "Transform",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is how we update the transform configuration to DataFibers.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "String",
            "optional": false,
            "field": "id",
            "description": "<p>task Id (_id in mongodb).</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "String",
            "optional": false,
            "field": "message",
            "description": "<p>OK.</p>"
          }
        ]
      }
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 404 Not Found\n{\n  \"code\" : \"409\",\n  \"message\" : \"PUT Request exception - Not Found.\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Transform"
  },
  {
    "type": "post",
    "url": "/uploaded_files",
    "title": "7.Upload file",
    "version": "0.1.1",
    "name": "uploadFiles",
    "group": "Transform",
    "permission": [
      {
        "name": "none"
      }
    ],
    "description": "<p>This is triggered through &quot;ADD File&quot; in the create transform view of Web Admin Console.</p>",
    "parameter": {
      "fields": {
        "Parameter": [
          {
            "group": "Parameter",
            "type": "binary",
            "optional": false,
            "field": "None",
            "description": "<p>Binary String of file content.</p>"
          }
        ]
      }
    },
    "success": {
      "fields": {
        "Success 200": [
          {
            "group": "Success 200",
            "type": "JsonObject[]",
            "optional": false,
            "field": "uploaded_file_name",
            "description": "<p>The name of the file uploaded.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Success-Response:",
          "content": "HTTP/1.1 200 OK\n{\n  \"code\" : \"200\",\n  \"uploaded_file_name\": \"/home/vagrant/flink_word_count.jar\",\n}",
          "type": "json"
        }
      ]
    },
    "error": {
      "fields": {
        "Error 4xx": [
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "code",
            "description": "<p>The error code.</p>"
          },
          {
            "group": "Error 4xx",
            "optional": false,
            "field": "message",
            "description": "<p>The error message.</p>"
          }
        ]
      },
      "examples": [
        {
          "title": "Error-Response:",
          "content": "HTTP/1.1 400 Bad Request\n{\n  \"code\" : \"400\",\n  \"uploaded_file_name\" : \"failed\"\n}",
          "type": "json"
        }
      ]
    },
    "filename": "stage/DFDataProcessor.java",
    "groupTitle": "Transform"
  }
] });

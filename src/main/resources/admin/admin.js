var myApp = angular.module('myApp', ['ng-admin']);
myApp.config(function(RestangularProvider) {
    RestangularProvider.addElementTransformer('posts', function(element) {
        element.connectorConfig_1 = element.connectorConfig;
        element.connectorConfig_2 = element.connectorConfig;
        element.connectorConfig_3 = element.connectorConfig;
        element.connectorConfig_4 = element.connectorConfig;
        return element;
    });
});

myApp.config(['NgAdminConfigurationProvider', function (nga) {
customHeaderTemplate =
'<div class="navbar-header">' +
    '<a class="navbar-brand" href="#" ng-click="appController.displayHome()">' +
        'DataFibers Web Console' +
    '</a>' +
'</div>' +
'<p class="navbar-text navbar-right">' +
    '<a href="https://github.com/datafibers-community/df_data_service">' +
        '<img src="https://raw.githubusercontent.com/datafibers/datafibers.github.io/master/img/logos/logo_blue.png" width="24" height="28">' +
    '</a>' +
'</p>';
    // create an admin application
    var admin = nga.application().title('DataFibers Admin Console').baseApiUrl('http://localhost:8080/api/df/');
    admin.header(customHeaderTemplate);
	var processor = nga.entity('processor').label('ALL');
    var connect = nga.entity('ps').label('CONNECTS');
	var transform = nga.entity('tr').label('TRANSFORMS');
	var schema = nga.entity('schema').identifier(nga.field('subject')).label('SCHEMA');
    var installed_connects = nga.entity('installed_connects').identifier(nga.field('class')).label('INSTALLED').readOnly();
    var process_history = nga.entity('hist').identifier(nga.field('uid')).label('HISTORY').readOnly();

    // set the fields of the connect entity list view
    connect.listView().sortField('name').fields([
        nga.field('id').label('Task ID').isDetailLink(true),
        nga.field('taskId', 'number').format('0o').label('Task Seq'),
        nga.field('name').label('Task Name'),
        nga.field('connectUid').label('Connect UID'),
        nga.field('connectorType').label('Type'),
        nga.field('status').label('Task Status')
    ]);

    // set the fields of the transform entity list view
    transform.listView().sortField('name').fields([
        nga.field('id').label('Task ID').isDetailLink(true),
        nga.field('taskSeq', 'number').format('0o').label('Task Seq'),
        nga.field('name').label('Task Name'),
        nga.field('connectUid').label('Transform UID'),
        nga.field('connectorType').label('Type'),
        nga.field('status').label('Task Status')
    ]);

  	schema.listView().sortField('name').fields([
  	    nga.field('subject').label('Subject Name').isDetailLink(true),
        nga.field('id').label('Schema ID'),
        nga.field('version').label('Schema Version'),
        nga.field('schema').label('Schema'),
        nga.field('compatibility').label('Compatibility')
    ]);

   	process_history.listView().sortField('cuid').fields([
   	    nga.field('cuid').label('Connect UID'),
        nga.field('file_name').label('Processed'),
        nga.field('file_size').label('Size (byte)'),
        nga.field('schema_version').label('Schema Version'),
        nga.field('schema_subject').label('Schema Subject'),
        nga.field('topic_sent').label('Topic'),
        nga.field('process_milliseconds').label('Time Spent (ms.)'),
        nga.field('status').label('Status')
     ]);

    connect.listView().title('Connects Dashboard');
    transform.listView().title('Transforms Dashboard');
    schema.listView().title('Schema Dashboard');
    schema.listView().batchActions([]);
    process_history.listView().title('Process History');
    process_history.listView().batchActions([]);

    connect.creationView().fields([
        nga.field('taskSeq', 'number').format('0o').label('Task Seq'),
        nga.field('name').label('Task Name'),
        //nga.field('connectUid').attributes({placeholder:'No space allowed and 5 chars min.'}).validation({ required: true, pattern: '[A-Za-z0-9\-]{5,20}' }).label('Connects'),
        nga.field('connectorType', 'choice')
                .choices([
                                {value:'CONNECT_KAFKA_SOURCE', label:'Source Flat File'},
                                {value:'CONNECT_KAFKA_SOURCE_AVRO', label:'Source Avro File'},
                                {value:'CONNECT_KAFKA_SINK', label:'Sink Flat File'},
                                {value:'CONNECT_KAFKA_HDFS_SINK', label:'Sink Hadoop|Hive'},
                                {value:'CONNECT_MONGODB_SINK', label:'Sink MongoDB'}
                         ]).label('Connect Type'),
        nga.field('status').editable(false).label('Task Status'),
        nga.field('description', 'text'),
        //nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig','json').label('Connect Config')
		.defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
		"connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
		"file" : "stock.json /* File name for streaming to Kafka. */",
		"tasks.max" : "1 /* Number of tasks in parallel.*/",
		"topic" : "stock /* The single Kafka topic name having data streamed. */"
		})
		.template('<ma-field ng-if="entry.values.connectorType == \'CONNECT_KAFKA_SOURCE\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
		nga.field('connectorConfig_1','json').label('Connect Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "connector.class" : "com.datafibers.kafka.connect.FileGenericSourceConnector",
        "file.location" : "/home/vagrant/ /* Folder where to read the files. */",
        "file.glob" : "*.{json,csv} /* File glob to filter file */",
        "file.overwrite" : "true /* Whether over-written file will re-extract */",
        "schema.subject" : "test-value /* The subject name in schema registry */",
        "schema.registry.uri" : "http://localhost:8081",
        "tasks.max" : "1 /* Number of tasks in parallel. */",
        "topic" : "stock /* The single Kafka topic name having data streamed. */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'CONNECT_KAFKA_SOURCE_AVRO\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
		nga.field('connectorConfig_2','json').label('Connect Config')
		.defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
		"connector.class" : "org.apache.kafka.connect.file.FileStreamSinkConnector",
		"file" : "test.txt /* File name to keep the data exported from Kafka. */",
        "tasks.max" : "1 /* Number of tasks in parallel. */",
		"topics" : "stock, test /* List of Kafka topics having data streamed out */"
		})
		.template('<ma-field ng-if="entry.values.connectorType == \'CONNECT_KAFKA_SINK\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
		nga.field('connectorConfig_3','json').label('Connect Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "connector.class" : "io.confluent.connect.hdfs.HdfsSinkConnector",
        "schema.compatibility" : "BACKWARD",
        "hdfs.url" : "hdfs://localhost:8020",
        "hive.metastore.uris" : "thrift://localhost:9083",
        "hive.integration" : "true /* Whether create Hive tables */",
        "tasks.max" : "1 /* Number of tasks in parallel. */",
        "flush.size" : "1 /* Number of rows to flush to HDFS. */",
        "topics" : "stock, test /* The Kafka topic names having data to sink. */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'CONNECT_KAFKA_HDFS_SINK\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
		nga.field('connectorConfig_4','json').label('Connect Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "connector.class" : "org.apache.kafka.connect.mongodb.MongodbSinkConnector",
        "host" : "localhost /* Hostname of MongoDB */",
        "port" : "27017 /* Port number of MongoDB */",
        "mongodb.database" : "DEFAULT_DB /* Mongo database name where to sink the data */",
        "mongodb.collections" : "df_test /* Mongo collection name where ro sink the data */",
        "tasks.max" : "1 /* Number of tasks in parallel. */",
        "bulk.size" : "1 /* Number of rows to flush to Mongo. */",
        "topics" : "stock, test /* The Kafka topic names having data to sink. */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'CONNECT_MONGODB_SINK\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true)
    ]);

    connect.editionView().fields([
        nga.field('taskSeq', 'number').label('Task Seq').editable(false),
        nga.field('name').label('Task Name').editable(false),
        nga.field('connectUid').label('Connect UID').editable(false),
        nga.field('connectorType').editable(false),
        nga.field('status').editable(false).label('Task Status'),
        nga.field('description', 'text'),
        //nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig','json').label('Connect Config')
    ]);

	transform.creationView().fields([
        nga.field('taskSeq', 'number').label('Task Seq'),
        nga.field('name').label('Task Name'),
        //nga.field('connectUid').attributes({placeholder:'No space allowed and 5 chars min.'}).validation({ required: true, pattern: '[A-Za-z0-9\-]{5,20}' }).label('Transforms'),
        nga.field('connectorType', 'choice')
                .choices([
                                {value:'TRANSFORM_FLINK_SQL_GENE', label:'Flink Streaming SQL (Json/Json String to Json/Json String)'},
                                {value:'TRANSFORM_FLINK_SQL_A2J', label:'Flink Streaming SQL (Avro to Json)'},
                                {value:'TRANSFORM_FLINK_SQL_J2J', label:'Flink Streaming SQL (Json to Json)'},
                                {value:'TRANSFORM_FLINK_SCRIPT', label:'Flink Table API'},
                                {value:'TRANSFORM_FLINK_UDF', label:'Flink User Defined Function'}]).label('Transforms Type'),
        nga.field('udfUpload', 'file').label('Upload Jar').uploadInformation({ 'url': 'http://localhost:8080/api/df/uploaded_files', 'method': 'POST', 'apifilename': 'uploaded_file_name' })
        .defaultValue('empty.jar')
        .validation({ validator: function(value) {
                if (value.indexOf('.jar') == -1) throw new Error ('Invalid .jar file!');
        } })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_FLINK_UDF\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('status').editable(false).label('Task Status'),
        nga.field('description', 'text'),
        //nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig_1','json').label('Transform Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "trans.jar" : "test_flink_udf.jar /* The name of UDF Jar file uploaded */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_FLINK_UDF\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig_2','json').label('Transforms Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "group.id" : "fink_table /* Kafka consumer id. */",
        "schema.subject" : "test-value /* The subject name for the schema */",
        "topic.for.query" : "stock /* The Kafka topic to query data */",
        "topic.for.result" : "output /* The Kafka topic to output data */",
        "trans.script" : "select(\"name\") /* The Flink Stream Table API */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_FLINK_SCRIPT\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig_3','json').label('Transforms Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "group.id" : "fink_sql /* Kafka consumer id. */",
        "schema.subject" : "test-value /* The subject name for the schema */",
        "topic.for.query" : "stock /* The Kafka topic to query data */",
        "topic.for.result" : "output /* The Kafka topic to output data */",
        "trans.sql" : "SELECT STREAM name, symbol from stock /* The Flink Stream SQL query.*/"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_FLINK_SQL_A2J\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig_4','json').label('Transforms Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "group.id" : "fink_sql /* Kafka consumer id. */",
        "column.name.list" : "name, symbol /* The list of Json column names output to Kafka topic.*/",
        "column.schema.list" : "string, string /* The list of data type for Json data */",
        "topic.for.query" : "stock /* The Kafka topic to query data */",
        "topic.for.result" : "output /* The Kafka topic to output data */",
        "trans.sql" : "SELECT STREAM name, symbol from stock /* The Flink Stream SQL query.*/"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_FLINK_SQL_J2J\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig','json').label('Transforms Config')
		.defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
		"group.id" : "Kafka consumer id, optional.",
		"data.format.input" : "json_string /* Can be json_string or json */",
		"data.format.output" : "json_string /* Can be json_string or json */",
		"avro.schema.enabled" : "false /* Whether AVRO schema is enabled in Kafka Connect.*/",
        "column.name.list" : "name, symbol /* The list of Json column names output to Kafka topic.*/",
        "column.schema.list" : "string, string /* The list of data type for Json data */",
        "topic.for.query" : "stock /* The Kafka topic to query data */",
        "topic.for.result" : "output /* The Kafka topic to output data */",
        "trans.sql" : "SELECT STREAM name, symbol from stock /* The Flink Stream SQL query.*/"
		})
		.template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_FLINK_SQL_GENE\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true)

    ]);

	transform.editionView().fields([
        nga.field('taskSeq', 'number').label('Task ID').editable(false),
        nga.field('name').label('Job Name').editable(false),
        nga.field('connectUid').label('Transform ID').editable(false),
        nga.field('connectorType').editable(false),
        nga.field('udfUpload', 'file').label('Upload Jar').uploadInformation({ 'url': 'http://localhost:8080/api/df/uploaded_files', 'method': 'POST', 'apifilename': 'uploaded_file_name' })
        .defaultValue('empty.jar')
        .validation({ validator: function(value) {
                if (value.indexOf('.jar') == -1) throw new Error ('Invalid .jar file!');
        } })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_FLINK_UDF\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('status').editable(false).label('Task Status'),
        nga.field('description', 'text'),
        //nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig','json').label('Transform Config')
    ]);

    schema.creationView().fields([
        nga.field('id', 'number').format('0o').label('Schema ID'),
        nga.field('subject').label('Subject'),
        nga.field('version').label('Schema Version'),
        nga.field('schema', 'json').label('Schema'),
        nga.field('compatibility', 'choice').choices([
                                {value:'NONE', label:'NONE'},
                                {value:'FULL', label:'FULL'},
                                {value:'FORWARD', label:'FORWARD'},
                                {value:'BACKWARD', label:'BACKWARD'}
                                ]).label('Compatibility')
    ]);

    schema.editionView().fields([
                                nga.field('id', 'number').editable(false).label('Schema ID').isDetailLink(false),
                                nga.field('subject').editable(false).label('Subject'),
                                nga.field('version').editable(false).label('Schema Version'),
                                nga.field('schema', 'json').label('Schema'),
                                nga.field('compatibility', 'choice').choices([
                                                        {value:'NONE', label:'NONE'},
                                                        {value:'FULL', label:'FULL'},
                                                        {value:'FORWARD', label:'FORWARD'},
                                                        {value:'BACKWARD', label:'BACKWARD'}
                                                        ]).label('Compatibility')
    ]);
	// TODO populate default value for each type of transforms/connects as template
    // use the same fields for the editionView as for the creationView

	schema.editionView().actions(['list']);

	// set the fields of the processor entity list view
    processor.listView().sortField('name').fields([
        nga.field('id').label('Task ID').isDetailLink(true),
        nga.field('taskSeq', 'number').format('0o').label('Task Seq'),
        nga.field('name').label('Task Name'),
        nga.field('connectUid').label('Connect or Transform UID'),
        nga.field('connectorType').label('Type'),
		nga.field('connectorCategory').label('Category'),
        nga.field('status').label('Task Status')
    ]);
    processor.listView().title('All Connects and Transforms');
    processor.listView().batchActions([]);

    // set the fields of the connect entity list view // TODO make sort work
    installed_connects.listView().fields([
        nga.field('name'),
        nga.field('type'),
        nga.field('subtype'),
        nga.field('certified'),
        nga.field('class')
    ]).sortField('name').sortDir('DESC');
    installed_connects.listView().title('Installed');
    installed_connects.listView().batchActions([]);

    // add the connect entity to the admin application
    admin.addEntity(processor).addEntity(connect).addEntity(transform).addEntity(installed_connects).addEntity(schema)
    .addEntity(process_history);

	// customize menubar
	admin.menu(nga.menu()
      .addChild(nga.menu(processor).icon('<span class="fa fa-globe fa-fw"></span>'))
      .addChild(nga.menu(connect).icon('<span class="fa fa-plug fa-fw"></span>'))
      .addChild(nga.menu(transform).icon('<span class="fa fa-flask fa-fw"></span>'))
      .addChild(nga.menu(schema).icon('<span class="fa fa-scribd fa-fw"></span>'))
      .addChild(nga.menu(installed_connects).icon('<span class="fa fa-cog fa-fw"></span>'))
      .addChild(nga.menu(process_history).icon('<span class="fa fa-history fa-fw"></span>'))
);
    // attach the admin application to the DOM and execute it
    nga.configure(admin);
}]);
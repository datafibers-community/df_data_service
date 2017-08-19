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
	var schema = nga.entity('schema').identifier(nga.field('subject')).label('TOPICS');
    var installed_connects = nga.entity('config').identifier(nga.field('class')).label('INSTALLED').readOnly();
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
  	    nga.field('subject').label('Topic Name').isDetailLink(true),
        nga.field('id').label('Topic ID'),
        nga.field('version').label('Topic Schema Version'),
        nga.field('schema').label('Topic Schema'),
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
        nga.field('last_modified_timestamp').label('Time Updated'),
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
        nga.field('connectorType', 'choice')
                .choices([
                                {value:'CONNECT_SOURCE_KAFKA_AvroFile', label:'Source Avro File'},
                                {value:'CONNECT_SINK_HDFS_AvroFile', label:'Sink Hadoop|Hive'},
                                {value:'CONNECT_SINK_MONGODB_AvroDB', label:'Sink MongoDB'}
                         ]).label('Connect Type'),
        nga.field('status').editable(false).label('Task Status'),
        nga.field('description', 'text'),
        nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig','json').label('Connect Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "connector_class" : "com.datafibers.kafka.connect.FileGenericSourceConnector",
        "file_location" : "/home/vagrant/df_data/ /* Folder where to read the files. */",
        "file_glob" : "*.{json,csv} /* File glob to filter file */",
        "file_overwrite" : "true /* Whether over-written file will re-extract */",
        "schema_registry_uri" : "http://localhost:8081",
        "tasks_max" : "1 /* Number of tasks in parallel. */",
        "topic" : "stock /* The single Kafka topic name having data streamed. */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'CONNECT_SOURCE_KAFKA_AvroFile\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig_1','json').label('Connect Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "connector_class" : "io.confluent.connect.hdfs.HdfsSinkConnector",
        "schema_compatibility" : "BACKWARD",
        "hdfs_url" : "hdfs://localhost:8020",
        "hive_metastore_uris" : "thrift://localhost:9083",
        "hive_integration" : "true /* Whether create Hive tables */",
        "tasks_max" : "1 /* Number of tasks in parallel. */",
        "flush_size" : "1 /* Number of rows to flush to HDFS. */",
        "topics" : "stock, test /* The Kafka topic names having data to sink. */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'CONNECT_SINK_HDFS_AvroFile\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
		nga.field('connectorConfig_2','json').label('Connect Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "connector_class" : "org.apache.kafka.connect.mongodb.MongodbSinkConnector",
        "host" : "localhost /* Hostname of MongoDB */",
        "port" : "27017 /* Port number of MongoDB */",
        "mongodb_database" : "DEFAULT_DB /* Mongo database name where to sink the data */",
        "mongodb_collections" : "df_test /* Mongo collection name where ro sink the data */",
        "tasks_max" : "1 /* Number of tasks in parallel. */",
        "bulk_size" : "1 /* Number of rows to flush to Mongo. */",
        "topics" : "stock, test /* The Kafka topic names having data to sink. */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'CONNECT_SINK_MONGODB_AvroDB\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true)
    ]);

    connect.editionView().fields([
        nga.field('taskSeq', 'number').label('Task Seq').editable(false),
        nga.field('name').label('Task Name').editable(false),
        nga.field('connectUid').label('Connect UID').editable(false),
        nga.field('connectorType').editable(false),
        nga.field('status').editable(false).label('Task Status'),
        nga.field('description', 'text'),
        nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig','json').label('Connect Config')
    ]);

	transform.creationView().fields([
        nga.field('taskSeq', 'number').label('Task Seq'),
        nga.field('name').label('Task Name'),
        nga.field('connectorType', 'choice')
                .choices([
                                {value:'TRANSFORM_EXCHANGE_FLINK_SQLA2A', label:'Flink Streaming SQL'},
                                {value:'TRANSFORM_EXCHANGE_FLINK_Script', label:'Flink Table API'},
                                {value:'TRANSFORM_EXCHANGE_FLINK_UDF', label:'Flink User Defined Function'}]).label('Transforms Type'),
        nga.field('udfUpload', 'file').label('Upload Jar').uploadInformation({ 'url': 'http://localhost:8080/api/df/uploaded_files', 'method': 'POST', 'apifilename': 'uploaded_file_name' })
        .defaultValue('empty.jar')
        .validation({ validator: function(value) {
                if (value.indexOf('.jar') == -1) throw new Error ('Invalid .jar file!');
        } })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_FLINK_UDF\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('status').editable(false).label('Task Status'),
        nga.field('description', 'text'),
        nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig','json').label('Transform Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "trans_jar" : "test_flink_udf.jar /* The name of UDF Jar file uploaded */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_EXCHANGE_FLINK_UDF\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig_1','json').label('Transforms Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "group_id" : "fink_table /* Kafka consumer id. */",
        "topic_in" : "stock /* The Kafka topic to query data */",
        "topic_out" : "output /* The Kafka topic to output data */",
        "trans_script" : "select(\"name\") /* The Flink Stream Table API */"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_EXCHANGE_FLINK_Script\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig_2','json').label('Transforms Config')
        .defaultValue({
		"config_ignored" : "remove this template marker to submit, /* this is comments */",
        "group_id" : "fink_sql /* Kafka consumer id. */",
        "topic_in" : "stock /* The Kafka topic to query data */",
        "topic_out" : "output /* The Kafka topic to output data */",
        "sink_key_fields":"name /* List of commas separated columns for keys in sink */",
        "trans_sql" : "SELECT name, symbol from stock /* The Flink Stream SQL query.*/"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'TRANSFORM_EXCHANGE_FLINK_SQLA2A\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true)
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
        nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig','json').label('Transform Config')
    ]);

    schema.creationView().fields([
        nga.field('id').label('Topic Name'),
        nga.field('version').label('Topic Schema Version'),
        nga.field('schema', 'json').label('Topic Schema'),
        nga.field('compatibility', 'choice').choices([
                                {value:'NONE', label:'NONE'},
                                {value:'FULL', label:'FULL'},
                                {value:'FORWARD', label:'FORWARD'},
                                {value:'BACKWARD', label:'BACKWARD'}
                                ]).label('Schema Compatibility')
    ]);

    schema.editionView().fields([
                                nga.field('id', 'number').editable(false).label('Topic ID').isDetailLink(false),
                                nga.field('subject').editable(false).label('Topic Name'),
                                nga.field('version').editable(false).label('Topic Schema Version'),
                                nga.field('schema', 'json').label('Topic Schema'),
                                nga.field('compatibility', 'choice').choices([
                                                        {value:'NONE', label:'NONE'},
                                                        {value:'FULL', label:'FULL'},
                                                        {value:'FORWARD', label:'FORWARD'},
                                                        {value:'BACKWARD', label:'BACKWARD'}
                                                        ]).label('Schema Compatibility')
    ]);

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

    // set the fields of the connect entity list view
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

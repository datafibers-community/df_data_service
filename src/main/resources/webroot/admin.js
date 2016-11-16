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
    var producer = nga.entity('ps').label('CONNECTS');
	var transformer = nga.entity('tr').label('TRANSFORMS');
    var installed_connects = nga.entity('installed_connects').identifier(nga.field('class')).label('INSTALLED').readOnly();

    // set the fields of the producer entity list view
    producer.listView().sortField('name').fields([
        nga.field('id').label('Job ID').isDetailLink(true),
        nga.field('taskId', 'number').format('0o').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').label('Processor'),
        nga.field('connectorType').label('Type'),
        nga.field('status').label('Job Status')
    ]);

    // set the fields of the transformer entity list view
    transformer.listView().sortField('name').fields([
        nga.field('id').label('Job ID').isDetailLink(true),
        nga.field('taskId', 'number').format('0o').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').label('Processor'),
        nga.field('connectorType').label('Type'),
        nga.field('status').label('Job Status')
    ]);

    producer.listView().title('Connects Dashboard');
    transformer.listView().title('Transforms Dashboard');

    producer.creationView().fields([
        nga.field('taskId', 'number').format('0o').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').attributes({placeholder:'No space allowed and 5 chars min.'}).validation({ required: true, pattern: '[A-Za-z0-9\-]{5,20}' }).label('Connects'),
        nga.field('connectorType', 'choice')
                .choices([
                                {value:'KAFKA_SOURCE', label:'Kafka Connect Source'},
                                {value:'KAFKA_SINK', label:'Kafka Connect Sink'}]).label('Connector Type'),
        nga.field('status').editable(false).label('Job Status'),
        nga.field('description', 'text'),
        nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig','json').label('Connects Config')
		.defaultValue({
		"config_ignored":"template marker, remove it to make config effective",
		"connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
		"file": "File name for streaming to Kafka.",
		"tasks.max": "Number of tasks in parallel.",
		"name": "Kafka Connect name.",
		"topic": "The single Kafka topic name having data streamed."
		})
		.template('<ma-field ng-if="entry.values.connectorType == \'KAFKA_SOURCE\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
		nga.field('connectorConfig_1','json').label('Connects Config')
		.defaultValue({
		"config_ignored":"template marker, remove it to make config effective",
		"connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
		"file": "File name to keep the data exported from Kafka.",
		"tasks.max": "Number of tasks in parallel.",
		"name": "Kafka Connect name.",
		"topics": "List of Kafka topics having data streamed out"
		})
		.template('<ma-field ng-if="entry.values.connectorType == \'KAFKA_SINK\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true)
    ]);

	transformer.creationView().fields([
        nga.field('taskId', 'number').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').attributes({placeholder:'No space allowed and 5 chars min.'}).validation({ required: true, pattern: '[A-Za-z0-9\-]{5,20}' }).label('Transforms'),
        nga.field('connectorType', 'choice')
                .choices([
                                {value:'FLINK_TRANS', label:'Flink Streaming SQL (Json|Json String)'},
                                {value:'FLINK_SQL_A2J', label:'Flink Streaming SQL (Avro to Json)'},
                                {value:'FLINK_SQL_J2J', label:'Flink Streaming SQL (Json to Json)'},
                                {value:'FLINK_UDF', label:'Flink User Defined Function'}]).label('Transforms Type'),
        nga.field('udfUpload', 'file').label('Upload Jar').uploadInformation({ 'url': 'http://localhost:8080/api/df/uploaded_files', 'method': 'POST', 'apifilename': 'uploaded_file_name' })
        .defaultValue('empty.jar')
        .validation({ validator: function(value) {
                if (value.indexOf('.jar') == -1) throw new Error ('Invalid .jar file!');
        } })
        .template('<ma-field ng-if="entry.values.connectorType == \'FLINK_UDF\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('status').editable(false).label('Job Status'),
        nga.field('description', 'text'),
        nga.field('jobConfig','json').defaultValue({}).label('Job Config'),
        nga.field('connectorConfig_1','json').label('Transforms Config')
        .defaultValue({
        "config_ignored":"template marker, remove it to make config effective",
        "group.id":"Kafka consumer id.",
        "data.format.input":"json_string|json",
        "data.format.output":"json_string|json",
        "avro.schema.enabled":"Whether AVRO schema is enabled in Kafka Connect.",
        "topic.for.query":"The Kafka topic to query data",
        "topic.for.result":"The Kafka topic to output data",
        "trans.jar":"The name of UDF Jar file uploaded"
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'FLINK_UDF\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig_2','json').label('Transforms Config')
        .defaultValue({
        "config_ignored":"template marker, remove it to make config effective",
        "group.id":"Kafka consumer id.",
        "schema.subject":"The subject name for the schema",
        "static.avro.schema":"The schema string as optional",
        "topic.for.query":"The Kafka topic to query data",
        "topic.for.result":"The Kafka topic to output data",
        "trans.sql":"The Flink Stream SQL query."
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'FLINK_SQL_A2J\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig_3','json').label('Transforms Config')
        .defaultValue({
        "config_ignored":"template marker, remove it to make config effective",
        "group.id":"Kafka consumer id, optional.",
        "schema.subject":"The subject name for the schema",
        "column.name.list":"The list of Json column names output to Kafka topic.",
        "column.schema.list":"The list of data type for Json data, such as string,string",
        "topic.for.query":"The Kafka topic to query data",
        "topic.for.result":"The Kafka topic to output data",
        "trans.sql":"The Flink Stream SQL query."
        })
        .template('<ma-field ng-if="entry.values.connectorType == \'FLINK_SQL_J2J\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true),
        nga.field('connectorConfig','json').label('Transforms Config')
		.defaultValue({
		"config_ignored":"template marker, remove it to make config effective",
		"group.id":"Kafka consumer id, optional.",
		"data.format.input":"json_string|json",
		"data.format.output":"json_string|json",
		"avro.schema.enabled":"Whether AVRO schema is enabled in Kafka Connect.",
		"column.name.list":"The list of Json column names output to Kafka topic.",
		"column.schema.list":"The list of data type for Json data, such as string,string",
		"topic.for.query":"The Kafka topic to query data",
		"topic.for.result":"The Kafka topic to output data",
		"trans.sql":"The Flink Stream SQL query."
		})
		.template('<ma-field ng-if="entry.values.connectorType == \'FLINK_TRANS\'" field="::field" value="entry.values[field.name()]" entry="entry" entity="::entity" form="formController.form" datastore="::formController.dataStore"></ma-field>', true)

    ]);

	// TODO populate default value for each type of transforms/connects as template
    // use the same fields for the editionView as for the creationView
    producer.editionView().fields(producer.creationView().fields());
	transformer.editionView().fields(transformer.creationView().fields());

	// set the fields of the proceesor entity list view
    processor.listView().sortField('name').fields([
        nga.field('id').label('Job ID').isDetailLink(true),
        nga.field('taskId', 'number').format('0o').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').label('Processor'),
        nga.field('connectorType').label('Type'),
		nga.field('connectorCategory').label('Category'),
        nga.field('status').label('Job Status')
    ]);
    processor.listView().title('All Connects and Transforms');
    processor.listView().batchActions([])

    // set the fields of the producer entity list view
    installed_connects.listView().sortField('class').fields([
        nga.field('class').label('Connects')
    ]);
    installed_connects.listView().title('Connects Installed');
    installed_connects.listView().batchActions([])

    // add the producer entity to the admin application
    admin.addEntity(processor).addEntity(producer).addEntity(transformer).addEntity(installed_connects);

	// customize menubar
	admin.menu(nga.menu()
  .addChild(nga.menu(processor).icon('<span class="fa fa-globe fa-fw"></span>'))
  .addChild(nga.menu(producer).icon('<span class="fa fa-plug fa-fw"></span>'))
  .addChild(nga.menu(transformer).icon('<span class="fa fa-flask fa-fw"></span>'))
  .addChild(nga.menu(installed_connects).icon('<span class="fa fa-cog fa-fw"></span>'))
);
    // attach the admin application to the DOM and execute it
    nga.configure(admin);
}]);
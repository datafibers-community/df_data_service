// in src/Connects.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { Datagrid, SelectField, UrlField, FunctionField, ChipField, TextField, DateField, RichTextField, ImageField, ReferenceField, ReferenceArrayField, ReferenceManyField, SingleFieldList, SelectArrayInput } from 'admin-on-rest';
import { AutocompleteInput, RadioButtonGroupInput, NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput, ReferenceInput, ReferenceArrayInput } from 'admin-on-rest';
import { EditButton, ShowButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/image/flare';
import { required, minLength, maxLength, minValue, maxValue, number, regex, email, choices } from 'admin-on-rest';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';
import EmbeddedArrayInputFormField from '../component/EmbeddedArrayInputFormField';
import ApproveButton from './ApproveButton';
import ReviewEditActions from './ReviewEditActions';
import rowStyle from './rowStyle';
import RefreshListActions from '../buttons/RefreshListActions'

export const ConnectIcon = Icon;

const ConnectShowTitle = ({ record }) => {
    return <span>Raw Json with ID. {record ? `"${record.id}"` : ''}</span>;
};

const ConnectTitle = ({ record }) => {
    return <span>ID. {record ? `"${record.id}"` : ''}</span>;
};

const ConnectFilter = (props) => (
    <Filter {...props}>
        <TextInput label="Search" source="q" alwaysOn />
        <TextInput label="Status" source="status" defaultValue="RUNNING" />
    </Filter>
);

export const ConnectShow = (props) => (
    <Show title={<ConnectShowTitle />} {...props}>
        <SimpleShowLayout>
	    <RawJsonRecordField />
        </SimpleShowLayout>
    </Show>
);

export const ConnectList = (props) => (
    <List {...props} title="Connect List" filters={<ConnectFilter />} actions={<RefreshListActions refreshInterval="5000" />}>
        <Datagrid rowStyle={rowStyle}
            headerOptions={{ adjustForCheckbox: true, displaySelectAll: true }}
            bodyOptions={{ displayRowCheckbox: true, stripedRows: false, showRowHover: true}}
            rowOptions={{ selectable: true }}
            options={{ multiSelectable: true }} >
            <TextField source="id" label="id" />
            <TextField source="taskSeq" label="task seq." />
            <TextField source="name" label="name" />
            <TextField source="connectorType" label="task type" />
            <ChipField source="status" label="status" />
            <ApproveButton style={{ padding: 0 }} />
            <EditButton />
        </Datagrid>
    </List>
);

export const ConnectEdit = (props) => (
    <Edit title={<ConnectTitle />} {...props} actions={<ReviewEditActions />}>
        <TabbedForm>
            <FormTab label="Overview">
                <DisabledInput source="taskSeq" label="Task Sequence" style={{ display: 'inline-block' }} />
                <ChipField source="status" label="Task Status" style={{ display: 'inline-block', marginLeft: 32, width: 150 }} />
                <TextInput source="name" label="Name" validate={[ required ]} style={{ width: 500 }} />
                <LongTextInput source="description" label="Task Description" style={{ width: 500 }} />
                <SelectField source="connectorType" label="Task Type" validate={[ required ]} choices={[
                        { id: 'CONNECT_SOURCE_KAFKA_AvroFile', name: 'Source Avro Files' },
                        { id: 'CONNECT_SOURCE_STOCK_AvroFile', name: 'Source Stock API' },
                        { id: 'CONNECT_SINK_HDFS_AvroFile', name: 'Sink Hadoop|Hive' },
                        { id: 'CONNECT_SINK_MONGODB_AvroDB',  name: 'Sink MongoDB' },
                        { id: 'CONNECT_SINK_KAFKA_JDBC',  name: 'Sink JDBC' },
                ]} />
                <NumberInput source="connectorConfig.tasks_max" label="Number of Sub-task to Submit" step={1} validate={[ number, minValue(1) ]}/>
            </FormTab>
            <FormTab label="Setting">
                <DisabledInput source="connectorConfig.cuid" label="ID or CUID or Name"/>
                <TextField source="connectorConfig.connector_class" label="Connect Class Library" style={{ maxWidth: 200 }} />
                <DependentInput dependsOn="connectorType" value="CONNECT_SOURCE_KAFKA_AvroFile">
                    <ReferenceInput source="connectorConfig.topic" label="Choose a topic to write data" reference="schema" validate={[ required ]} allowEmpty>
                        <AutocompleteInput optionText="subject" />
                    </ReferenceInput>
                    <BooleanInput source="connectorConfig.file_overwrite" label="Allow File Overwrite?" />
                    <TextInput source="connectorConfig.file_location" label="Path where to load the files" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.file_glob" label="Pattern/Glob to match the files" style={{ display: 'inline-block' }} validate={[ required ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_SOURCE_STOCK_AvroFile">
                    <TextInput source="connectorConfig.topic" label="Edit a topic to write data" validate={[ required ]} />
                    <SelectInput source="connectorConfig.portfolio" label="Portfolio, selected symbols list" validate={[ required ]} choices={[
                            { id: 'None', name: 'Input Symbols' },
                            { id: 'Top 10 IT Service', name: 'Top 10 IT Service' },
                            { id: 'Top 10 Technology', name: 'Top 10 Technology' },
                            { id: 'Top 10 US Banks', name: 'Top 10 US Banks' },
                            { id: 'Top 10 US Telecom', name: 'Top 10 US Telecom' },
                            { id: 'Top 10 Life Insurance', name: 'Top 10 Life Insurance' },
                    ]} />
                    <DependentInput dependsOn="connectorConfig.portfolio" value="None">
                        <LongTextInput source="connectorConfig.symbols" label="List of Stock Symbols (separated by ,)" style={{ width: 500 }}/>
                    </DependentInput>
                    <NumberInput source="connectorConfig.interval" label="API Refresh Interval (sec.)" defaultValue={5} step={5} validate={[ required, minValue(5) ]} />
                    <SelectInput source="connectorConfig.spoof" label="Use Spoofing Data?" validate={[ required ]} choices={[
                                                { id: 'NONE', name: 'No Spoofing' },
                                                { id: 'PAST', name: 'Spoof from Past Market Data' },
                                                { id: 'OTHER', name: 'Spoof from Random Data' },
                    ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_SINK_MONGODB_AvroDB">
                    <LongTextInput source="connectorConfig.topics" label="Topics to sink data from (use , separate multiple values" />
                    <TextInput source="connectorConfig.host" label="Server Hostname" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.port" label="Server Port" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <TextInput source="connectorConfig.mongodb_database" label="Database Name" />
                    <LongTextInput source="connectorConfig.mongodb_collections" label="Collections where to sink the files (use , separate multiple values)" style={{ width: 500 }} />
                    <NumberInput source="connectorConfig.bulk_size" label="Bulk size of rows to sink" step={1} validate={[ required, number, minValue(1) ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_SINK_HDFS_AvroFile">
                    <LongTextInput source="connectorConfig.topics" label="Topics to Sink Data From (use , separate multiple values" style={{ width: 500 }}/>
                    <BooleanInput source="connectorConfig.hive_integration" label="Enable Hive Metadata" />
                    <DependentInput dependsOn="connectorConfig.hive_integration" value={true}>
                        <SelectInput source="connectorConfig.schema_compatibility" label="Schema Compatibility" validate={[ required ]} choices={[
                                                                        { id: 'BACKWARD', name: 'BACKWARD' },
                                                                        { id: 'FORWARD', name: 'FORWARD' },
                                                                        { id: 'FULL', name: 'FULL' },
                                            ]} />
                        <DisabledInput source="connectorConfig.hive_metastore_uris" label="Hive Metastore URL" />
                    </DependentInput>
                    <DisabledInput source="connectorConfig.hdfs_url" label="HDFS URL" />
                    <NumberInput source="connectorConfig.flush_size" label="Bulk size of rows to sink" step={1} validate={[ required, minValue(1) ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_SINK_KAFKA_JDBC">
                    <LongTextInput source="connectorConfig.topics" label="Topics to sink data from (use , separate multiple values" />
                    <LongTextInput source="connectorConfig.connection_url" label="JDBC_URL, such as jdbc:mysql://localhost:3306/db_name" style={{ width: 500 }} validate={[ required ]} />
                    <BooleanInput source="connectorConfig.auto_create" label="Auto Create Table ?" defaultValue={true} />
                    <NumberInput source="connectorConfig.bulk_size" label="Bulk size of rows to sink" defaultValue="10" step={1} validate={[ required, minValue(1) ]} />
                </DependentInput>
            </FormTab>
            <FormTab label="State">
                <ReferenceManyField addLabel={false} reference="status" target="id">
                    <Datagrid>
                        <TextField source="jobId" label="Engine Job ID." />
                        <ChipField source="taskState" label="Engine Job State" />
                        <TextField source="subTaskId" label="Subtask ID." />
                        <ChipField source="state" label="Subtask State" />
                        <TextField source="trace" label="Trace" />
                    </Datagrid>
                </ReferenceManyField>
            </FormTab>
        </TabbedForm>
    </Edit>
);

export const ConnectCreate = (props) => (
    <Create title="Create New Connect Task Guide" {...props}>
        <TabbedForm>
            <FormTab label="Overview">
                <NumberInput source="taskSeq" label="Task Sequence Number, eg. 1, 2, ..." defaultValue={1} step={1}/>
                <LongTextInput source="name" label="Task Name" validate={[ required ]} style={{ width: 500 }} />
                <LongTextInput source="description" label="Task Description" defaultValue="This is default description." style={{ width: 500 }} />
                <RadioButtonGroupInput source="connectorCategory" label="Category" choices={[
                    { id: 'source', name: 'Source' },
                    { id: 'sink', name: 'Sink' },
                ]} defaultValue="source"   />
                <DependentInput dependsOn="connectorCategory" value="source" >
                    <SelectInput source="connectorType" label="Task Type" validate={[ required ]} choices={[
                            { id: 'CONNECT_SOURCE_KAFKA_AvroFile', name: 'Avro Files' },
                            { id: 'CONNECT_SOURCE_STOCK_AvroFile', name: 'Stock API' },
                    ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorCategory" value="sink">
                    <SelectInput source="connectorType" label="Task Type" validate={[ required ]} choices={[
                            { id: 'CONNECT_SINK_HDFS_AvroFile', name: 'Hadoop|Hive' },
                            { id: 'CONNECT_SINK_MONGODB_AvroDB',  name: 'MongoDB' },
                            { id: 'CONNECT_SINK_KAFKA_JDBC',  name: 'JDBC' },
                    ]} />
                </DependentInput>
                <NumberInput source="connectorConfig.tasks_max" label="Number of sub-task to submit" defaultValue={1} step={1} validate={[ number, minValue(1) ]} />
            </FormTab>
            <FormTab label="Setting">
                <DependentInput dependsOn="connectorType" value="CONNECT_SOURCE_KAFKA_AvroFile">
                    <ReferenceInput source="connectorConfig.topic" label="Choose a topic to write data" reference="schema" validate={[ required ]} allowEmpty>
                        <AutocompleteInput optionText="subject" />
                    </ReferenceInput>
                    <LongTextInput source="connectorConfig.schema_registry_uri" label="Schema Registry URI" defaultValue="http://localhost:8081" validate={[ required ]} style={{ width: 500 }} />
                    <BooleanInput source="connectorConfig.file_overwrite" label="Allow File Overwrite ?" defaultValue={true} />
                    <TextInput source="connectorConfig.file_location" label="Path where to load the files" style={{ display: 'inline-block' }} defaultValue={"/home/vagrant/df_data"} validate={[ required ]} />
                    <TextInput source="connectorConfig.file_glob" label="Pattern/Glob to match the files" style={{ display: 'inline-block', marginLeft: 32 }} defaultValue="*.{json,csv}" validate={[ required ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_SOURCE_STOCK_AvroFile">
                    <LongTextInput source="connectorConfig.topic" label="Automatically create a topic to write data" validate={[ required ]} style={{ width: 500 }} />
                    <LongTextInput source="connectorConfig.schema_registry_uri" label="Schema Registry URI" defaultValue="http://localhost:8081" validate={[ required ]} style={{ width: 500 }} />
                    <SelectInput source="connectorConfig.portfolio" label="Portfolio, selected symbols list" validate={[ required ]} choices={[
                            { id: 'None', name: 'Input Symbols' },
                            { id: 'Top 10 IT Service', name: 'Top 10 IT Service' },
                            { id: 'Top 10 Technology', name: 'Top 10 Technology' },
                            { id: 'Top 10 US Banks', name: 'Top 10 US Banks' },
                            { id: 'Top 10 US Telecom', name: 'Top 10 US Telecom' },
                            { id: 'Top 10 Life Insurance', name: 'Top 10 Life Insurance' },
                    ]} />
                    <DependentInput dependsOn="connectorConfig.portfolio" value="None">
                        <LongTextInput source="connectorConfig.symbols" label="List of Stock Symbols (separated by ,)" style={{ width: 500 }}/>
                    </DependentInput>
                    <NumberInput source="connectorConfig.interval" label="API Refresh Interval (sec.)" style={{ display: 'inline-block' }} defaultValue={5} step={5} validate={[ required, minValue(5) ]} />
                    <SelectInput source="connectorConfig.spoof" label="Use Spoofing Data?" validate={[ required ]} choices={[
                                                { id: 'NONE', name: 'No Spoofing' },
                                                { id: 'PAST', name: 'Spoof from Past Market Data' },
                                                { id: 'OTHER', name: 'Spoof from Random Data' },
                    ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_SINK_MONGODB_AvroDB">
                    <ReferenceArrayInput source="connectorConfig.topics" label="Choose topics to write data" reference="schema" validate={[ required ]} allowEmpty>
                        <SelectArrayInput optionText="subject" />
                    </ReferenceArrayInput>
                    <TextInput source="connectorConfig.host" label="Server Hostname" defaultValue="localhost" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.port" label="Server Port" defaultValue="27017" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <TextInput source="connectorConfig.mongodb_database" label="Database Name" defaultValue="DEFAULT_DB" validate={[ required ]} />
                    <LongTextInput source="connectorConfig.mongodb_collections" label="Collections where to sink the files (use , separate multiple values)" style={{ width: 500 }} validate={[ required ]} />
                    <NumberInput source="connectorConfig.bulk_size" label="Bulk size of rows to sink" defaultValue="1" step={1} validate={[ required, minValue(1) ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_SINK_HDFS_AvroFile">
                    <ReferenceArrayInput source="connectorConfig.topics" label="Choose topics to write data" reference="schema" validate={[ required ]} allowEmpty>
                        <SelectArrayInput optionText="subject" />
                    </ReferenceArrayInput>
                    <BooleanInput source="connectorConfig.hive_integration" label="Enable Hive Metadata" defaultValue={true} validate={[ required ]} />
                    <DependentInput dependsOn="connectorConfig.hive_integration" value={true}>
                    <LongTextInput source="connectorConfig.hive_metastore_uris" label="Hive Metastore URL" defaultValue="thrift://localhost:9083" style={{ width: 500 }} validate={[ required ]} />
                    <SelectInput source="connectorConfig.schema_compatibility" label="Schema Compatibility" validate={[ required ]} choices={[
                                                                    { id: 'BACKWARD', name: 'BACKWARD' },
                                                                    { id: 'FORWARD', name: 'FORWARD' },
                                                                    { id: 'FULL', name: 'FULL' },
                                        ]} />
                    </DependentInput>
                    <LongTextInput source="connectorConfig.hdfs_url" label="HDFS URL" defaultValue="hdfs://localhost:9000" style={{ width: 500 }} validate={[ required ]} />
                    <NumberInput source="connectorConfig.flush_size" label="Bulk size of rows to sink" defaultValue="10" step={1} validate={[ required, minValue(1) ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_SINK_KAFKA_JDBC">
                    <ReferenceArrayInput source="connectorConfig.topics" label="Choose topics to write data" reference="schema" style={{ width: 500 }} validate={[ required ]} allowEmpty>
                        <SelectArrayInput optionText="subject" />
                    </ReferenceArrayInput>
                    <LongTextInput source="connectorConfig.connection_url" label="JDBC_URL" defaultValue="jdbc:mysql://localhost:3306/DB_NAME" style={{ width: 500 }} validate={[ required ]} />
                    <TextInput source="connectorConfig.connection_user" label="User Name" defaultValue="root" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.connection_password" label="Password" type="password" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <BooleanInput source="connectorConfig.auto_create" label="Auto Create Table ?" defaultValue={true} />
                    <NumberInput source="connectorConfig.bulk_size" label="Bulk size of rows to sink" defaultValue="10" step={1} validate={[ required, minValue(1) ]} />
                </DependentInput>
            </FormTab>
        </TabbedForm>
    </Create>
);


// in src/Connects.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { Datagrid, SelectField, UrlField, FunctionField, ChipField, TextField, DateField, RichTextField, ImageField, ReferenceField, ReferenceManyField, SingleFieldList } from 'admin-on-rest';
import { NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput } from 'admin-on-rest';
import { EditButton, ShowButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/action/settings-input-component';

export const ConnectIcon = Icon;

const RawRecordField = ({ record, source }) => <pre dangerouslySetInnerHTML={{ __html: JSON.stringify(record, null, '\t')}}></pre>;
RawRecordField.defaultProps = { label: 'Raw Json' };

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
	    <RawRecordField />
        </SimpleShowLayout>
    </Show>
);

export const ConnectList = (props) => (
    <List {...props} title="Connect List" filters={<ConnectFilter />}>
        <Datagrid
            headerOptions={{ adjustForCheckbox: true, displaySelectAll: true }}
            bodyOptions={{ displayRowCheckbox: true, stripedRows: true, showRowHover: true}}
            rowOptions={{ selectable: true }}
            options={{ multiSelectable: true }}>
            <TextField source="id" label="id" />
            <TextField source="taskSeq" label="task seq." />
            <TextField source="name" label="name" />
            <TextField source="connectorType" label="task type" />
            <ChipField source="status" label="status" />
            <EditButton />
        </Datagrid>
    </List>
);

export const ConnectEdit = (props) => (
    <Edit title={<ConnectTitle />} {...props}>
        <TabbedForm>
            <FormTab label="Overview">
                <DisabledInput source="taskSeq" label="Task Sequence" />
                <TextInput source="name" label="Name" />
		        <SelectField source="connectorType" label="Task Type" choices={[
    			{ id: 'CONNECT_KAFKA_SOURCE_AVRO', name: 'Source Avro Files' },
    			{ id: 'CONNECT_KAFKA_HDFS_SINK', name: 'Sink Hadoop|Hive' },
  		        { id: 'CONNECT_MONGODB_SINK',  name: 'Sink MongoDB' }]} />
                <ChipField source="status" label="Task Status" />
		        <LongTextInput source="description" label="Task Description" />
                <ReferenceField label="Status" source="id" reference="status" >
                    <TextField source="state" />
                </ReferenceField>
            </FormTab>
            <FormTab label="Setting">
                <DisabledInput source="connectorConfig.cuid" label="ID or CUID or Name"/>
		        <TextField source="connectorConfig.['connector.class']" label="Connect Class Library" style={{ maxWidth: 544 }} />
                <NumberInput source="connectorConfig.['tasks.max']" label="Number of Sub-task to Submit" step={1}/>
		        <DependentInput dependsOn="connectorType" value="CONNECT_KAFKA_SOURCE_AVRO">
		        <TextInput source="connectorConfig.schema_subject" label="Schema for Files/Topic" style={{ display: 'inline-block' }}  />
                <TextInput source="connectorConfig.topic" label="Topic to Write Data" style={{ display: 'inline-block' }} />
		        <BooleanInput source="connectorConfig.file_overwrite" label="Allow File Overwrite" />
		        <TextInput source="connectorConfig.file_location" label="Path Where to Load the Files" style={{ display: 'inline-block' }} />
		        <TextInput source="connectorConfig.file_glob" label="Pattern/Glob to Match the Files" style={{ display: 'inline-block' }} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="CONNECT_MONGODB_SINK">
                <LongTextInput source="connectorConfig.topics" label="Topic to Sink Data From (use , seperate multiple values" />
		        <LongTextInput source="connectorConfig.mongodb_collections" label="Collections Where to Sink the Files (use , seperate multiple values)" />
                <TextInput source="connectorConfig.mongodb_database" label="The Database Name" />
                <NumberInput source="connectorConfig.bulk_size" label="The Bulk Size of Rows to Sink" step={1} />
                </DependentInput>	
		        <DependentInput dependsOn="connectorType" value="CONNECT_KAFKA_HDFS_SINK">
                <LongTextInput source="connectorConfig.topics" label="Topic to Sink Data From (use , seperate multiple values" />
                <BooleanInput source="connectorConfig.hive_integration" label="Enable Hive Metadata" style={{ display: 'inline-block' }} />
                <DisabledInput source="connectorConfig.hive_metastore_uris" label="Hive Metastore URL" style={{ display: 'inline-block' , marginLeft: 32 }} />
		        <DisabledInput source="connectorConfig.hdfs_url" label="HDFS URL" style={{ display: 'inline-block', marginLeft: 32 }} />
                <NumberInput source="connectorConfig.flush_size" label="The Bulk Size of Rows to Sink" step={1} />
                </DependentInput>
	        </FormTab>
	        <FormTab label="Status">
            </FormTab>
        </TabbedForm>
    </Edit>
);

export const ConnectCreate = (props) => (
    <Create {...props}>
        <SimpleForm label="Overview">
            <NumberInput source="taskSeq" label="task seq." />
            <LongTextInput source="name" label="name" />
            <SelectInput source="connectorType" label="Task Type" choices={[
                        { id: 'CONNECT_KAFKA_SOURCE_AVRO', name: 'Source Avro Files' },
                        { id: 'CONNECT_KAFKA_HDFS_SINK', name: 'Sink Hadoop|Hive' },
                        { id: 'CONNECT_MONGODB_SINK',  name: 'Sink MongoDB' },
            ]} />
            <LongTextInput source="description" label="Task Description" defaultValue="This is default description." />
	    <NumberInput source="connectorConfig.tasks_max" label="Number of Sub-task to Submit" defaultValue="1" step={1}/>		
            <DependentInput dependsOn="connectorType" value="CONNECT_KAFKA_SOURCE_AVRO">
            <LongTextInput source="connectorConfig.connector_class" label="Class Name" defaultValue="com.datafibers.kafka.connect.FileGenericSourceConnector" style={{ maxWidth: 544 }} />
            <TextInput source="connectorConfig.schema_subject" label="Schema for Files/Topic" style={{ display: 'inline-block' }}  />
            <TextInput source="connectorConfig.topic" label="Topic to Write Data" style={{ display: 'inline-block' }} />
            <BooleanInput source="connectorConfig.file_overwrite" label="Allow File Overwrite" defaultValue="true" />
            <TextInput source="connectorConfig.file_location" label="Path Where to Load the Files" style={{ display: 'inline-block' }} />
            <TextInput source="connectorConfig.file_glob" label="Pattern/Glob to Match the Files" style={{ display: 'inline-block' }} />
            </DependentInput>
            <DependentInput dependsOn="connectorType" value="CONNECT_MONGODB_SINK">
	    <LongTextInput source="connectorConfig.connector_class" label="" defaultValue="org.apache.kafka.connect.mongodb.MongodbSinkConnector" style={{ maxWidth: 544 }} />	
            <LongTextInput source="connectorConfig.topics" label="Topic to Sink Data From (use , seperate multiple values" />
            <LongTextInput source="connectorConfig.mongodb_collections" label="Collections Where to Sink the Files (use , seperate multiple values)" />
            <TextInput source="connectorConfig.mongodb_database" label="The Database Name" />
            <NumberInput source="connectorConfig.bulk_size" label="The Bulk Size of Rows to Sink" defaultValue="1" step={1} />
            </DependentInput>
            <DependentInput dependsOn="connectorType" value="CONNECT_KAFKA_HDFS_SINK">
	    <LongTextInput source="connectorConfig.connector_class" label="" defaultValue="io.confluent.connect.hdfs.HdfsSinkConnector" style={{ maxWidth: 544 }} />	
            <LongTextInput source="connectorConfig.topics" label="Topic to Sink Data From (use , seperate multiple values" />
            <BooleanInput source="connectorConfig.hive_integration" label="Enable Hive Metadata" style={{ display: 'inline-block' }} />
            <DisabledInput source="connectorConfig.hive_metastore_uris" label="Hive Metastore URL" style={{ display: 'inline-block' , marginLeft: 32 }} />
            <DisabledInput source="connectorConfig.hdfs_url" label="HDFS URL" style={{ display: 'inline-block', marginLeft: 32 }} />
            <NumberInput source="connectorConfig.flush_size" label="The Bulk Size of Rows to Sink" step={1} />
            </DependentInput>
	</SimpleForm>
    </Create>
);
                                                                                                                                                                                          1,1         

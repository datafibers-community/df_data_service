// in src/Connects.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { Datagrid, SelectField, UrlField, FunctionField, ChipField, TextField, DateField, RichTextField, ImageField, ReferenceField, ReferenceArrayField, ReferenceManyField, SingleFieldList, SelectArrayInput } from 'admin-on-rest';
import { AutocompleteInput, NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput, ReferenceInput, ReferenceArrayInput } from 'admin-on-rest';
import { EditButton, ShowButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/image/flare';
import { required, minLength, maxLength, minValue, maxValue, number, regex, email, choices } from 'admin-on-rest';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';

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
                <TextInput source="name" label="Name" validate={[ required ]} />
                <SelectField source="connectorType" label="Task Type" validate={[ required ]} choices={[
                        { id: 'CONNECT_KAFKA_SOURCE_AVRO', name: 'Source Avro Files' },
                        { id: 'CONNECT_KAFKA_HDFS_SINK', name: 'Sink Hadoop|Hive' },
                        { id: 'CONNECT_MONGODB_SINK',  name: 'Sink MongoDB' },
                ]} />

                <ChipField source="status" label="Task Status" />
                <LongTextInput source="description" label="Task Description" />
                <NumberInput source="connectorConfig.['tasks.max']" label="Number of Sub-task to Submit" step={1}/>
            </FormTab>
            <FormTab label="Setting">
                <DisabledInput source="connectorConfig.cuid" label="ID or CUID or Name"/>
                <TextField source="connectorConfig.['connector.class']" label="Connect Class Library" style={{ maxWidth: 544 }} />
                <LongTextInput source="connectorConfig.['schema.registry.uri']" label="Schema Registry URI, such as http://localhost:8081" />
                <DependentInput dependsOn="connectorType" value="CONNECT_KAFKA_SOURCE_AVRO">
                <ReferenceInput source="connectorConfig.topic" label="Choose a topic to write data" reference="schema" validate={[ required ]} allowEmpty>
                    <AutocompleteInput optionText="subject" />
                </ReferenceInput>
                <BooleanInput source="connectorConfig.['file.overwrite']" label="Allow File Overwrite" />
                <TextInput source="connectorConfig.['file.location']" label="Path Where to Load the Files" style={{ display: 'inline-block' }} validate={[ required ]} />
                <TextInput source="connectorConfig.['file.glob']" label="Pattern/Glob to Match the Files" style={{ display: 'inline-block' }} validate={[ required ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_MONGODB_SINK">
                <LongTextInput source="connectorConfig.topics" label="Topics to Sink Data From (use , seperate multiple values" />
                <TextInput source="connectorConfig.host" label="MongoDB Hostname" style={{ display: 'inline-block' }} validate={[ required ]} />
                <TextInput source="connectorConfig.port" label="MongoDB Port" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                <TextInput source="connectorConfig.['mongodb.database']" label="The Database Name" />
                <LongTextInput source="connectorConfig.['mongodb.collections']" label="Collections Where to Sink the Files (use , seperate multiple values)" />
                <NumberInput source="connectorConfig.['bulk.size']" label="The Bulk Size of Rows to Sink" step={1} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_KAFKA_HDFS_SINK">
                <LongTextInput source="connectorConfig.topics" label="Topics to Sink Data From (use , seperate multiple values" />
                <BooleanInput source="connectorConfig.['hive.integration']" label="Enable Hive Metadata" style={{ display: 'inline-block' }} />
                <DisabledInput source="connectorConfig.['hive.metastore_uris']" label="Hive Metastore URL" style={{ display: 'inline-block' , marginLeft: 32 }} />
                <DisabledInput source="connectorConfig.['hdfs.url']" label="HDFS URL" style={{ display: 'inline-block', marginLeft: 32 }} />
                <NumberInput source="connectorConfig.['flush.size']" label="The Bulk Size of Rows to Sink" step={1} />
                </DependentInput>
            </FormTab>
            <FormTab label="State">
                <ReferenceManyField addLabel={false} reference="status" target="id">
                    <Datagrid>
                        <TextField source="jobId" label="Engine Job ID." />
                        <ChipField source="taskState" label="Engine Job State" />
                        <TextField source="subTaskId" label="Subtask ID." />
                        <ChipField source="state" label="Subtask State" />
                        <TextField source="worker_id" label="Worker ID." />
                        <ShowButton />
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
                <NumberInput source="taskSeq" label="Task Sequence Number, eg. 1, 2, ..." />
                <LongTextInput source="name" label="Task Name" validate={[ required ]} />
                <SelectInput source="connectorType" label="Task Type" validate={[ required ]} choices={[
                        { id: 'CONNECT_KAFKA_SOURCE_AVRO', name: 'Source Avro Files' },
                        { id: 'CONNECT_KAFKA_HDFS_SINK', name: 'Sink Hadoop|Hive' },
                        { id: 'CONNECT_MONGODB_SINK',  name: 'Sink MongoDB' },
                ]} />
                <LongTextInput source="description" label="Task Description" defaultValue="This is default description." />
                <NumberInput source="connectorConfig.['tasks.max']" label="Number of Sub-task to Submit" defaultValue={1} step={1}/>
            </FormTab>
            <FormTab label="Setting">
                <DependentInput dependsOn="connectorType" value="CONNECT_KAFKA_SOURCE_AVRO">
                    <ReferenceInput source="connectorConfig.topic" label="Choose a topic to write data" reference="schema" validate={[ required ]} allowEmpty>
                        <AutocompleteInput optionText="subject" />
                    </ReferenceInput>
                    <LongTextInput source="connectorConfig.['schema.registry.uri']" label="Schema Registry URI, such as http://localhost:8081" validate={[ required ]} />
                    <BooleanInput source="connectorConfig.['file.overwrite']" label="Allow File Overwrite" defaultValue={true} />
                    <TextInput source="connectorConfig.['file.location']" label="Path Where to Load the Files" style={{ display: 'inline-block' }} defaultValue={"/home/vagrant/df_data"} validate={[ required ]} />
                    <TextInput source="connectorConfig.['file.glob']" label="Pattern/Glob to Match the Files" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_MONGODB_SINK">
                    <ReferenceArrayInput source="connectorConfig.topics" label="Choose topics to write data" reference="schema" validate={[ required ]} allowEmpty>
                        <SelectArrayInput optionText="subject" />
                    </ReferenceArrayInput>
                    <TextInput source="connectorConfig.host" label="MongoDB Hostname" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.port" label="MongoDB Port" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <TextInput source="connectorConfig.['mongodb.database']" label="The Database Name" validate={[ required ]} />
                    <LongTextInput source="connectorConfig.['mongodb.collections']" label="Collections Where to Sink the Files (use , separate multiple values)" validate={[ required ]} />
                    <NumberInput source="connectorConfig.['bulk.size']" label="The Bulk Size of Rows to Sink" defaultValue="1" step={1} validate={[ required ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorType" value="CONNECT_KAFKA_HDFS_SINK">
                    <ReferenceArrayInput source="connectorConfig.topics" label="Choose topics to write data" reference="schema" validate={[ required ]} allowEmpty>
                        <SelectArrayInput optionText="subject" />
                    </ReferenceArrayInput>
                    <BooleanInput source="connectorConfig.['hive.integration']" label="Enable Hive Metadata" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <DisabledInput source="connectorConfig.['hive.metastore.uris']" label="Hive Metastore URL" style={{ display: 'inline-block' , marginLeft: 32 }} validate={[ required ]} />
                    <DisabledInput source="connectorConfig.['hdfs.url']" label="HDFS URL" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <NumberInput source="connectorConfig.['flush.size']" label="The Bulk Size of Rows to Sink" step={1} validate={[ required ]} />
                </DependentInput>
            </FormTab>
        </TabbedForm>
    </Create>
);
                                                                                                                                                                                          1,1         

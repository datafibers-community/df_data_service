// in src/Schemas.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { Datagrid, SelectField, FunctionField, ChipField, TextField, DateField, RichTextField, NumberField } from 'admin-on-rest';
import { SelectArrayInput, AutocompleteInput, NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput, ReferenceInput, ReferenceArrayInput } from 'admin-on-rest';
import { ShowButton, EditButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/image/blur-on';
import { required, minLength, maxLength, minValue, maxValue, number, regex, email, choices } from 'admin-on-rest';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';
import EmbeddedArrayInputFormField from '../component/EmbeddedArrayInputFormField';

export const SchemaIcon = Icon;

const RawRecordField = ({ record, source }) => <pre dangerouslySetInnerHTML={{ __html: JSON.stringify(record, null, '\t')}}></pre>;
RawRecordField.defaultProps = { label: 'Raw Json' };

const SchemaShowTitle = ({ record }) => {
    return <span>Raw Json with ID. {record ? `"${record.id}"` : ''}</span>;
};

const SchemaTitle = ({ record }) => {
    return <span>Topic Name. {record ? `"${record.id}"` : ''}</span>;
};

const SchemaFilter = (props) => (
    <Filter {...props}>
        <TextInput label="Search" source="q" alwaysOn />
        <TextInput label="Compatibility" source="status" defaultValue="FULL" />
    </Filter>
);

export const SchemaShow = (props) => (
    <Show title={<SchemaShowTitle />} {...props}>
        <SimpleShowLayout>
            <RawRecordField />
        </SimpleShowLayout>
    </Show>
);

export const SchemaList = (props) => (
    <List {...props} title="Topic List" filters={<SchemaFilter />}>
        <Datagrid bodyOptions={{ stripedRows: true, showRowHover: true}} >
	        <TextField source="id" label="Topic Name" />
            <TextField source="schema.name" label="Schema Name" />
            <TextField source="schema.type" label="Schema Type" />
            <NumberField source="version" label="Schema Version" />
            <TextField source="compatibility" label="Compatibility" />
            <EditButton />
        </Datagrid>
    </List>
);

export const SchemaEdit = (props) => (
    <Edit title={<SchemaTitle />} {...props}>
        <SimpleForm>
	        <DisabledInput source="id" label="Topic Name" />
            <TextInput source="schema.name" label="Schema Name" />
            <DisabledInput source="schema.type" label="Schema Type" />
            <NumberInput source="version" label="Schema Version" />
            <SelectInput source="compatibility" label="Compatibility" validate={[ required ]} choices={[
                        { id: 'NONE', name: 'NONE' },
                        { id: 'FULL', name: 'FULL' },
                        { id: 'BACKWARD',  name: 'BACKWARD' },
                        { id: 'FORWARD',  name: 'FORWARD' }, ]}
            />
            <RawJsonRecordSpecificField source="schema.fields" label="fields" />
        </SimpleForm>
    </Edit>
);

export const SchemaCreate = (props) => (
    <Create title="Create New Topic Guide" {...props}>
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
            <FormTab label="Testing">
                <EmbeddedArrayInputFormField source="links">
                    <TextInput source="url" />
                    <TextInput source="context"/>
                </EmbeddedArrayInputFormField>
            </FormTab>
        </TabbedForm>
    </Create>
);

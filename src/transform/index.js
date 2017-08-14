// in src/Connects.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { ReferenceField, Datagrid, SelectField, UrlField, FunctionField, ChipField, TextField, DateField, RichTextField, ImageField } from 'admin-on-rest';
import { SelectArrayInput, AutocompleteInput, NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput, ReferenceInput, ReferenceArrayInput } from 'admin-on-rest';
import { EditButton, ShowButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import { required, minLength, maxLength, minValue, maxValue, number, regex, email, choices } from 'admin-on-rest';
import Icon from 'material-ui/svg-icons/image/transform';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';

export const TransformIcon = Icon;

const TransformShowTitle = ({ record }) => {
    return <span>Raw Json with ID. {record ? `"${record.id}"` : ''}</span>;
};

const TransformTitle = ({ record }) => {
    return <span>ID. {record ? `"${record.id}"` : ''}</span>;
};

const TransformFilter = (props) => (
    <Filter {...props}>
        <TextInput label="Search" source="q" alwaysOn />
        <TextInput label="Status" source="status" defaultValue="RUNNING" />
    </Filter>
);

export const TransformShow = (props) => (
    <Show title={<TransformShowTitle />} {...props}>
        <SimpleShowLayout>
	    <RawJsonRecordField />
        </SimpleShowLayout>
    </Show>
);

export const TransformList = (props) => (
    <List {...props} title="Transform List" filters={<TransformFilter />}>
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

export const TransformEdit = (props) => (
    <Edit title={<TransformTitle />} {...props}>
        <TabbedForm>
            <FormTab label="Overview">
                <DisabledInput source="taskSeq" label="Task Sequence" />
                <TextInput source="name" label="Name" validate={[ required ]} />
		        <SelectField source="connectorType" label="Task Type" validate={[ required ]} choices={[
    			{ id: 'TRANSFORM_FLINK_SQL_A2A', name: 'Flink Streaming SQL' },
    			{ id: 'TRANSFORM_FLINK_SCRIPT', name: 'Flink Table API' },
  		        { id: 'TRANSFORM_FLINK_UDF',  name: 'Flink User Defined Function' },
		        ]} />
                <ChipField source="status" label="Task Status" />
		        <LongTextInput source="description" label="Task Description" />
            </FormTab>
            <FormTab label="Setting">
                <DisabledInput source="connectorConfig.cuid" label="ID or CUID or Name"/>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_FLINK_SQL_A2A">
                    <TextInput source="connectorConfig.['topic.in']" label="A Topic to Read Data" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.['topic.out']" label="A Topic to Write Data" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <LongTextInput source="connectorConfig.['group.id']" label="Consumer ID to Read Data. (Optional)" />
		            <LongTextInput source="connectorConfig.['sink.key.fields']" label="List of Commas Separated Columns for Keys in Sink" />
		            <LongTextInput source="connectorConfig.['trans.sql']" label="Stream SQL Statement, such as select * from ..." validate={[ required ]} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_FLINK_SCRIPT">
                    <LongTextInput source="connectorConfig.topics" label="Topics to Sink Data From (use , seperate multiple values" />
		            <LongTextInput source="connectorConfig.['mongodb.collections']" label="Collections Where to Sink the Files (use , seperate multiple values)" />
                    <TextInput source="connectorConfig.['mongodb.database']" label="The Database Name" />
                    <NumberInput source="connectorConfig.['bulk.size']" label="The Bulk Size of Rows to Sink" step={1} />
                </DependentInput>	
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_FLINK_UDF">
                    <LongTextInput source="connectorConfig.topics" label="Topics to Sink Data From (use , seperate multiple values" />
                    <BooleanInput source="connectorConfig.['hive.integration']" label="Enable Hive Metadata" style={{ display: 'inline-block' }} />
                    <DisabledInput source="connectorConfig.['hive.metastore_uris']" label="Hive Metastore URL" style={{ display: 'inline-block' , marginLeft: 32 }} />
		            <DisabledInput source="connectorConfig.['hdfs.url']" label="HDFS URL" style={{ display: 'inline-block', marginLeft: 32 }} />
                    <NumberInput source="connectorConfig.['flush.size']" label="The Bulk Size of Rows to Sink" step={1} />
                </DependentInput>
	        </FormTab>
            <FormTab label="State">
                <ReferenceField label="Engine Job|Task ID." source="id" reference="status" linkType={false}>
                    <TextField source="jobId" />
                </ReferenceField>
                <ReferenceField label="Engine Job|Task State" source="id" reference="status" linkType={false}>
                    <ChipField source="jobState" />
                </ReferenceField>
                <ReferenceField label="List of Sub Job|Task" source="id" reference="status" linkType={false}>
                    <RawJsonRecordSpecificField source="subTask" />
                </ReferenceField>
            </FormTab>
        </TabbedForm>
    </Edit>
);

export const TransformCreate = (props) => (
    <Create title="Create New Transform Task Guide" {...props}>
        <TabbedForm>
            <FormTab label="Overview">
                <NumberInput source="taskSeq" label="Task Sequence Number, eg. 1, 2, ..." />
                <TextInput source="name" label="Name" validate={[ required ]} />
		        <SelectInput source="connectorType" label="Task Type" validate={[ required ]} choices={[
    			{ id: 'TRANSFORM_FLINK_SQL_A2A', name: 'Flink Streaming SQL' },
    			{ id: 'TRANSFORM_FLINK_SCRIPT', name: 'Flink Table API' },
  		        { id: 'TRANSFORM_FLINK_UDF',  name: 'Flink User Defined Function' },
		        ]} />
		        <LongTextInput source="description" label="Task Description" />
            </FormTab>
            <FormTab label="Setting">
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_FLINK_SQL_A2A">
                    <ReferenceArrayInput source="connectorConfig.['topic.in']" label="Choose Topics to Read Data" reference="schema" validate={[ required ]} allowEmpty>
                        <SelectArrayInput optionText="subject" />
                    </ReferenceArrayInput>
                    <ReferenceInput source="connectorConfig.['topic.out']" label="Choose a Topic to Write Data" reference="schema" validate={[ required ]} allowEmpty>
                        <AutocompleteInput optionText="subject" />
                    </ReferenceInput>
                    <LongTextInput source="connectorConfig.['group.id']" label="Consumer ID to Read Data. (Optional)" />
		            <LongTextInput source="connectorConfig.['sink.key.fields']" label="List of Commas Separated Columns for Keys in Sink" />
		            <LongTextInput source="connectorConfig.['trans.sql']" label="Stream SQL Statement" validate={[ required ]} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_FLINK_SCRIPT">
                    <TextInput source="connectorConfig.['topic.in']" label="A Topic to Read Data" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.['topic.out']" label="A Topic to Write Data" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <LongTextInput source="connectorConfig.['group.id']" label="Consumer ID to Read Data. (Optional)" />
		            <LongTextInput source="connectorConfig.['trans.script']" label="Stream Script Statement, such as select(name).count()" validate={[ required ]} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_FLINK_UDF">
                    <TextInput source="connectorConfig.['trans.jar']" label="UDF Jar File Name" validate={[ required ]} />
		        </DependentInput>
            </FormTab>    
        </TabbedForm>
    </Create>
);

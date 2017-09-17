// in src/Connects.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { ReferenceField, Datagrid, SelectField, UrlField, FunctionField, ChipField, LongTextField, TextField, DateField, RichTextField, ImageField } from 'admin-on-rest';
import { SelectArrayInput, AutocompleteInput, NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput, ReferenceInput, ReferenceArrayInput, ReferenceManyField } from 'admin-on-rest';
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
                <DisabledInput source="taskSeq" label="Task Sequence" style={{ display: 'inline-block' }} />
                <ChipField source="status" label="Task Status" style={{ display: 'inline-block', marginLeft: 32, width: 150}} />
                <TextInput source="name" label="Name" validate={[ required ]} />
		        <LongTextInput source="description" label="Task Description" />
		        <SelectField source="connectorType" label="Task Type" validate={[ required ]} choices={[
    			{ id: 'TRANSFORM_EXCHANGE_FLINK_SQLA2A', name: 'Flink Streaming SQL' },
    			{ id: 'TRANSFORM_EXCHANGE_FLINK_Script', name: 'Flink Table API' },
  		        { id: 'TRANSFORM_EXCHANGE_FLINK_UDF',  name: 'Flink User Defined Function' },
		        ]} />
            </FormTab>
            <FormTab label="Setting">
                <DisabledInput source="connectorConfig.cuid" label="ID or CUID or Name"/>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_SQLA2A">
                    <TextInput source="connectorConfig.topic_in" label="A Topic to Read Data" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.topic_out" label="A Topic to Write Data" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <LongTextInput source="connectorConfig.group_id" label="Consumer ID to Read Data. (Optional)" />
		            <LongTextInput source="connectorConfig.sink_key_fields" label="List of Commas Separated Columns for Keys in Sink" />
		            <LongTextInput source="connectorConfig.trans_sql" label="Stream SQL Statement, such as select * from ..." validate={[ required ]} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_Script">
                    <TextInput source="connectorConfig.topic_in" label="A Topic to Read Data" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.topic_out" label="A Topic to Write Data" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <LongTextInput source="connectorConfig.group_id" label="Consumer ID to Read Data. (Optional)" />
		            <LongTextInput source="connectorConfig.sink_key_fields" label="List of Commas Separated Columns for Keys in Sink" />
		            <LongTextInput source="connectorConfig.trans_script" label="Stream SQL Statement, such as select * from ..." validate={[ required ]} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_UDF">
                    <TextInput source="connectorConfig.trans_jar" label="UDF Jar File Name" validate={[ required ]} />
                </DependentInput>
	        </FormTab>
            <FormTab label="State">
                <ReferenceManyField addLabel={false} reference="status" target="id">
                    <Datagrid>
                        <TextField source="jobId" label="Engine Job ID." />
                        <ChipField source="taskState" label="Engine Job State" />
                        <TextField source="subTaskId" label="Engine Job ID." />
                        <ChipField source="status" label="Subtask State" />
                        <TextField source="name" label="Subtask Desc." />
                        <ShowButton />
                    </Datagrid>
                </ReferenceManyField>
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
		        <LongTextInput source="description" label="Task Description" />
		        <SelectInput source="connectorType" label="Task Type" validate={[ required ]} choices={[
    			{ id: 'TRANSFORM_EXCHANGE_FLINK_SQLA2A', name: 'Flink Streaming SQL' },
    			{ id: 'TRANSFORM_EXCHANGE_FLINK_Script', name: 'Flink Table API' },
  		        { id: 'TRANSFORM_EXCHANGE_FLINK_UDF',  name: 'Flink User Defined Function' },
		        ]} />
            </FormTab>
            <FormTab label="Setting">
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_SQLA2A">
                    <ReferenceArrayInput source="connectorConfig.topic_in" label="Choose Topics to Read Data" reference="schema" validate={[ required ]} allowEmpty>
                        <SelectArrayInput optionText="subject" />
                    </ReferenceArrayInput>
                    <ReferenceInput source="connectorConfig.topic_out" label="Choose a Topic to Write Data" reference="schema" validate={[ required ]} allowEmpty>
                        <AutocompleteInput optionText="subject" />
                    </ReferenceInput>
                    <LongTextInput source="connectorConfig.group_id" label="Consumer ID to Read Data. (Optional)" />
		            <LongTextInput source="connectorConfig.sink_key_fields" label="Key Columns in Sink (separated by ,)" />
		            <LongTextInput source="connectorConfig.trans_sql" label="Stream SQL Statement" validate={[ required ]} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_Script">
                    <TextInput source="connectorConfig.topic_in" label="A Topic to Read Data" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.topic_out" label="A Topic to Write Data" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <LongTextInput source="connectorConfig.group_id" label="Consumer ID to Read Data. (Optional)" />
		            <LongTextInput source="connectorConfig.trans_script" label="Stream Script Statement, such as select(name).count()" validate={[ required ]} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_UDF">
                    <TextInput source="connectorConfig.trans_jar" label="UDF Jar File Name" validate={[ required ]} />
		        </DependentInput>
            </FormTab>    
        </TabbedForm>
    </Create>
);

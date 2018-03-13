// in src/Connects.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { ReferenceField, Datagrid, SelectField, UrlField, FunctionField, ChipField, LongTextField, TextField, DateField, RichTextField, ImageField } from 'admin-on-rest';
import { RadioButtonGroupInput, NullableBooleanInput, SelectArrayInput, AutocompleteInput, NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput, DateInput, ReferenceInput, ReferenceArrayInput, ReferenceManyField } from 'admin-on-rest';
import { EditButton, ShowButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import { required, minLength, maxLength, minValue, maxValue, number, regex, email, choices } from 'admin-on-rest';
import Icon from 'material-ui/svg-icons/device/widgets.js';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';
import RefreshListActions from '../buttons/RefreshListActions'

export const ModelIcon = Icon;

const ModelShowTitle = ({ record }) => {
    return <span>Raw Json with ID. {record ? `"${record.id}"` : ''}</span>;
};

const ModelTitle = ({ record }) => {
    return <span>ID. {record ? `"${record.id}"` : ''}</span>;
};

const ModelFilter = (props) => (
    <Filter {...props}>
        <TextInput label="Search" source="q" alwaysOn />
        <TextInput label="Category" source="category" defaultValue="Classification" />
    </Filter>
);

export const ModelShow = (props) => (
    <Show title={<ModelShowTitle />} {...props}>
        <SimpleShowLayout>
	    <RawJsonRecordField />
        </SimpleShowLayout>
    </Show>
);

export const ModelList = (props) => (
    <List {...props} title="Model List" filters={<ModelFilter />} actions={<RefreshListActions refreshInterval="10000" />}>
        <Datagrid
            headerOptions={{ adjustForCheckbox: true, displaySelectAll: true }}
            bodyOptions={{ displayRowCheckbox: true, stripedRows: true, showRowHover: true}}
            rowOptions={{ selectable: true }}
            options={{ multiSelectable: true }}>
            <TextField source="id" label="id" />
            <TextField source="name" label="name" />
            <SelectField source="category" label="Category" choices={[
                			{ id: 'classification', name: 'Classification' },
                			{ id: 'regression', name: 'Regression' },
                			{ id: 'clustering', name: 'Clustering' },
                			{ id: 'recommendation', name: 'Recommendation' },
            		        ]} />
            <SelectField source="type" label="Type" choices={[
                			{ id: 'sparkml', name: 'Spark MLLib' },
                			{ id: 'scikit-learn', name: 'Scikit-Learn' },
                			{ id: 'tensorflow', name: 'TensorFlow' },
                			{ id: 'xgboost', name: 'Xgboost' },
              		        { id: 'dl4j',  name: 'DeepLearning4J' },
            		        ]} />
            <DateField source="updateDate" label="Update Date" />
            <EditButton />
        </Datagrid>
    </List>
);

export const ModelEdit = (props) => (
    <Edit title={<ModelTitle />} {...props}>
        <TabbedForm>
            <FormTab label="Overview">
                <TextInput source="name" label="Name" validate={[ required ]} style={{ width: 500 }} />
		        <LongTextInput source="description" label="Model Description" style={{ width: 500 }} />
                <SelectField source="category" label="Model Category" choices={[
                                { id: 'classification', name: 'Classification' },
                                { id: 'regression', name: 'Regression' },
                                { id: 'clustering', name: 'Clustering' },
                                { id: 'recommendation', name: 'Recommendation' },
                                ]} defaultValue='classification' />
                <SelectField source="type" label="Model Type" choices={[
                                { id: 'sparkml', name: 'Spark MLLib' },
                                { id: 'scikit-learn', name: 'Scikit-Learn' },
                                { id: 'tensorflow', name: 'TensorFlow' },
                                { id: 'xgboost', name: 'Xgboost' },
                                { id: 'dl4j',  name: 'DeepLearning4J' },
                                ]} defaultValue='sparkml' />
                <DateInput source="updateDate" label="Create/Update Date" />
            </FormTab>
            <FormTab label="Setting">
		        <LongTextInput source="path" label="Model HDFS Path" style={{ width: 500 }}/>
		        <TextInput source="udf" label="UDF Name Registered"/>
		        <TextInput source="modelOutputPara" label="Model Output Type"/>
                <EmbeddedArrayInput source="modelInputPara" label="Add Model Input Parameters">
                    <TextInput source="para_name" label="Model's Parameter Name" style={{ display: 'inline-block', float: 'left' }} />
                    <TextInput source="para_type" label="Model's Parameter Type" style={{ display: 'inline-block', marginLeft: 32}}/>
                </EmbeddedArrayInput>
                <TextInput source="idTrained" label="Model Trained/Persisted by Job Id"/>
	        </FormTab>
        </TabbedForm>
    </Edit>
);

export const ModelCreate = (props) => (
    <Create title="Create New Model Task Guide" {...props}>
        <TabbedForm>
            <FormTab label="Overview">
                <TextInput source="name" label="Name" validate={[ required ]} style={{ width: 500 }} />
		        <LongTextInput source="description" label="Model Description" style={{ width: 500 }} />
                <SelectField source="category" label="Model Category" choices={[
                                { id: 'classification', name: 'Classification' },
                                { id: 'regression', name: 'Regression' },
                                { id: 'clustering', name: 'Clustering' },
                                { id: 'recommendation', name: 'Recommendation' },
                                ]} defaultValue='classification' />
                <SelectField source="type" label="Model Type" choices={[
                                { id: 'sparkml', name: 'Spark MLLib' },
                                { id: 'scikit-learn', name: 'Scikit-Learn' },
                                { id: 'tensorflow', name: 'TensorFlow' },
                                { id: 'xgboost', name: 'Xgboost' },
                                { id: 'dl4j',  name: 'DeepLearning4J' },
                                ]} defaultValue='sparkml' />
                <DateInput source="updateDate" label="Create/Update Date" />
            </FormTab>
            <FormTab label="Setting">
		        <LongTextInput source="path" label="Model HDFS Path" style={{ width: 500 }}/>
		        <TextInput source="udf" label="UDF Name Registered"/>
		        <TextInput source="modelOutputPara" label="Model Output Type"/>
                <EmbeddedArrayInput source="modelInputPara" label="Add Model Input Parameters">
                    <TextInput source="para_name" label="Model's Parameter Name" style={{ display: 'inline-block', float: 'left' }} />
                    <TextInput source="para_type" label="Model's Parameter Type" style={{ display: 'inline-block', marginLeft: 32}}/>
                </EmbeddedArrayInput>
            </FormTab>    
        </TabbedForm>
    </Create>
);

// in src/Schemas.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { Datagrid, SelectField, FunctionField, ChipField, TextField, DateField, RichTextField, NumberField } from 'admin-on-rest';
import { NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput } from 'admin-on-rest';
import { ShowButton, EditButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/image/blur-on';
import { required, minLength, maxLength, minValue, maxValue, number, regex, email, choices } from 'admin-on-rest';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';

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

// in src/Schemas.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { Datagrid, SelectField, FunctionField, ChipField, TextField, DateField, RichTextField, NumberField } from 'admin-on-rest';
import { NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput } from 'admin-on-rest';
import { EditButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/image/blur-on';

export const SchemaIcon = Icon;

const RawRecordField = ({ record, source }) => <pre dangerouslySetInnerHTML={{ __html: JSON.stringify(record, null, '\t')}}></pre>;
RawRecordField.defaultProps = { label: 'Raw Json' };

const SchemaShowTitle = ({ record }) => {
    return <span>Raw Json with ID. {record ? `"${record.id}"` : ''}</span>;
};

const SchemaTitle = ({ record }) => {
    return <span>ID. {record ? `"${record.id}"` : ''}</span>;
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
        <Datagrid
            headerOptions={{ adjustForCheckbox: true, displaySelectAll: true }}
            bodyOptions={{ displayRowCheckbox: true, stripedRows: true, showRowHover: true}}
            rowOptions={{ selectable: true }}
            options={{ multiSelectable: true }}>    
	    <TextField source="id" label="id" />
            <TextField source="subject" label="Name" />
            <TextField source="schema.name" label="Desc." />
            <TextField source="schema.type" label="Schema Type" />
            <NumberField source="version" label="Schema Version" />
            <TextField source="compatibility" label="Compatibility" />
            <EditButton />
        </Datagrid>
    </List>
);

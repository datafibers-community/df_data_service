// in src/Connects.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { ReferenceField, Datagrid, SelectField, UrlField, FunctionField, ChipField, TextField, DateField, RichTextField, ImageField } from 'admin-on-rest';
import { NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput } from 'admin-on-rest';
import { ShowButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/action/language';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';
import RefreshListActions from '../buttons/RefreshListActions'

export const ProcessorIcon = Icon;

const ProcessorShowTitle = ({ record }) => {
    return <span>Raw Json with ID. {record ? `"${record.id}"` : ''}</span>;
};

const ProcessorFilter = (props) => (
    <Filter {...props}>
        <TextInput label="Search" source="q" alwaysOn />
    </Filter>
);

export const ProcessorShow = (props) => (
    <Show title={<ProcessorShowTitle />} {...props}>
        <SimpleShowLayout>
	    <RawJsonRecordField />
        </SimpleShowLayout>
    </Show>
);

export const ProcessorList = (props) => (
    <List {...props} title="All Live Task Status Board"
    actions={<RefreshListActions refreshInterval="5000" />} filters={<ProcessorFilter />}
    >
        <Datagrid bodyOptions={{ stripedRows: true, showRowHover: true}} >
            <TextField source="id" label="id" />
            <TextField source="taskSeq" label="seq." />
            <TextField source="name" label="name" />
            <TextField source="connectorCategory" label="category" />
            <TextField source="connectorType" label="type" />
            <ChipField source="status" label="status" />
            <ShowButton />
        </Datagrid>
    </List>
);
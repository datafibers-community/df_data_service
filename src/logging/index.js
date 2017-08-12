// in src/Loggings.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { Datagrid, SelectField, FunctionField, ChipField, TextField, DateField, RichTextField, NumberField } from 'admin-on-rest';
import { NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput } from 'admin-on-rest';
import { EditButton, ShowButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/image/timelapse';

export const LoggingIcon = Icon;

const RawRecordField = ({ record, source }) => <pre dangerouslySetInnerHTML={{ __html: JSON.stringify(record, null, '\t')}}></pre>;
RawRecordField.defaultProps = { label: 'Raw Json' };

const LoggingShowTitle = ({ record }) => {
    return <span>Raw Log Json with ID. {record ? `"${record.id}"` : ''}</span>;
};

const LoggingTitle = ({ record }) => {
    return <span>ID. {record ? `"${record.id}"` : ''}</span>;
};

const LoggingFilter = (props) => (
    <Filter {...props}>
        <TextInput label="Search" source="q" alwaysOn />
    </Filter>
);

export const LoggingShow = (props) => (
    <Show title={<LoggingShowTitle />} {...props}>
        <SimpleShowLayout>
            <RawRecordField />
        </SimpleShowLayout>
    </Show>
);

export const LoggingList = (props) => (
    <List {...props} title="Logging List" filters={<LoggingFilter />}>
        <Datagrid >
	        <TextField source="id" label="log id" />
            <TextField source="timestamp" label="timestamp" />
            <TextField source="level" label="level" />
            <TextField source="className" label="class name" />
            <TextField source="method" label="method" />
            <NumberField source="lineNumber" label="line" />
            <ShowButton />
        </Datagrid>
    </List>
);

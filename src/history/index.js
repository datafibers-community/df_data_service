// in src/Connects.js
import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { ReferenceField, Datagrid, SelectField, UrlField, FunctionField, ChipField, TextField, DateField, RichTextField, ImageField, NumberField } from 'admin-on-rest';
import { NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/image/assistant-photo';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';

export const HistIcon = Icon;

export const HistList = (props) => (
    <List {...props} title="Process History" >
        <Datagrid bodyOptions={{ stripedRows: true, showRowHover: true}} >
            <TextField source="cuid" label="id" />
            <TextField source="file_name" label="Processed" />
            <TextField source="file_size" label="Size (byte)" />
            <TextField source="schema_subject" label="Topic" />
            <NumberField source="process_milliseconds" label="Time Spent (ms.)" />
            <TextField source="last_modified_timestamp" label="Last Update" />
            <TextField source="status" label="Status" />
        </Datagrid>
    </List>
);
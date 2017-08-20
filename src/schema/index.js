// in src/Schemas.js
import React from 'react';
import { translate } from 'admin-on-rest';
import Avatar from 'material-ui/Avatar';
import LightBulbIcon from 'material-ui/svg-icons/action/lightbulb-outline';
import { Card, CardHeader, CardText } from 'material-ui/Card';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { Datagrid, SelectField, FunctionField, ChipField, TextField, DateField, RichTextField, NumberField, ReferenceManyField } from 'admin-on-rest';
import { SelectArrayInput, AutocompleteInput, NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput, ReferenceInput, ReferenceArrayInput } from 'admin-on-rest';
import { ShowButton, EditButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import Icon from 'material-ui/svg-icons/image/blur-on';
import { required, minLength, maxLength, minValue, maxValue, number, regex, email, choices } from 'admin-on-rest';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';
import EmbeddedArrayInput from '../component/EmbeddedArrayInput';
import EmbeddedArrayField from '../component/EmbeddedArrayField';
import ShowProcessorButton from '../buttons/ShowProcessorButton';

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
        <TabbedForm>
            <FormTab label="Overview">
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
            </FormTab>
            <FormTab label="Fields">
                <Card style={{ margin: '2em' }}>
                     <CardHeader
                         title="Please well understand the schema compatibility before editing."
                         style={{ fontWeight: 'bold',  textAlign: 'center' }}
                         avatar={<Avatar backgroundColor="#FFEB3B" icon={<LightBulbIcon />} />}
                     />
                </Card>
                <EmbeddedArrayInput source="schema.fields" label="">
                    <TextInput source="name" label="Column Name"/>
                    <SelectInput source="type" label="Column Date Type" choices={[
                        { id: 'string', name: 'String/Text' },
                        { id: 'long', name: 'Long 64bit' },
                        { id: 'int',  name: 'Integer 32bit' },
                        { id: 'boolean',  name: 'Boolean 0 (false) or 1 (true)' },
                        { id: 'null',  name: 'Null' }]} />
                </EmbeddedArrayInput>
            </FormTab>
            <FormTab label="Tasks Related">
                <ReferenceManyField addLabel={false} reference="s2t" target="id">
                    <Datagrid>
                            <TextField source="id" label="id" />
                            <TextField source="taskSeq" label="task seq." />
                            <TextField source="name" label="name" />
                            <TextField source="connectorType" label="task type" />
                            <ChipField source="status" label="status" />
                        <ShowProcessorButton />
                    </Datagrid>
                </ReferenceManyField>
            </FormTab>
        </TabbedForm>
    </Edit>
);

export const SchemaCreate = (props) => (
    <Create title="Create New Topic Guide" {...props}>
        <TabbedForm>
            <FormTab label="Overview">
                <TextInput source="id" label="Topic Name" validate={[ required ]} />
                <SelectInput source="schema.type" label="Schema Type" validate={[ required ]} defaultValue="record" choices={[
                             { id: 'record', name: 'Record' },
                             { id: 'enum', name: 'Enum' },
                             { id: 'array',  name: 'Array' },
                             { id: 'map',  name: 'Map' },
                             { id: 'fixed',  name: 'Fixed' },]}
                 />
                <NumberInput source="version" label="Schema Version" />
                <SelectInput source="compatibility" label="Compatibility" validate={[ required ]} defaultValue="NONE" choices={[
                            { id: 'NONE', name: 'NONE' },
                            { id: 'FULL', name: 'FULL' },
                            { id: 'BACKWARD',  name: 'BACKWARD' },
                            { id: 'FORWARD',  name: 'FORWARD' }, ]}
                />
            </FormTab>
            <FormTab label="Fields">
                <EmbeddedArrayInput source="schema.fields" label="">
                    <TextInput source="name" label="Column Name"/>
                    <SelectInput source="type" label="Column Date Type" defaultValue="string" choices={[
                        { id: 'string', name: 'String/Text' },
                        { id: 'long', name: 'Long 64bit' },
                        { id: 'int',  name: 'Integer 32bit' },
                        { id: 'boolean',  name: 'Boolean 0 (false) or 1 (true)' },
                        { id: 'null',  name: 'Null' }]} />
                </EmbeddedArrayInput>
            </FormTab>
        </TabbedForm>
    </Create>
);

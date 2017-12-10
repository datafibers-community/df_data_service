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
    actions={<RefreshListActions refreshInterval="10000" />} filters={<ProcessorFilter />}
    >
        <Datagrid bodyOptions={{ stripedRows: true, showRowHover: true}} >
            <TextField source="id" label="id" />
            <TextField source="taskSeq" label="seq." />
            <TextField source="name" label="name" />
            <TextField source="connectorCategory" label="category" />
            <SelectField source="connectorType" label="type" choices={[
<<<<<<< HEAD
                                    { id: 'INTERNAL_METADATA_COLLECT', name: 'Internal Meta Sink'},
=======
				    { id: 'INTERNAL_METADATA_COLLECT', name: 'Internal Meta Sink'},
>>>>>>> bf365867ba3906d27e575b4042e28660c29bad1f
                                    { id: 'CONNECT_SOURCE_KAFKA_AvroFile', name: 'Source Avro Files' },
                                    { id: 'CONNECT_SOURCE_STOCK_AvroFile', name: 'Source Stock API' },
                                    { id: 'CONNECT_SINK_HDFS_AvroFile', name: 'Sink Hadoop|Hive' },
                                    { id: 'CONNECT_SINK_MONGODB_AvroDB',  name: 'Sink MongoDB' },
                                    { id: 'CONNECT_SINK_KAFKA_JDBC',  name: 'Sink JDBC' },
                                    { id: 'TRANSFORM_EXCHANGE_FLINK_SQLA2A', name: 'Stream SQL on Queue' },
                         			{ id: 'TRANSFORM_EXCHANGE_SPARK_SQL', name: 'Batch SQL off Queue' },
                         			{ id: 'TRANSFORM_EXCHANGE_FLINK_Script', name: 'Stream Script on Queue' },
                         			{ id: 'TRANSFORM_EXCHANGE_SPARK_STREAM', name: 'Batch Script off Queue' },
                       		        { id: 'TRANSFORM_EXCHANGE_FLINK_UDF',  name: 'Stream UDF on Queue' },
                       		        { id: 'TRANSFORM_EXCHANGE_SPARK_UDF',  name: 'Batch UDF on Queue' },
            ]} />
            <ChipField source="status" label="status" />
            <ShowButton />
        </Datagrid>
    </List>
);

import React from 'react';
import { Filter, List, Edit, Create } from 'admin-on-rest';
import { ReferenceField, Datagrid, SelectField, UrlField, FunctionField, ChipField, LongTextField, TextField, DateField, RichTextField, ImageField } from 'admin-on-rest';
import { RadioButtonGroupInput, NullableBooleanInput, SelectArrayInput, AutocompleteInput, NumberInput, DisabledInput, BooleanInput, LongTextInput, SelectInput, TextInput, ReferenceInput, ReferenceArrayInput, ReferenceManyField } from 'admin-on-rest';
import { EditButton, ShowButton } from 'admin-on-rest';
import { Show, SimpleShowLayout, SimpleForm, TabbedForm, FormTab } from 'admin-on-rest';
import RichTextInput from 'aor-rich-text-input';
import { DependentInput } from 'aor-dependent-input';
import { required, minLength, maxLength, minValue, maxValue, number, regex, email, choices } from 'admin-on-rest';
import Icon from 'material-ui/svg-icons/image/transform';
import RawJsonRecordField from '../component/RawJsonRecordField';
import RawJsonRecordSpecificField from '../component/RawJsonRecordSpecificField';
import RefreshListActions from '../buttons/RefreshListActions'
import EmbeddedArrayInput from '../component/EmbeddedArrayInput';
import EmbeddedArrayField from '../component/EmbeddedArrayField';
import { Card, CardHeader, CardActions, CardText } from 'material-ui/Card';
import InfoIcon from 'material-ui/svg-icons/content/content-paste';
import Avatar from 'material-ui/Avatar';

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
    <List {...props} title="Transform List" filters={<TransformFilter />} actions={<RefreshListActions refreshInterval="10000" />}>
        <Datagrid
            headerOptions={{ adjustForCheckbox: true, displaySelectAll: true }}
            bodyOptions={{ displayRowCheckbox: true, stripedRows: true, showRowHover: true}}
            rowOptions={{ selectable: true }}
            options={{ multiSelectable: true }}>
            <TextField source="id" label="id" />
            <TextField source="taskSeq" label="task seq." />
            <TextField source="name" label="name" />
            <SelectField source="connectorType" label="Task Type" choices={[
    			{ id: 'TRANSFORM_EXCHANGE_FLINK_SQLA2A', name: 'Stream SQL' },
    			{ id: 'TRANSFORM_EXCHANGE_SPARK_SQL', name: 'Batch SQL' },
    			{ id: 'TRANSFORM_EXCHANGE_FLINK_Script', name: 'Stream API' },
    			{ id: 'TRANSFORM_EXCHANGE_SPARK_STREAM', name: 'Batch API' },
  		        { id: 'TRANSFORM_EXCHANGE_FLINK_UDF',  name: 'Stream UDF/Jar' },
  		        { id: 'TRANSFORM_EXCHANGE_SPARK_UDF',  name: 'Batch UDF/Jar' },
  		        { id: 'TRANSFORM_MODEL_SPARK_STREAM', name: 'Model over Stream'},
  		        { id: 'TRANSFORM_MODEL_SPARK_TRAIN',  name: 'Model Training' },
            		        ]} />
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
		        <LongTextInput source="description" label="Task Description" style={{ width: 500 }} />
		        <SelectField source="connectorType" label="Task Type" validate={[ required ]} choices={[
    			{ id: 'TRANSFORM_EXCHANGE_FLINK_SQLA2A', name: 'Stream SQL' },
    			{ id: 'TRANSFORM_EXCHANGE_SPARK_SQL', name: 'Batch SQL' },
    			{ id: 'TRANSFORM_EXCHANGE_FLINK_Script', name: 'Stream API' },
    			{ id: 'TRANSFORM_EXCHANGE_SPARK_STREAM', name: 'Batch API' },
  		        { id: 'TRANSFORM_EXCHANGE_FLINK_UDF',  name: 'Stream UDF/Jar' },
  		        { id: 'TRANSFORM_EXCHANGE_SPARK_UDF',  name: 'Batch UDF/Jar' },
  		        { id: 'TRANSFORM_MODEL_SPARK_STREAM', name: 'Model over Stream'},
  		        { id: 'TRANSFORM_MODEL_SPARK_TRAIN',  name: 'Model Training' },
		        ]} />

            </FormTab>
            <FormTab label="Setting">
                <DisabledInput source="connectorConfig.cuid" label="ID or CUID or Name"/>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_SQLA2A">
                    <TextInput source="connectorConfig.topic_in" label="A Topic to Read Data" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.topic_out" label="A Topic to Write Data" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <LongTextInput source="connectorConfig.group_id" label="Consumer ID to Read Data. (Optional)" style={{ width: 500 }} />
		            <LongTextInput source="connectorConfig.sink_key_fields" label="List of Commas Separated Columns for Keys in Sink" style={{ width: 500 }} />
		            <LongTextInput source="connectorConfig.trans_sql" label="Stream SQL Query, such as select * from ..." validate={[ required ]} style={{ width: 500 }} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_SPARK_SQL">
                    <RadioButtonGroupInput source="connectorConfig.stream_back_flag" label="Stream Back" style={{ display: 'inline-block', width: 400}} choices={[
                                                        { id: 'true', name: 'Enable Stream back batch result to queue' },
                                                        { id: 'false', name: 'Disable stream back' },
                    ]} />
                    <RadioButtonGroupInput source="connectorConfig.choose_or_create" label="Topic and Schema" style={{ display: 'inline-block', marginLeft: 32, width: 400}} choices={[
                                        { id: 'true', name: 'Create a new topic with result schema' },
                                        { id: 'false', name: 'Choose an existing topic ' },
                    ]} />
                    <LongTextInput source="connectorConfig.stream_back_topic" label="Stream Back Topic" style={{ width: 500 }}/>
		            <LongTextInput source="connectorConfig.trans_sql" label="Spark SQL over Hive Queries (Comments --, Queries separate by ;)" validate={[ required ]} style={{ width: 500 }} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_MODEL_SPARK_TRAIN">
                    <LongTextInput source="connectorConfig.ml_pipe" label="Training Pipe Code" validate={[ required ]} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_Script">
                    <TextInput source="connectorConfig.topic_in" label="A Topic to Read Data" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.topic_out" label="A Topic to Write Data" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <LongTextInput source="connectorConfig.group_id" label="Consumer ID to Read Data. (Optional)" style={{ width: 500 }} />
		            <LongTextInput source="connectorConfig.sink_key_fields" label="List of Commas Separated Columns for Keys in Sink" style={{ width: 500 }} />
		            <LongTextInput source="connectorConfig.trans_script" label="Stream Statement" validate={[ required ]} style={{ width: 500 }} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_UDF">
                    <TextInput source="connectorConfig.trans_jar" label="UDF Jar File Name" validate={[ required ]} />
                </DependentInput>
	        </FormTab>
            <FormTab label="State">
                <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_SQLA2A">
                    <ReferenceManyField addLabel={false} reference="status" target="id">
                        <Datagrid>
                            <TextField source="jobId" label="Engine Job ID." />
                            <ChipField source="taskState" label="Engine Job State" />
                            <TextField source="subTaskId" label="Subtask ID." />
                            <ChipField source="status" label="Subtask State" />
                            <TextField source="name" label="Subtask Desc." />
                            <ShowButton />
                        </Datagrid>
                    </ReferenceManyField>
		        </DependentInput>
                <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_SPARK_SQL">
                    <DisabledInput source="jobConfig.livy_session_id" label="Livy Session ID" style={{ display: 'inline-block' }} />
                    <DisabledInput source="jobConfig.livy_session_state" label="Livy Session State" style={{ display: 'inline-block', marginLeft: 32 }} /><br />
                    <DisabledInput source="jobConfig.livy_statement_id" label="Livy Statement ID" style={{ display: 'inline-block' }} />
                    <DisabledInput source="jobConfig.livy_statement_state" label="Livy Statement State" style={{ display: 'inline-block', marginLeft: 32 }} />
                    <ChipField source="jobConfig.livy_statement_status" label="Query Status" />
                    <DependentInput dependsOn="status" value="FINISHED">
                        <RichTextField source="jobConfig.livy_statement_output" label="Last Query Result Set Preview - top 20 rows "/>
                    </DependentInput>
                    <DependentInput dependsOn="status" value="FAILED">
                        <RichTextField source="jobConfig.livy_statement_exception" label="Query Exceptions"/>
                    </DependentInput>
		        </DependentInput>
                <DependentInput dependsOn="connectorType" value="TRANSFORM_MODEL_SPARK_TRAIN">
                    <DisabledInput source="jobConfig.livy_session_id" label="Livy Session ID" style={{ display: 'inline-block' }} />
                    <DisabledInput source="jobConfig.livy_session_state" label="Livy Session State" style={{ display: 'inline-block', marginLeft: 32 }} /><br />
                    <DisabledInput source="jobConfig.livy_statement_id" label="Livy Statement ID" style={{ display: 'inline-block' }} />
                    <DisabledInput source="jobConfig.livy_statement_state" label="Livy Statement State" style={{ display: 'inline-block', marginLeft: 32 }} />
                    <ChipField source="jobConfig.livy_statement_status" label="Query Status" />
                    <DependentInput dependsOn="status" value="FINISHED">
                        <RichTextField source="jobConfig.livy_statement_output" label="Query Result Set Preview - top 20 rows "/>
                    </DependentInput>
                    <DependentInput dependsOn="status" value="FAILED">
                        <RichTextField source="jobConfig.livy_statement_exception" label="Query Exceptions"/>
                    </DependentInput>
		        </DependentInput>
            </FormTab>
        </TabbedForm>
    </Edit>
);

export const TransformCreate = (props) => (
    <Create title="Create New Transform Task Guide" {...props}>
        <TabbedForm>
            <FormTab label="Overview">
                <NumberInput source="taskSeq" label="Task Sequence Number, eg. 1, 2, ..." defaultValue={1}/>
                <TextInput source="name" label="Name" validate={[ required ]} style={{ width: 500 }} />
		        <LongTextInput source="description" label="Task Description" style={{ width: 500 }} />
                <SelectInput source="connectorType" label="Task Type" validate={[ required ]} choices={[
                        { id: 'TRANSFORM_EXCHANGE_FLINK_SQLA2A', name: 'Stream SQL' },
                        { id: 'TRANSFORM_EXCHANGE_SPARK_SQL', name: 'Batch SQL' },
                        { id: 'TRANSFORM_EXCHANGE_FLINK_Script', name: 'Stream API' },
                        { id: 'TRANSFORM_EXCHANGE_SPARK_STREAM', name: 'Batch API' },
                        { id: 'TRANSFORM_EXCHANGE_FLINK_UDF',  name: 'Stream UDF/Jar' },
                        { id: 'TRANSFORM_EXCHANGE_SPARK_UDF',  name: 'Batch UDF/Jar' },
                        { id: 'TRANSFORM_MODEL_SPARK_STREAM', name: 'Model over Stream'},
                        { id: 'TRANSFORM_MODEL_SPARK_TRAIN',  name: 'Model Training' }, ]} defaultValue='TRANSFORM_MODEL_SPARK_TRAIN' />
            </FormTab>
            <FormTab label="Setting">
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_SQLA2A">
                    <ReferenceArrayInput source="connectorConfig.topic_in" label="Choose a Read Topics" reference="schema" allowEmpty>
                        <SelectArrayInput optionText="subject" />
                    </ReferenceArrayInput>
                    <BooleanInput source="connectorConfig.choose_or_create" label="New or Choose Topic?" defaultValue={false} style={{ width: 500 }} />
                    <DependentInput dependsOn="connectorConfig.choose_or_create" value={true}>
                        <LongTextInput source="connectorConfig.topic_out" label="Auto Create a Write Topic" style={{ width: 500 }}/>
                    </DependentInput>
                    <DependentInput dependsOn="connectorConfig.choose_or_create" value={false}>
                        <ReferenceInput source="connectorConfig.topic_out" label="Choose a Write Topic Existed" reference="schema" validate={[ required ]} allowEmpty>
                            <AutocompleteInput optionText="subject" />
                        </ReferenceInput>
                    </DependentInput>
                    <LongTextInput source="connectorConfig.group_id" label="Consumer ID to Read Data. (Optional)" style={{ width: 500 }} />
		            <LongTextInput source="connectorConfig.sink_key_fields" label="Key Columns in Sink (separated by ,)" style={{ width: 500 }} />
		            <LongTextInput source="connectorConfig.trans_sql" label="Stream SQL Query (Comments --)" defaultValue="--Only single sql statement is supported" validate={[ required ]} style={{ width: 500 }} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_SPARK_SQL">
                    <BooleanInput source="connectorConfig.stream_back_flag" label="Stream Back the Result?" defaultValue={false} style={{ width: 500 }} />
                    <DependentInput dependsOn="connectorConfig.stream_back_flag" value={true}>
                        <BooleanInput source="connectorConfig.choose_or_create" label="Choose/Create Topic?" defaultValue={false} style={{ width: 500 }} />
                        <DependentInput dependsOn="connectorConfig.choose_or_create" value={true}>
                            <LongTextInput source="connectorConfig.stream_back_topic" label="Auto Create a Write Topic" style={{ width: 500 }}/>
                        </DependentInput>
                        <DependentInput dependsOn="connectorConfig.choose_or_create" value={false}>
                            <ReferenceInput source="connectorConfig.stream_back_topic" label="Choose a Write Topic Existed" reference="schema" validate={[ required ]} allowEmpty>
                                <AutocompleteInput optionText="subject" />
                            </ReferenceInput>
                        </DependentInput>
                    </DependentInput>
		            <LongTextInput source="connectorConfig.trans_sql" label="Batch SQL Queries (Comments --, Queries separate by ;)" defaultValue="--Result preview is only available for the last query." validate={[ required ]} style={{ width: 500 }} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_Script">
                    <TextInput source="connectorConfig.topic_in" label="A Topic to Read Data" style={{ display: 'inline-block' }} validate={[ required ]} />
                    <TextInput source="connectorConfig.topic_out" label="A Topic to Write Data" style={{ display: 'inline-block', marginLeft: 32 }} validate={[ required ]} />
                    <LongTextInput source="connectorConfig.group_id" label="Consumer ID to Read Data. (Optional)" style={{ width: 500 }} />
		            <LongTextInput source="connectorConfig.trans_script" label="Stream Script Statement, such as select(name).count()" validate={[ required ]} style={{ width: 500 }} />
		        </DependentInput>
		        <DependentInput dependsOn="connectorType" value="TRANSFORM_EXCHANGE_FLINK_UDF">
                    <TextInput source="connectorConfig.trans_jar" label="UDF Jar File Name" validate={[ required ]} />
		        </DependentInput>

				<DependentInput dependsOn="connectorType" value="TRANSFORM_MODEL_SPARK_TRAIN">
					<BooleanInput source="connectorConfig.ml_guide_enabled" label="Guideline Enabled?" defaultValue={true}/>
					<DependentInput dependsOn="connectorConfig.ml_guide_enabled" value={true}>
						<SelectInput source="connectorConfig.feature_source" label="Feature Source Data" validate={[ required ]} style={{ display: 'inline-block', float: 'left' }} choices={[
							{ id: 'FEATURE_SRC_FILE', name: 'File Libsvm|CSV' },
							{ id: 'FEATURE_SRC_HTABLE', name: 'Hive Table' },
							{ id: 'FEATURE_SRC_HQL', name: 'Hive Query' },
							]} defaultValue='FEATURE_SRC_FILE' />
                        <NumberInput source="connectorConfig.ml_feature_source_sample" label="Training Data Ratio %" defaultValue={100} step={20} style={{ display: 'inline-block', marginLeft: 32}} />
                        <BooleanInput source="connectorConfig.ml_feature_source_cache" label="Cache?" defaultValue={false} style={{ display: 'inline-block', marginLeft: 32}} />
						<DependentInput dependsOn="connectorConfig.ml_feature_source" value="FEATURE_SRC_FILE">
							<LongTextInput source="connectorConfig.ml_feature_source_value" label="File Path (Local file use file:///. process files based on the extension.)" defaultValue="/tmp/data/mllib/sample_libsvm_data.txt" style={{ width: 500 }} />
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.ml_feature_source" value="FEATURE_SRC_HTABLE">
							<LongTextInput source="connectorConfig.ml_feature_source_value" label="Hive Table/View Name (database_name.table/view_name)" style={{ width: 500 }} />
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.ml_feature_source" value="FEATURE_SRC_HQL">
							<LongTextInput source="connectorConfig.ml_feature_source_value" label="Hive/Spark SQL Query" style={{ width: 500 }} />
						</DependentInput>
						<BooleanInput source="connectorConfig.ml_feature_extract_enabled" label="Feature Extract?" defaultValue={false} style={{ width: 500 }} />
						<DependentInput dependsOn="connectorConfig.ml_feature_extract_enabled" value={true}>
                            <EmbeddedArrayInput source="connectorConfig.ml_feature_extract_array" label="">
                            <SelectInput source="method" label="Extract Method" validate={[ required ]} style={{ display: 'inline-block', float: 'left' }} choices={[
                                    { id: 'FEATURE_EXTRACT_TFIDF', name: 'TF-IDF' },
                                    { id: 'FEATURE_EXTRACT_W2V', name: 'Word to Vector' },
                                    { id: 'FEATURE_EXTRACT_CV', name: 'Count Vectorizer' },
                                    ]} defaultValue='FEATURE_EXTRACT_TFIDF' />
                            <TextInput source="inputCols" label="Set Input Columns (, as separator)" style={{ display: 'inline-block', marginLeft: 32}} />
                            <TextInput source="outputCol" label="Set Output Column" style={{ display: 'inline-block', marginLeft: 32}} />
                            </EmbeddedArrayInput>
						</DependentInput>
						<BooleanInput source="connectorConfig.ml_feature_transform_enabled" label="Feature Transform?" defaultValue={false} style={{ width: 500 }} />
						<DependentInput dependsOn="connectorConfig.ml_feature_transform_enabled" value={true}>
                            <EmbeddedArrayInput source="connectorConfig.ml_feature_transform_array" label="">
                            <SelectInput source="method" label="Transform Method" validate={[ required ]} style={{ display: 'inline-block', float: 'left' }} choices={[
                                    { id: 'FEATURE_TRANS_STRINGINDEX', name: 'String Indexer' },
                                    { id: 'FEATURE_TRANS_VA', name: 'Vector Assembler' },
                                    ]} defaultValue='FEATURE_TRANS_STRINGINDEX' />
                            <TextInput source="inputCols" label="Set Input Columns (, as separator)" style={{ display: 'inline-block', marginLeft: 32}} />
                            <TextInput source="outputCol" label="Set Output Column" style={{ display: 'inline-block', marginLeft: 32}} />
                            </EmbeddedArrayInput>
						</DependentInput>
						<BooleanInput source="connectorConfig.ml_feature_selector_enabled" label="Feature Selector?" defaultValue={false} style={{ width: 500 }} />
						    <DependentInput dependsOn="connectorConfig.ml_feature_selector_enabled" value={true}>
                                <EmbeddedArrayInput source="connectorConfig.ml_feature_selector_array" label="">
                                <SelectInput source="method" label="Transform Method" validate={[ required ]} style={{ display: 'inline-block', float: 'left' }} choices={[
                                        { id: 'FEATURE_SELE_VS', name: 'Vector Slicer' },
                                        { id: 'FEATURE_SELE_CHISQ', name: 'ChiSq Selector' },
                                        ]} defaultValue='FEATURE_SELE_VS' />
                                <TextInput source="inputCols" label="Set Input Columns (, as separator)" style={{ display: 'inline-block', marginLeft: 32}} />
                                <TextInput source="outputCol" label="Set Output Column" style={{ display: 'inline-block', marginLeft: 32}} />
                                </EmbeddedArrayInput>
                            </DependentInput>
						<SelectInput source="connectorConfig.ml_model_category" label="Choose Algorithm Category" style={{ display: 'inline-block', float: 'left' }} validate={[ required ]} choices={[
							{ id: 'ML_CLASS_CLF', name: 'Classification' },
							{ id: 'ML_CLASS_RES', name: 'Regression' },
							{ id: 'ML_CLASS_CLS', name: 'Clustering' },
							{ id: 'ML_CLASS_REC', name: 'Recommendation' },
							]} defaultValue='ML_CLASS_CLF' />
						<DependentInput dependsOn="connectorConfig.ml_model_category" value="ML_CLASS_CLF">
							<SelectInput source="connectorConfig.ml_model_class_method" label="Choose Algorithm" validate={[ required ]} style={{ display: 'inline-block', marginLeft: 32}} choices={[
							{ id: 'ML_CLASS_CLF_NB', name: 'Naive Bayes' },
							{ id: 'ML_CLASS_CLF_LR', name: 'Logistic Regression' },
							{ id: 'ML_CLASS_CLF_DTC', name: 'Decision Tree Classifier' },
							{ id: 'ML_CLASS_CLF_LSVM', name: 'Linear Support Vector Machine' },
							]} defaultValue='ML_CLASS_CLF_NB' />
							<DependentInput dependsOn="connectorConfig.ml_model_class_method" value="ML_CLASS_CLF_NB">
                                <Card style={{ width: 750 }}>
                                    <CardHeader
                                                     title="Naive Bayes Classifiers "
                                                     subtitle="algorithm information and parameters"
                                                     style={{ fontWeight: 'bold',  textAlign: 'left' }}
                                                     avatar={<Avatar backgroundColor="#008000" icon={<InfoIcon />} />}
                                    />
                                    <CardText>
                                    Naive Bayes classifiers are a family of simple probabilistic classifiers based on applying Bayes’ theorem with strong (naive) independence assumptions between the features.
                                    The current implementation supports both Multinomial naive Bayes and Bernoulli naive Bayes. For example, by converting documents into TF-IDF vectors, it can be used for document classification.
                                    By making every vector a binary (0/1) data, it can also be used as Bernoulli NB. The input feature values must be non-negative.
                                    <p></p>
                                    <div>Below is commonly used model algorithm parameters</div>
                                    <li><b>modelType</b>: "multinomial" or "bernoulli", default is "multinomial"</li>
                                    <li><b>featuresCol</b>: The name of the feature column, default is features</li>
                                    <li><b>labelCol</b>: The name of the label column, default is label</li>
                                    <li><b>predictionCol</b>: The name of the prediction column, default is prediction</li>
                                    <li><b>smoothing</b>: used to smooth categorical data. Quite commonly for model tuning and selection. </li>
                                    </CardText>
                                </Card>
                            </DependentInput>
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.ml_model_category" value="ML_CLASS_RES">
							<SelectInput source="connectorConfig.ml_model_class_method" label="Choose Algorithm" validate={[ required ]} style={{ display: 'inline-block', marginLeft: 32}} choices={[
							{ id: 'ML_CLASS_RES_LR', name: 'Linear Regression' },
							{ id: 'ML_CLASS_RES_DTR', name: 'Decision Tree Regression' },
							{ id: 'ML_CLASS_RES_RFR', name: 'Random Forest Regression' },
							{ id: 'ML_CLASS_RES_GBTR', name: 'Gradient-boosted Tree Regression' },
							]} defaultValue='ML_CLASS_RES_LR' />
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.ml_model_category" value="ML_CLASS_CLS">
							<SelectInput source="connectorConfig.ml_model_class_method" label="Choose Algorithm" validate={[ required ]} style={{ display: 'inline-block', marginLeft: 32}} choices={[
							{ id: 'ML_CLASS_CLS_KM', name: 'K-means' },
							{ id: 'ML_CLASS_CLS_LDA', name: 'Latent Dirichlet Allocation (LDA)' },
							{ id: 'ML_CLASS_CLS_BKM', name: 'Bisecting K-means' },
							{ id: 'ML_CLASS_CLS_GMM', name: 'Gaussian Mixture Model (GMM)' },
							]} defaultValue='ML_CLASS_CLS_KM' />
						</DependentInput>
					    <DependentInput dependsOn="connectorConfig.ml_model_category" value="ML_CLASS_REC">
							<SelectInput source="connectorConfig.ml_model_class_method" label="Choose Algorithm" validate={[ required ]} style={{ display: 'inline-block', marginLeft: 32}} choices={[
							{ id: 'ML_CLASS_REC_CF', name: 'Collaborative Filtering' },
							]} defaultValue='ML_CLASS_CLS_KM' />
						</DependentInput>
						<EmbeddedArrayInput source="connectorConfig.ml_model_para_overwrite_array" label="Add Model Parameters Overwrite">
                              <TextInput source="para_name" label="Model's Parameter Name" style={{ display: 'inline-block', float: 'left' }} />
                              <TextInput source="para_value" label="Model's Parameter Value" style={{ display: 'inline-block', marginLeft: 32}}/>
                        </EmbeddedArrayInput>
						<SelectInput source="connectorConfig.ml_model_evaluator" label="Disable or Choose Evaluator" validate={[ required ]} style={{ display: 'inline-block', float: 'left' }} choices={[
			            { id: 'ML_EVA_NA', name: 'Do Not Evaluate' },
						{ id: 'ML_EVA_RE', name: 'Regression' },
						{ id: 'ML_EVA_BC', name: 'Binary Classification' },
						{ id: 'ML_EVA_MC', name: 'Multiclass Classification' },
						]}  defaultValue='ML_EVA_NA' />
                        <DependentInput dependsOn="connectorConfig.ml_model_evaluator" value="ML_EVA_RE">
							<SelectInput source="connectorConfig.ml_model_evaluator_metric" label="Choose Evaluator Metric" validate={[ required ]} style={{ display: 'inline-block', marginLeft: 32}} choices={[
							{ id: 'rmse', name: 'Root Mean Squared Error' },
							{ id: 'mse', name: 'Mean Squared Error' },
							{ id: 'r2', name: 'r2 Metric' },
							{ id: 'mae', name: 'Mean Absolute Error' },
							]} defaultValue='rmse' />
						</DependentInput>
                        <DependentInput dependsOn="connectorConfig.ml_model_evaluator" value="ML_EVA_BC">
							<SelectInput source="connectorConfig.ml_model_evaluator_metric" label="Choose Evaluator Metric" validate={[ required ]} style={{ display: 'inline-block', marginLeft: 32}} choices={[
							{ id: 'areaUnderROC', name: 'Area Under ROC ' },
							{ id: 'areaUnderPR', name: 'Area Under PR' },
							]} defaultValue='areaUnderROC' />
						</DependentInput>
                        <DependentInput dependsOn="connectorConfig.ml_model_evaluator" value="ML_EVA_MC">
							<SelectInput source="connectorConfig.ml_model_evaluator_metric" label="Choose Evaluator Metric" validate={[ required ]} style={{ display: 'inline-block', marginLeft: 32}} choices={[
							{ id: 'f1', name: 'F1' },
							{ id: 'weightedPrecision', name: 'Weighted Precision' },
							{ id: 'weightedRecall', name: 'Weighted Recall' },
							{ id: 'accuracy', name: 'Accuracy' },
							]} defaultValue='f1' />
						</DependentInput>
						<BooleanInput source="connectorConfig.ml_model_tuning" label="Model Tuning?" defaultValue={false} options={{ labelPosition: 'right' }} style={{ width: 500 }} />
						<DependentInput dependsOn="connectorConfig.ml_model_tuning" value={true}>
                            <RadioButtonGroupInput source="connectorConfig.model_validator" label="Choose Validation Method" validate={[ required ]} style={{ width: 750 }} choices={[
                                { id: 'ML_VAL_CV',  name: 'Cross : Iterate folders of parameters grid with multiple data sets' },
                                { id: 'ML_VAL_TVS', name: 'Split : Iterate parameters grid with single ratio data sets' },
                                ]} defaultValue='ML_VAL_CV' />
                            <DependentInput dependsOn="connectorConfig.ml_model_validator" value="ML_VAL_CV">
                                <NumberInput source="connectorConfig.model_validator_fold" label="Number of Folds, such as 2" step={1}/>
                            </DependentInput>
                            <DependentInput dependsOn="connectorConfig.ml_model_validator" value="ML_VAL_TVS">
                                <NumberInput source="connectorConfig.model_validator_ratio" label="Training Data Ratio %, such as 80" step={10}/>
                            </DependentInput>
                            <EmbeddedArrayInput source="connectorConfig.ml_model_validator_para_array" label="Add Model Parameters Grid">
                                <TextInput source="para_name" label="Model's Parameter Name" style={{ display: 'inline-block', float: 'left' }} />
                                <TextInput source="para_value" label="Trail Values List (, as separator)" style={{ display: 'inline-block', marginLeft: 32}}/>
                            </EmbeddedArrayInput>
                        </DependentInput>
                        <BooleanInput source="connectorConfig.ml_model_persist_enabled" label="Model Persist?" defaultValue={false} options={{ labelPosition: 'right' }} style={{ width: 250 }} />
						<DependentInput dependsOn="connectorConfig.ml_model_persist_enabled" value={true}>
                            <LongTextInput source="connectorConfig.ml_model_persist_path" label="HDFS path to save the trained model" style={{ width: 500 }}/>
                        </DependentInput>
                    </DependentInput>
					<DependentInput dependsOn="connectorConfig.ml_guide_enabled" value={false}>
                        <SelectInput source="connectorConfig.ml_pipe_kind" label="Choose API" validate={[ required ]} choices={[
                                                    { id: 'pyspark', name: 'Python DataFrame' },
                                                    { id: 'spark', name: 'Scala DataFrame' },
                                                    { id: 'mlsql', name: 'Machine Learning SQL' }, ]} defaultValue='ML_PIPE_PYTHON' />
						<LongTextInput source="connectorConfig.ml_pipe" label="API Code" defaultValue="" />
					</DependentInput>
				</DependentInput>
            </FormTab>    
        </TabbedForm>
    </Create>
);

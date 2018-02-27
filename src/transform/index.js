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
                        <RichTextField source="jobConfig.livy_statement_output" label="Last Query Result Set Preview - top 10 rows "/>
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
		        <RadioButtonGroupInput source="connectorCategory" label="Category" choices={[
                                    { id: 'stream', name: 'Stream' },
                                    { id: 'batch', name: 'Batch' },
                ]} defaultValue='batch' />
                <DependentInput dependsOn="connectorCategory" value="stream" >
                    <SelectInput source="connectorType" label="Task Type" validate={[ required ]} choices={[
                        { id: 'TRANSFORM_EXCHANGE_FLINK_SQLA2A', name: 'Stream SQL' },
                        { id: 'TRANSFORM_EXCHANGE_FLINK_Script', name: 'Stream API' },
                        { id: 'TRANSFORM_EXCHANGE_FLINK_UDF',  name: 'Stream UDF/Jar' },
                        { id: 'TRANSFORM_MODEL_SPARK_STREAM', name: 'Model over Stream'},
                    ]} />
                </DependentInput>
                <DependentInput dependsOn="connectorCategory" value="batch">
                    <SelectInput source="connectorType" label="Task Type" validate={[ required ]} choices={[
                        { id: 'TRANSFORM_EXCHANGE_SPARK_SQL', name: 'Batch SQL' },
                        { id: 'TRANSFORM_EXCHANGE_SPARK_STREAM', name: 'Batch API' },
                        { id: 'TRANSFORM_EXCHANGE_SPARK_UDF',  name: 'Batch UDF/Jar' },
                        { id: 'TRANSFORM_MODEL_SPARK_TRAIN',  name: 'Model Training' },
                    ]} defaultValue='TRANSFORM_MODEL_SPARK_TRAIN'/>
		        </DependentInput>
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
						<SelectInput source="connectorConfig.feature_source" label="Feature Source" validate={[ required ]} style={{ display: 'inline-block', float: 'left' }} choices={[
							{ id: 'FEATURE_SRC_FILE', name: 'File Libsvm|CSV' },
							{ id: 'FEATURE_SRC_HTABLE', name: 'Hive Table' },
							{ id: 'FEATURE_SRC_HQL', name: 'Hive Query' },
							]} defaultValue='FEATURE_SRC_HQL' />
                        <NumberInput source="connectorConfig.feature_source_sample" label="Training Data Ratio %" defaultValue={100} step={20}/>
						<DependentInput dependsOn="connectorConfig.feature_source" value="FEATURE_SRC_FILE">
							<LongTextInput source="connectorConfig.feature_source_file_path" label="File Path (Local file use file:///. process files based on the extension.)" style={{ width: 500 }} />
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.feature_source" value="FEATURE_SRC_HTABLE">
							<LongTextInput source="connectorConfig.feature_source_hive_table" label="Hive Table/View Name (database_name.table/view_name)" style={{ width: 500 }} />
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.feature_source" value="FEATURE_SRC_HQL">
							<LongTextInput source="connectorConfig.feature_source_hive_query" label="Hive/Spark SQL Query" style={{ width: 500 }} />
						</DependentInput>
						<BooleanInput source="connectorConfig.feature_extract_enabled" label="Feature Extract?" defaultValue={false} style={{ width: 500 }} />
						<DependentInput dependsOn="connectorConfig.feature_extract_enabled" value={true}>
						<EmbeddedArrayInput source="connectorConfig.feature_extract_method_array" label="">
						<SelectInput source="method" label="Extract Method" validate={[ required ]} style={{ display: 'inline-block', float: 'left' }} choices={[
								{ id: 'FEATURE_EXTRACT_TFIDF', name: 'TF-IDF' },
								{ id: 'FEATURE_EXTRACT_W2V', name: 'Word to Vector' },
								{ id: 'FEATURE_EXTRACT_CV', name: 'Count Vectorizer' },
								]} defaultValue='FEATURE_EXTRACT_TFIDF' />
						<TextInput source="connectorConfig.feature_extract_para_input" label="Set Input Column"/>
						</EmbeddedArrayInput>
						</DependentInput>
						<BooleanInput source="connectorConfig.feature_transform_enabled" label="Feature Transform?" defaultValue={false} style={{ width: 500 }} />
						<DependentInput dependsOn="connectorConfig.feature_transform_enabled" value={true}>
						<EmbeddedArrayInput source="connectorConfig.feature_transform_method_array" label="">
						<SelectInput source="method" label="Transform Method" validate={[ required ]} style={{ display: 'inline-block', float: 'left' }} choices={[
								{ id: 'FEATURE_TRANS_STRINGINDEX', name: 'String Indexer' },
								{ id: 'FEATURE_TRANS_VA', name: 'Vector Assembler' },
								]} defaultValue='FEATURE_TRANS_STRINGINDEX' />
						<TextInput source="connectorConfig.feature_extract_para_input" label="Set Input Columns (, as separator)"/>
						</EmbeddedArrayInput>
						</DependentInput>
						<BooleanInput source="connectorConfig.feature_selector_enabled" label="Feature Selector?" defaultValue={false} style={{ width: 500 }} />
						<SelectInput source="connectorConfig.model_categry" label="Choose Algorithm Category" style={{ display: 'inline-block', float: 'left' }} validate={[ required ]} choices={[
							{ id: 'ML_CLASS_CLF', name: 'Classification' },
							{ id: 'ML_CLASS_RES', name: 'Regression' },
							{ id: 'ML_CLASS_CLS', name: 'Clustering' },
							{ id: 'ML_CLASS_REC', name: 'Recommendation' },
							]} defaultValue='ML_CLASS_CLF' />
						<DependentInput dependsOn="connectorConfig.model_categry" value="ML_CLASS_CLF">
							<SelectInput source="connectorConfig.model_class_method" label="Choose Algorithm" validate={[ required ]} choices={[
							{ id: 'ML_CLASS_CLF_NB', name: 'Naive Bayes' },
							{ id: 'ML_CLASS_CLF_LR', name: 'Logistic Regression' },
							{ id: 'ML_CLASS_CLF_DTC', name: 'Decision Tree Classifier' },
							{ id: 'ML_CLASS_CLF_LSVM', name: 'Linear Support Vector Machine' },
							]} defaultValue='ML_CLASS_CLF_NB' />
							<DependentInput dependsOn="connectorConfig.model_class_method" value="ML_CLASS_CLF_NB">
                                <Card style={{ width: 500 }}>
                                    <CardHeader
                                                     title="Naive Bayes Classifiers "
                                                     subtitle="algorithm information and parameters"
                                                     style={{ fontWeight: 'bold',  textAlign: 'left' }}
                                                     avatar={<Avatar backgroundColor="#008000" icon={<InfoIcon />} />}
                                    />
                                    <CardText>
                                    Naive Bayes classifiers are a family of simple probabilistic classifiers based on applying Bayes’ theorem with strong (naive) independence assumptions between the features.
                                    The current implementation supports both multinomial naive Bayes and Bernoulli naive Bayes. For example, by converting documents into TF-IDF vectors, it can be used for document classification.
                                    By making every vector a binary (0/1) data, it can also be used as Bernoulli NB (see here). The input feature values must be nonnegative.
                                    <p></p>
                                    <div>Below is commonly set parameters in the name format of set[Parameters]</div>
                                    <li>ModelType: "multinomial" as default or "bernoulli"</li>
                                    <li>FeaturesCol: The name of the feature column</li>
                                    <li>LabelCol: The name of the label column</li>
                                    <li>PredictionCol: : The name of the prediction column</li>
                                    </CardText>
                                </Card>
                            </DependentInput>
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.model_categry" value="ML_CLASS_RES">
							<SelectInput source="connectorConfig.model_class_method" label="Choose Algorithm" validate={[ required ]} choices={[
							{ id: 'ML_CLASS_RES_LR', name: 'Linear Regression' },
							{ id: 'ML_CLASS_RES_DTR', name: 'Decision Tree Regression' },
							{ id: 'ML_CLASS_RES_RFR', name: 'Random Forest Regression' },
							{ id: 'ML_CLASS_RES_GBTR', name: 'Gradient-boosted Tree Regression' },
							]} defaultValue='ML_CLASS_RES_LR' />
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.model_categry" value="ML_CLASS_CLS">
							<SelectInput source="connectorConfig.model_class_method" label="Choose Algorithm" validate={[ required ]} choices={[
							{ id: 'ML_CLASS_CLS_KM', name: 'K-means' },
							{ id: 'ML_CLASS_CLS_LDA', name: 'Latent Dirichlet Allocation (LDA)' },
							{ id: 'ML_CLASS_CLS_BKM', name: 'Bisecting K-means' },
							{ id: 'ML_CLASS_CLS_GMM', name: 'Gaussian Mixture Model (GMM)' },
							]} defaultValue='ML_CLASS_CLS_KM' />
						</DependentInput>
						<DependentInput dependsOn="connectorConfig.model_categry" value="ML_CLASS_REC">
							<SelectInput source="connectorConfig.model_class_method" label="Choose Algorithm" validate={[ required ]} choices={[
							{ id: 'ML_CLASS_REC_CF', name: 'Collaborative Filtering' },
							]} defaultValue='ML_CLASS_REC_CF' />
						</DependentInput>
					</DependentInput>
					<DependentInput dependsOn="connectorConfig.ml_guide_enabled" value={false}>
						<LongTextInput source="connectorConfig.ml_pipe" label="Build machine learning pipeline from API" defaultValue="--support coding in scala/python" validate={[ required ]} style={{ width: 500 }} />
					</DependentInput>
				</DependentInput>
            </FormTab>    
        </TabbedForm>
    </Create>
);

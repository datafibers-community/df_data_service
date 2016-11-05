# DataFibers Data Services Design

## Connect



## Transform
All final data will kept as Avro format. All interim data will keep as Json format since we'll have less efforts for schema
* Avro_Avro_SQL: Simple data transformation from Avro to Avro format
* Avro_Json_SQL: Usually play as preparing transformation for other Json transformations
* Json_Avro_SQL: Convert Json data to Avro data with schema created
* Json_Json_Router: Data to different topics on different conditions
* Json_Json_Fiter: Filter data for specific conditions
* Json_Json_LKP
* Json_Json_Exp
* Json_Json_Agg
* Json_Json_Copy
* Json_Json_Union
* Json_Json_Concat
* Json_Json_Merge
* Json_Json_Tag: Add/Tag additional data to the original topic

## Topics
#### Naming Conversion for Internal Topics
DF_INTER_TOPICS_TRANSFORMS_JOB-NAME_TASK-NAME_TRANSFORM-NAME_TOPIC-NAME. For example,
DF_INTER_TOPICS_TRANSFORMS_TestFlinkSql_TestFlinkSQLTask01_Avro_Json_SQL_testStage



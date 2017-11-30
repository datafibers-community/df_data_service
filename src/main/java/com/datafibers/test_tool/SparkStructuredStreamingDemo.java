/*
package com.datafibers.test_tool;

import com.databricks.spark.avro.SchemaConverters;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

*/
/**
 * Structured streaming demo using Avro'ed Kafka topic as input
 *//*

public class SparkStructuredStreamingDemo {

    private static Injection<GenericRecord, byte[]> recordInjection;
    private static StructType type;
    private static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"company_name\", \"type\":\"string\" },"
            + "  { \"name\":\"symbol\", \"type\":\"string\" },"
            + "  { \"name\":\"exchange\", \"type\":\"string\" },"
            + "  { \"name\":\"ask_size\", \"type\":\"int\" }"
            + "]}";
    private static Schema.Parser parser = new Schema.Parser();
    private static Schema schema = parser.parse(USER_SCHEMA);
    private static int schemaSize = schema.getFields().size();
    private static Object[] records = new Object[schemaSize];

    static { //once per VM, lazily
        recordInjection = GenericAvroCodecs.toBinary(schema);
        type = (StructType) SchemaConverters.toSqlType(schema).dataType();
    }

    public static void main(String[] args) throws StreamingQueryException {
        //set log4j programmatically
        LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        //configure Spark
        SparkConf conf = new SparkConf()
                .setAppName("kafka-structured")
                .setMaster("local[*]");

        //initialize spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        //reduce task number
        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");

        //data stream from kafka
        Dataset<Row> ds1 = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test_spark2")
                .option("startingOffsets", "earliest")
                .load();

        //start the streaming query
        sparkSession.udf().register("deserialize", (byte[] data) -> {
            byte[] transplant = new byte[data.length - 5];
            System.arraycopy(data, 5, transplant, 0, data.length - 5);
            GenericRecord record = recordInjection.invert(transplant).get();
            for(int i = 0; i < schemaSize; i ++) {
                if(schema.getFields().get(i).schema().getType().toString().equalsIgnoreCase("STRING")) {
                    records[i] = record.get(schema.getFields().get(i).name()).toString();
                } else {
                    records[i] = record.get(schema.getFields().get(i).name());
                }
            }
            return RowFactory.create(records);

        }, DataTypes.createStructType(type.fields()));
        ds1.printSchema();

        Dataset<Row> ds2 = ds1.selectExpr("cast(key as string) as keys", "deserialize(value) as rows")
                .select("rows.*");

        ds2.printSchema();

        StreamingQuery query1 = ds2
                .writeStream()
                .queryName("Test query")
                //.outputMode("complete")
                .format("console")
                .start();

        query1.awaitTermination();

    }
}
*/

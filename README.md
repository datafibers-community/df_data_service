# DataFibers Data Services
DataFibers (DF) - A pure streaming processing application on Kafka and Flink. 
The DF processor has two components defined to deal with stream ETL (Extract, Transform, and Load).
* **Connects** is to leverage Kafka Connect REST API on Confluent v.3.0.0 to landing or publishing data in or out of Apache Kafka.
* **Transforms** is to leverage streaming processing engine, such as Apache Flink, for data transformation.

## Building
 
You build the project using:

```
mvn clean package
```

## Testing

The application is tested using [vertx-unit](http://vertx.io/docs/vertx-unit/java/).

## Packaging

The application is packaged as a _fat jar_, using the 
[Maven Shade Plugin](https://maven.apache.org/plugins/maven-shade-plugin/).

## Running

Once packaged, just launch the _fat jar_ as follows ways

* Default with no parameters to launch standalone mode with web ui.
```
java -jar df-data-service-1.0-SNAPSHOT-fat.jar
```

* For more running features checking help option
```
java -jar df-data-service-1.0-SNAPSHOT-fat.jar -h
```

## Web UI
http://localhost:8000/admin/
<img src="https://raw.githubusercontent.com/datafibers/datafibers_web_src/master/themes/hugo-agency-theme/static/img/UI.PNG" width="800">

## Manual
https://www.gitbook.com/read/book/datafibers/datafibers-complete-guide

## Todo
- [x] Fetch all installed connectors/plugins in regularly frequency
- [x] Need to report connector or job status
- [x] Need an initial method to import all available|paused|running connectors from kafka connect
- [x] Add Flink Table API engine
- [ ] Add memory LKP
- [ ] Add Connects, Transforms Logging URL
- [ ] Add to generic function to do connector validation before creation
- [ ] Add submit other job actions, such as start, hold, etc
- [ ] Add Spark Structure Streaming
- [ ] Topic visualization
- [ ] Launch 3rd party jar
- [ ] Job level control and schedule
- [ ] Job metrics

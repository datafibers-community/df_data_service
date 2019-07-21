#!/usr/bin/env bash
mvn package -DskipTests; java -jar target/df-data-service-*.jar

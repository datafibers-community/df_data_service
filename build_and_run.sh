#!/usr/bin/env bash
mvn package -DskipTests; java -jar target/df-data-service-1.0-SNAPSHOT-fat.jar

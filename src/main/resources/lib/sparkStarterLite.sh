#!/bin/bash

export HADOOP_CONF_DIR=/etc/spark2/conf/yarn-conf

principal=$1
jar=$2
class=$3
queue=$4
loadingId=$5

driverMemory=1g
executorMemory=1g
maxExecutors=1
executorCores=1
memoryOverhead=1000
driverResultSize=1g

spark2-submit \
--master yarn \
--deploy-mode client \
--driver-memory $driverMemory \
--executor-memory $executorMemory \
--executor-cores $executorCores \
--conf spark.dynamicAllocation.maxExecutors=$maxExecutors \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.yarn.executor.memoryOverhead=$memoryOverhead \
--conf spark.driver.maxResultSize=$driverResultSize \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.hadoop.mapreduce.output.fileoutputformat.compress=true \
--conf spark.hadoop.mapreduce.output.fileoutputformat.compress.codec=lz4 \
--conf spark.io.compression.codec=lz4 \
--conf spark.broadcast.compress=true \
--conf spark.shuffle.compress=true \
--conf spark.shuffle.spill.compress=true \
--conf spark.rdd.compress=true \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.loadingId=$loadingId \
--queue $queue \
--principal $principal \
--keytab stork.keytab \
--class $class \
$jar

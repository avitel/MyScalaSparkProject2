#!/bin/bash

export HADOOP_CONF_DIR=/etc/spark2/conf/yarn-conf

principal=$1
jar=$2
class=$3
queue=$4
loadingId=$5
deltaMinus=$6
sourceUrl=$7
sourceUser=$8
passwordAliasQa=$9
sourceSchema=${10}
sourceTable=${11}
replicaSchema=${12}
replicaTable=${13}
primaryKeys=${14}
excludedColumns=${15}
replicaReader=${16}
ctlQaEntityId=${17}
samplePercent=${18}
jdbcDriver=${19}
jceksPathQa=${20}
replicaPartitionDivider=${21}
summaryGenerator=${22}

driverMemory=32g
executorMemory=32g
maxExecutors=300
executorCores=4
memoryOverhead=8000
driverResultSize=4g
kryoserializerBufferMax=512m
autoBroadcastJoinThreshold=104857600

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
--conf spark.kryoserializer.buffer.max=$kryoserializerBufferMax \
--conf spark.sql.autoBroadcastJoinThreshold=$autoBroadcastJoinThreshold \
--conf spark.loadingId=$loadingId \
--conf spark.deltaMinus=$deltaMinus \
--conf spark.jdbcDriver=$jdbcDriver \
--conf spark.sourceUrl=$sourceUrl \
--conf spark.sourceUser=$sourceUser \
--conf spark.jceksPathQa=$jceksPathQa \
--conf spark.passwordAliasQa=$passwordAliasQa \
--conf spark.sourceSchema=$sourceSchema \
--conf spark.sourceTable=$sourceTable \
--conf spark.replicaSchema=$replicaSchema \
--conf spark.replicaTable=$replicaTable \
--conf spark.primaryKeys=$primaryKeys \
--conf spark.excludedColumns=$excludedColumns \
--conf spark.replicaReader=$replicaReader \
--conf spark.summaryGenerator=$summaryGenerator \
--conf spark.replicaPartitionDivider=$replicaPartitionDivider \
--conf spark.ctlQaEntityId=$ctlQaEntityId \
--conf spark.samplePercent=$samplePercent \
--queue $queue \
--principal $principal \
--keytab stork.keytab \
--class $class \
$jar

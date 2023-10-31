#!/bin/bash

mvn -f ../pom.xml -DskipTests clean install &&
sshpass -p 'ONGzbF65JqYGkzp5xDSEP0sw' rsync ../target/hdfs-template.jar -r -v --progress -e ssh tuz_foms_hadoop_dev_cmp@ambari-sdp-01.gisoms-platform.dev2.pd15.foms.mtp:/home/tuz_foms_hadoop_dev_cmp/manual-deploy/hdfs-template.jar &&
sshpass -p 'ONGzbF65JqYGkzp5xDSEP0sw' ssh -p 9022 tuz_foms_hadoop_dev_cmp@ambari-sdp-01.gisoms-platform.dev2.pd15.foms.mtp "yarn application -list | awk '\$2 ~ \"hdfs-template\" { print \$1 }' | xargs yarn application -kill &> /dev/null || true" &&
sshpass -p 'ONGzbF65JqYGkzp5xDSEP0sw' ssh -p 9022 tuz_foms_hadoop_dev_cmp@ambari-sdp-01.gisoms-platform.dev2.pd15.foms.mtp SPARK_MAJOR_VERSION=2 spark-submit \
--name hdfs-template \
--conf spark.yarn.submit.waitAppCompletion=false \
--conf spark.hadoop.url=hadoopname-sdp-01.gisoms-platform.dev2.pd15.foms.mtp:8020 \
--class ru.element.lab.HdfsApplication \
--deploy-mode cluster \
--driver-memory 4g \
--executor-memory 3g \
--executor-cores 1 \
--master yarn \
--conf spark.memory.fraction=0.4 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.executorIdleTimeout=300 \
--conf spark.dynamicAllocation.cachedExecutorIdleTimeout=600s \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.dynamicAllocation.maxExecutors=1 \
--conf spark.dynamicAllocation.minExecutors=1 \
--jars /home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-aws-3.2.0.jar \
manual-deploy/hdfs-template.jar

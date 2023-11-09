#!/bin/bash

mvn -f ../pom.xml -DskipTests clean install &&
sshpass -p 'pass' rsync ../target/hdfs-template.jar -r -v --progress -e ssh login@host:/home/tuz_foms_hadoop_dev_cmp/manual-deploy/hdfs-template.jar &&
sshpass -p 'pass' ssh -p 9022 login@host "yarn application -list | awk '\$2 ~ \"hdfs-template\" { print \$1 }' | xargs yarn application -kill &> /dev/null || true" &&
sshpass -p 'pass' ssh -p 9022 login@host SPARK_MAJOR_VERSION=2 spark-submit \
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
--conf spark.driver.extraLibraryPath='spark.driver.extraClassPath=/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-aws-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-shaded-guava-1.1.1.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-client-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-common-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/guava-28.0-jre.jar' \
--jars '/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-aws-3.2.0.jar,/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-shaded-guava-1.1.1.jar,/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-client-3.2.0.jar,/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-common-3.2.0.jar,/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/guava-28.0-jre.jar' \
--driver-class-path '/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-aws-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-shaded-guava-1.1.1.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-client-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-common-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/guava-28.0-jre.jar' \
--conf 'spark.driver.extraClassPath=/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-aws-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-shaded-guava-1.1.1.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-client-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-common-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/guava-28.0-jre.jar' \
--conf 'spark.executor.extraClassPath=/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-aws-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-shaded-guava-1.1.1.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-client-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/hadoop-common-3.2.0.jar:/home/tuz_foms_hadoop_dev_cmp/spark3jars/emd2hdfsjars/guava-28.0-jre.jar' \
manual-deploy/hdfs-template.jar

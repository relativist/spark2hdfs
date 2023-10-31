package ru.element.lab;


import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class HdfsApplication {
    public static void main(String[] args) {
        final HdfsApplication app = new HdfsApplication();
        app.runTask();
    }

    private void runTask() {
        //final SparkConf sparkConf = confLocal();
        final SparkConf sparkConf = confHadoop(); //todo to hadoop

        SparkSession sparkSession = SparkSession.builder()
            .config(sparkConf).getOrCreate();

        //sparkSession.sparkContext().setCheckpointDir("/tmp/hdfs-checkpoint-3");//todo to2
        sparkSession.sparkContext().setCheckpointDir("/tmp/hdfs-checkpoint-2");//todo to2
        final String checkpointCurrent = sparkSession.sparkContext().checkpointDir().get();
        log.info(String.format("Hdfs checkpoint: %s", checkpointCurrent));

        String hdfsPath = "hdfs:///tmp/hdfs-app-pd15/oms/777/2019/1/dfd994da-7fbe-4dba-96ff-3d543c109d0c/";
        log.info(String.format("Manual build hdfs: %s", hdfsPath));

        String s3Path = "s3a://cmp-dev1/zip/zip.csv";
        log.info(String.format("Run1: %s", s3Path));


        List<String> stringList = Arrays.asList("строка 1", "строка 2", "строка 3");
        Dataset<String> dataset = sparkSession.createDataset(stringList, Encoders.STRING());

        dataset.show();
        System.out.println("Start");


        try {

            sparkSession.read().text(s3Path).show();

        } catch (Exception e) {
            log.error("EEEE:", e);
            e.printStackTrace();
        } finally {
            sparkSession.stop();
        }
    }


    public SparkConf confLocal() {
        SparkConf conf = new SparkConf()
            .setAppName("emd2hdfs")
            .setMaster("local");
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1");

        conf.set("fs.s3a.access.key", "7bb2e375b0b14c7d9c3152f49b5ee3c7");
        conf.set("fs.s3a.secret.key", "6611af62323644f2a3653e02c65904e6");
        conf.set("spark.hadoop.fs.s3a.endpoint", "https://s3.gos.sbercloud.dev/");
        conf.set("fs.s3a.endpoint", "https://s3.gos.sbercloud.dev/");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.path.style.access", "true");

        return conf;
    }

    public SparkConf confHadoop() {
        SparkConf conf = new SparkConf()
            .setAppName("emd2hdfs");
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1");

        conf.set("fs.s3a.access.key", "7bb2e375b0b14c7d9c3152f49b5ee3c7");
        conf.set("fs.s3a.secret.key", "6611af62323644f2a3653e02c65904e6");
        conf.set("spark.hadoop.fs.s3a.endpoint", "https://s3.gos.sbercloud.dev/");
        conf.set("fs.s3a.endpoint", "https://s3.gos.sbercloud.dev/");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.path.style.access", "true");

        return conf;
    }
}
package ru.element.lab;


import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
public class HdfsApplication {
    public static final Boolean IS_LOCAL_RUN = false;
    public static final String CHECKPOINT_SITNIKOV = "/tmp/hdfs-checkpoint-sitnikov";
    public static final String CHECKPOINT_TUZ = "/tmp/hdfs-checkpoint-tuz";

    public static final String S3_ENDPOINT = "https://s3.gos.sbercloud.dev/";
    public static final String S3_SECRET_KEY = "6611af62323644f2a3653e02c65904e6";
    public static final String S3_ACCESS_KEY = "7bb2e375b0b14c7d9c3152f49b5ee3c7";
    public static final String HDFS_URL = "hdfs://hadoopname-sdp-02.gisoms-platform.dev2.pd15.foms.mtp:8020/";

    public static void main(String[] args) {
        final HdfsApplication app = new HdfsApplication();

        //final String s3Key = "zip/zip.csv";
        final String s3Key = "100/RH11_1901164.xml"; // small
        //final String s3Key = "big_file/RH62_211001.xml"; //big
        final String bucket = "cmp-dev1";
        app.runTask(bucket, s3Key);

        //app.readS3();
    }

    private void readS3() {
        final SparkConf sparkConf = getConf();
        SparkSession sparkSession = SparkSession.builder()
            .config(sparkConf).getOrCreate();
        System.out.println("Start read.");
        try {
            String s3Path = "s3a://cmp-dev1/zip/zip.csv";
            sparkSession.read().text(s3Path).show();

        } catch (Exception e) {
            log.error("EEEE:", e);
            e.printStackTrace();
        } finally {
            sparkSession.stop();
        }
        System.out.println("Stop read.");
    }

    private void runTask(String bucket, String s3Key) {
        final OffsetDateTime start = OffsetDateTime.now();
        System.out.println("Start " + start);
        final SparkConf sparkConf = getConf();

        SparkSession sparkSession = SparkSession.builder()
            .config(sparkConf).getOrCreate();

        final String checkpointDir = IS_LOCAL_RUN ? CHECKPOINT_SITNIKOV : CHECKPOINT_TUZ;
        sparkSession.sparkContext().setCheckpointDir(checkpointDir);
        final String checkpointCurrent = sparkSession.sparkContext().checkpointDir().get();
        log.info(String.format("Hdfs checkpoint: %s", checkpointCurrent));

        final String originalFile = getOriginalFileName(s3Key);
        final String hdfsGzFilePath = checkpointDir + "/" + originalFile + ".gz";

        readS3AndStoreToHdfs(bucket, s3Key, hdfsGzFilePath, sparkSession);

        final Dataset<Row> ds = sparkSession.read()
            .format("gzip")
            .text(HDFS_URL + hdfsGzFilePath);
        ds.show(false);

        final String destPathParquet = IS_LOCAL_RUN ? checkpointDir : "/cmp/data/oms/777/2023/11" ;
        final UUID uuid = UUID.randomUUID();
        ds.write()
            .format("parquet")
            .option("compression", "gzip")
            .mode(SaveMode.Overwrite)
            .save(HDFS_URL + destPathParquet + "/" + uuid);
        System.out.println("Stored as " + uuid);

        new HdfsService(HDFS_URL).cleanFile(hdfsGzFilePath, sparkSession.sparkContext().hadoopConfiguration());

        if (!IS_LOCAL_RUN) {
            new HdfsService(HDFS_URL).cleanFile(checkpointDir, sparkSession.sparkContext().hadoopConfiguration());
        }

        System.out.println("Finished. Duration: " + Duration.between(start, OffsetDateTime.now()));
    }

    private void readS3AndStoreToHdfs(String bucket, String s3Key, String storedFilePath, SparkSession session) {
        final S3Service s3Service = new S3Service(S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY);
        final HdfsService hdfsService = new HdfsService(HDFS_URL);

        try (final InputStream inputStream = s3Service.getFile(bucket, s3Key)) {
            System.out.println("MANUAL FILE CONTENT: ");
            hdfsService.saveToHdfs(inputStream, storedFilePath);
        } catch (IOException e) {
            log.error("On read from s3 service: ", e);
        }
    }

    private String getOriginalFileName(String s3Key) {
        final int idx = s3Key.indexOf("/");
        if (idx >= 0) {
            return s3Key.substring(idx + 1);
        }
        return s3Key;
    }

    public SparkConf getConf() {
        SparkConf conf = new SparkConf()
            .setAppName("emd2hdfs")
            .setMaster("local");

        if (IS_LOCAL_RUN) {
            conf.setMaster("local");
            conf.set("spark.streaming.checkpointDir", HDFS_URL + CHECKPOINT_SITNIKOV);
        }
        /*conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0");
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-common:3.2.0");
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-client:3.2.0");*/

        conf.set("fs.s3a.access.key", S3_ACCESS_KEY);
        conf.set("fs.s3a.secret.key", S3_SECRET_KEY);
        conf.set("fs.s3a.endpoint", S3_ENDPOINT);
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("parquet.example.schema", "message data { required binary blob; }");

        return conf;
    }
}
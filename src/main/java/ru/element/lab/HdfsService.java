package ru.element.lab;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

@Slf4j
public class HdfsService {
    private final String hdfsUrl;

    public HdfsService(String hdfsUrl) {
        this.hdfsUrl = hdfsUrl;
    }

    public void saveToHdfs(InputStream inputStream, String destPath) {
        // Путь к файлу в HDFS
        String hdfsFilePath = hdfsUrl + destPath;

        // Конфигурация Hadoop
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);

        GzipCodec gzipCodec = new GzipCodec();
        gzipCodec.setConf(conf);

        try(FileSystem fs = FileSystem.get(conf);
            OutputStream outputStream = fs.create(new Path(hdfsFilePath));
            OutputStream gzipOutputStream = gzipCodec.createOutputStream(outputStream)) {

            // Копирование данных из InputStream в выходной поток
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                gzipOutputStream.write(buffer, 0, bytesRead);
            }

            System.out.println("Stored to HDFS." + hdfsFilePath);
        } catch (IOException e) {
            log.error("", e);
            e.printStackTrace();
        }
    }

    public void cleanFile(String storedFilePath, Configuration configuration) {
        try {
            FileSystem fs = FileSystem.get(new URI(hdfsUrl), configuration);
            Path filePath = new Path(storedFilePath);
            final ContentSummary cs = fs.getContentSummary(filePath);

            if (cs != null) {
                System.out.println(String.format("Clean %s,  dirs: %s,  files: %s,  length(bytes): %s,  length(human):%s",
                    storedFilePath,
                    cs.getDirectoryCount(),
                    cs.getFileCount(),
                    cs.getLength(),
                    FileUtils.byteCountToDisplaySize(cs.getLength())));
            }

            fs.delete(filePath, true);
        } catch (Exception e) {
            log.error(String.format("When delete checkpoint: %s", ExceptionUtils.getStackTrace(e)));
        }
    }
}

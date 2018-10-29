package com.pt.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class FileSystemOperation {
    public static Configuration conf;
    public static FileSystem fs;

    static {
        try {
            conf = new Configuration();
            conf.addResource(FileSystemOperation.class.getResourceAsStream("/hdfs/conf.xml"));
            fs = FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        String filePath = "/user/panteng/test.txt";
        //deleteFile(filePath);
        createFile(filePath, "第一个文件abc\n第二行");
        apendFile(filePath, "\n第三行");
        readFile(filePath).forEach(System.out::println);
    }

    public static void createFile(String filePath, String content) throws IOException {
        FSDataOutputStream outputStream = fs.createFile(new Path(filePath)).build();
        outputStream.write(content.getBytes("utf-8"));
        outputStream.flush();
        outputStream.close();
    }

    public static void deleteFile(String filePath) throws IOException {
        fs.deleteOnExit(new Path(filePath));
    }

    public static List<String> readFile(String path) throws IOException {
        FSDataInputStream inputStream = fs.open(new Path(path));
        return IOUtils.readLines(inputStream);
    }

    public static void apendFile(String path, String content) throws IOException {
        FSDataOutputStream outputStream = fs.append(new Path(path));
        outputStream.write(content.getBytes("utf-8"));
        outputStream.flush();
        outputStream.close();
    }
}

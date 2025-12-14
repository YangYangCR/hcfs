package com.data.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

public class HttpsFileSystemExample {

    public static void main(String[] args) throws Exception {
        // Hadoop 配置
        Configuration conf = new Configuration();

        // 如果访问 WebHDFS over HTTPS，可能需要以下设置
        conf.set("fs.https.impl", "org.apache.hadoop.fs.http.HttpFileSystem");
        conf.set("fs.https.impl.disable.cache", "true"); // 关闭缓存，方便调试

        // HttpsFileSystem URL（示例）
        String url = "http://192.168.32.130:8080/TDP/1";

        Path path = new Path(url);

        FileSystem fs = path.getFileSystem(conf);
        FileStatus fileStatus = fs.getFileStatus(path);
        System.out.println("xxx" + fileStatus.toString());


        fs.close();
    }
}

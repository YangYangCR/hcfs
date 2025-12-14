package com.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HCFSExample {

    public static void main(String[] args) throws Exception {

        // Hadoop 配置
        Configuration conf = new Configuration();

        // 使用本地文件系统（也是 HCFS）
        FileSystem fs = FileSystem.get(conf);

        // 要访问的路径（本地路径）
        Path path = new Path("file:///tmp");

        // 判断路径是否存在
        if (fs.exists(path)) {
            System.out.println("目录存在：" + path);

            // 列出文件
            FileStatus[] statuses = fs.listStatus(path);
            for (FileStatus status : statuses) {
                System.out.println(
                        (status.isDirectory() ? "DIR " : "FILE") +
                                " - " + status.getPath().getName()
                );
            }
        } else {
            System.out.println("目录不存在：" + path);
        }

        fs.close();
    }
}

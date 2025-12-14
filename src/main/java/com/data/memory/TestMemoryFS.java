package com.data.memory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class TestMemoryFS {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.memory.impl", "com.data.memory.MemoryFileSystem");

        FileSystem fs = FileSystem.get(new URI("memory:///"), conf);

        Path file = new Path("memory:///hello.txt");
        try (FSDataOutputStream out = fs.create(file, true)) {
            out.writeUTF("Hello HCFS!");
        }

        try (FSDataInputStream in = fs.open(file)) {
            System.out.println(in.readUTF()); // 输出: Hello HCFS!
        }
    }
}

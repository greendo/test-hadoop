package com.serg.testhadoop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

@SpringBootApplication
public class TestHadoopApplication {

	public static void main(String[] args) {
		SpringApplication.run(TestHadoopApplication.class, args);
	}

	static void test() {

		// Set HADOOP user
		System.setProperty("HADOOP_USER_NAME", "hduser");
		System.setProperty("hadoop.home.dir", "/");
		Path hdfswritepath = new Path("testpath/testfile.txt");
		String hdfsuri = "hdfs://vps613231.ovh.net:9000";
		// ====== Init HDFS File System Object
		Configuration conf = new Configuration();
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("fs.default.name", hdfsuri);
		conf.setBoolean("dfs.support.append", true);

		FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
		//Init output stream
		FSDataOutputStream outputStream = fs.create(hdfswritepath);
		//Cassical output stream usage
		outputStream.writeBytes("example text");
		outputStream.close();
	}

}


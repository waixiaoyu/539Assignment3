package com.yyy.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtils {

	private static final String host = "128.6.5.42";
	private static final String port = "9000";
	private static Configuration conf = null;

	private static Configuration getConfiguration() {
		if (conf == null) {
			conf = new Configuration();
			conf.set("mapred.jop.tracker", "hdfs://" + host + ":9001");
			conf.set("fs.default.name", "hdfs://" + host + ":9000");
		}
		return conf;
	}

	public static void write(String str) throws IOException {
		write(getConfiguration(), str);
	}

	public static String read(String filePath) throws IOException {
		return read(getConfiguration(), filePath);
	}

	public static void write(Configuration cf, String str) throws IOException {
		Configuration conf = cf;
		byte[] buff = str.getBytes();
		FileSystem hdfs = FileSystem.get(conf);
		Path dfs = new Path("hdfs://" + host + ":" + port + "/temp");
		FSDataOutputStream outputStream = hdfs.create(dfs);
		outputStream.write(buff, 0, buff.length);
		hdfs.close();
	}

	public static String read(Configuration cf, String filePath) throws IOException {
		Configuration conf = cf;
		String fileContent = null;
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		InputStream inputStream = null;
		ByteArrayOutputStream outputStream = null;
		try {
			inputStream = fs.open(path);
			outputStream = new ByteArrayOutputStream(inputStream.available());
			IOUtils.copyBytes(inputStream, outputStream, conf);
			fileContent = outputStream.toString();
		} finally {
			// following code is dangerous, because close the fs, it will affect
			// the context.write

			// IOUtils.closeStream(inputStream);
			// IOUtils.closeStream(outputStream);
			// fs.close();
		}
		return fileContent;
	}

	public static void main(String[] args) throws IOException {
		// write("456");
		// System.out.println(read("hdfs://" + host + ":" + port + "/temp"));
	}
}

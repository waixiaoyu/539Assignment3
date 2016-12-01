package com.yyy.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtils {
	private static Configuration conf = null;

	public static void deleteOutputDirectory(Configuration cf, Path outputPath) throws IOException {
		conf = cf;
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			System.out.println("Deleting output path before proceeding." + outputPath.getName());
			fs.delete(outputPath, true);
		}
	}
}

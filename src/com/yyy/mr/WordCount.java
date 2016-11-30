package com.yyy.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yyy.utils.HadoopUtils;

public class WordCount {

	public static enum Counters {
		LINE_NUMBER, WORDS_NUMBER
	};

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
			context.getCounter(Counters.LINE_NUMBER).increment(1);

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			context.getCounter(Counters.WORDS_NUMBER).increment(1);
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				System.out.println(key + "-" + val);
			}
			result.set(sum);
			context.write(key, result);

		}
	}

	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		HadoopUtils.deleteOutputDirectory(conf, new Path("hdfs://128.6.5.42:9000/out"));

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://128.6.5.42:9000/words.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://128.6.5.42:9000/out"));
		System.out.println((job.waitForCompletion(true) ? 0 : 1));
	}
}
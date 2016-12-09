package com.yyy.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.yyy.utils.HadoopUtils;

public class PGNCount3_2 {

	private static Logger log = Logger.getLogger(PGNCount3_2.class);

	private static String tempFilePath = "";
	private final static String SEPARATOR = "\t";

	private static final String host = "128.6.5.42";

	public static class TokenizerMapper extends Mapper<Object, Text, DoubleWritable, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(SEPARATOR);
			Double d = Double.valueOf(String.valueOf(strs[1])) + Double.valueOf("0." + String.valueOf(strs[2]));
			context.write(new DoubleWritable(d), new Text(strs[0]));
		}

	}

	public static class DoubleKeyDescComparator extends WritableComparator {

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		protected DoubleKeyDescComparator() {
			super(DoubleWritable.class, true);
		}

	}

	public static class KeySectionPartitioner<K, V> extends Partitioner<K, V> {

		public KeySectionPartitioner() {
		}

		@Override
		public int getPartition(K key, V value, int numReduceTasks) {
			// if the key is bigger than the max value, it must be in first
			// partition, that is return 0
			Double dKey = ((DoubleWritable) key).get();
			int maxValue = 1000;
			int keySection = 0;
			if (numReduceTasks > 1 && dKey < maxValue) {
				int sectionValue = maxValue / (numReduceTasks - 1);
				int count = 0;
				while ((dKey - sectionValue * count) > sectionValue) {
					count++;
				}
				keySection = numReduceTasks - 1 - count;
			}
			return keySection;
		}

	}

	public static class DoubleReducer extends Reducer<DoubleWritable, Text, Text, Text> {

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text val : values) {
				String strValue = key.toString().replace('.', '\t');
				context.write(val, new Text(strValue));
			}

		}
	}

	public static void main(String[] args) throws Exception {

		String outputPath = "hdfs://" + host + ":9000/out3_2";

		Configuration conf = new Configuration();

		HadoopUtils.deleteOutputDirectory(conf, new Path(outputPath));

		// Following two lines are used for hdfs i/o normally, when developing
		// remoting.
		conf.set("mapred.jop.tracker", "hdfs://" + host + ":9001");
		conf.set("fs.default.name", "hdfs://" + host + ":9000");

		tempFilePath = "hdfs://" + host + ":9000/temp" + String.valueOf(System.currentTimeMillis());

		Job job = Job.getInstance(conf, "pgn count");
		job.setJarByClass(PGNCount3_2.class);

		job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(DoubleReducer.class);

		job.setPartitionerClass(KeySectionPartitioner.class);

		job.setSortComparatorClass(DoubleKeyDescComparator.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://" + host + ":9000/out3_1/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.out.println((job.waitForCompletion(true) ? 0 : 1));

		// delete the temp file
		HadoopUtils.deleteOutputDirectory(conf, new Path(tempFilePath));
	}
}
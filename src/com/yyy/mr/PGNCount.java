package com.yyy.mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.yyy.utils.HadoopUtils;

import chesspresso.game.Game;
import chesspresso.pgn.PGNReader;

public class PGNCount {

	private static Logger log = Logger.getLogger(PGNCount.class);

	public static enum Counters {
		ROUND_NUMBER
	};

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final static Text oneAndRadio = new Text("1" + "\t" + "0");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			InputStream inputStream = new ByteArrayInputStream(value.getBytes());
			PGNReader pgnReader = new PGNReader(inputStream, "tmp name");

			Game game;
			try {
				while (true) {
					game = pgnReader.parseGame();
					if (game == null) {
						break;
					}
					// 0->white,1->draw,2->black
					switch (game.getResult()) {
					case 0:
						context.write(new Text("White"), oneAndRadio);
						break;
					case 1:
						context.write(new Text("Draw"), oneAndRadio);
						break;
					case 2:
						context.write(new Text("Black"), oneAndRadio);
						break;
					default:
						log.error("switch/case into default");
						break;
					}
					context.getCounter(Counters.ROUND_NUMBER).increment(1);
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Long lTotal = context.getCounter(Counters.ROUND_NUMBER).getValue();

			double radio = 0.0;
			int count = 0;
			int sum = 0;
			for (Text val : values) {
				sum += Integer.valueOf(val.toString().split("\t")[0]);
				if (val.toString().split("\t")[1].equals("0")) {
					radio += 1.0 / (double) lTotal;
				} else {
					radio += Double.valueOf(val.toString().split("\t")[1]);
				}
				count++;
			}
			context.write(key, new Text(String.valueOf(sum + "\t" + radio)));

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		HadoopUtils.deleteOutputDirectory(conf, new Path("hdfs://128.6.5.42:9000/out"));

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(PGNCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		WholeFileInputFormat.addInputPath(job, new Path("hdfs://128.6.5.42:9000/pgn/*"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://128.6.5.42:9000/out"));
		System.out.println((job.waitForCompletion(true) ? 0 : 1));
	}
}
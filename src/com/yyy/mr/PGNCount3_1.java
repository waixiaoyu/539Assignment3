package com.yyy.mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.yyy.inputformat.WholeFileInputFormat;
import com.yyy.utils.HadoopUtils;

import chesspresso.game.Game;
import chesspresso.pgn.PGNReader;

public class PGNCount3_1 {

	private static Logger log = Logger.getLogger(PGNCount3_1.class);

	private static String tempFilePath = "";
	private final static String SEPARATOR = "\t";

	private static final String host = "128.6.5.42";


	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final static String ONE = "1";

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
					/**
					 * key=Player1id Color, value=(opponent name) one
					 */
					Text tWhiteKey = new Text(game.getWhite());
					Text tBlackKey = new Text(game.getBlack());
					Text tWhiteValue = new Text(game.getBlack() + SEPARATOR + ONE);
					Text tBlackValue = new Text(game.getWhite() + SEPARATOR + ONE);

					context.write(tWhiteKey, tWhiteValue);
					context.write(tBlackKey, tBlackValue);
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}

	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			Set<String> setOpponents = new HashSet<>();
			for (Text val : values) {
				String[] strs = val.toString().split(SEPARATOR);
				setOpponents.add(strs[0]);
				sum += Integer.valueOf(strs[1]);

			}
			context.write(key, new Text(String.valueOf(sum) + SEPARATOR + String.valueOf(setOpponents.size())));

		}
	}

	public static void main(String[] args) throws Exception {

		String outputPath = "hdfs://" + host + ":9000/out3_1";

		Configuration conf = new Configuration();

		HadoopUtils.deleteOutputDirectory(conf, new Path(outputPath));

		// Following two lines are used for hdfs i/o normally, when developing
		// remoting.
		conf.set("mapred.jop.tracker", "hdfs://" + host + ":9001");
		conf.set("fs.default.name", "hdfs://" + host + ":9000");

		tempFilePath = "hdfs://" + host + ":9000/temp" + String.valueOf(System.currentTimeMillis());

		Job job = Job.getInstance(conf, "pgn count");
		job.setJarByClass(PGNCount3_1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		WholeFileInputFormat.addInputPath(job, new Path("hdfs://" + host + ":9000/pgn/*"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.out.println((job.waitForCompletion(true) ? 0 : 1));

		// delete the temp file
		HadoopUtils.deleteOutputDirectory(conf, new Path(tempFilePath));
	}
}
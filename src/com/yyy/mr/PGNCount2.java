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

import com.yyy.inputformat.WholeFileInputFormat;
import com.yyy.utils.HDFSUtils;
import com.yyy.utils.HadoopUtils;

import chesspresso.game.Game;
import chesspresso.pgn.PGNReader;

public class PGNCount2 {

	private static Logger log = Logger.getLogger(PGNCount2.class);

	private static String tempFilePath = "";
	private final static String SEPARATOR = "\t";

	private static final String host = "128.6.5.42";
	private static final String port = "9000";

	public static enum Counters {
		ROUND_NUMBER
	};

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final static String ONE = "1";
		private final static String ZERO = "0";
		private final static String WHITE = "White";
		private final static String BLACK = "Black";
		private static Text t110 = new Text(ONE + SEPARATOR + ONE + SEPARATOR + ZERO);
		private static Text t101 = new Text(ONE + SEPARATOR + ZERO + SEPARATOR + ONE);
		private static Text t100 = new Text(ONE + SEPARATOR + ZERO + SEPARATOR + ZERO);

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
					 * key=Player1id Color, value=(round number) (round number
					 * of winning againt higher-rate) (round number of losing
					 * againt lower-rate) (fight against higher)
					 */
					String strWhiteId = game.getWhite();
					Text tWhiteKey = new Text(strWhiteId + SEPARATOR + WHITE);
					String strBlackId = game.getBlack();
					Text tBlackKey = new Text(strBlackId + SEPARATOR + BLACK);

					Text tWhiteValue = null;
					Text tBlackValue = null;
					// 0->white,1->draw,2->black
					switch (game.getResult()) {
					case 0:
						if (game.getWhiteElo() > game.getBlackElo()) {
							tWhiteValue = new Text(t100 + SEPARATOR + ZERO);
							tBlackValue = new Text(t100 + SEPARATOR + ONE);

						} else {
							tWhiteValue = new Text(t110 + SEPARATOR + ONE);
							tBlackValue = new Text(t101 + SEPARATOR + ZERO);
						}
						break;
					case 1:
						if (game.getWhiteElo() > game.getBlackElo()) {
							tWhiteValue = new Text(t100 + SEPARATOR + ZERO);
							tBlackValue = new Text(t100 + SEPARATOR + ONE);

						} else {
							tWhiteValue = new Text(t100 + SEPARATOR + ONE);
							tBlackValue = new Text(t100 + SEPARATOR + ZERO);
						}
						break;
					case 2:
						if (game.getWhiteElo() > game.getBlackElo()) {
							tWhiteValue = new Text(t101 + SEPARATOR + ZERO);
							tBlackValue = new Text(t110 + SEPARATOR + ONE);
						} else {
							tWhiteValue = new Text(t100 + SEPARATOR + ONE);
							tBlackValue = new Text(t100 + SEPARATOR + ZERO);
						}
						break;
					default:
						log.error("switch/case into default");
						break;
					}
					context.write(tWhiteKey, tWhiteValue);
					context.write(tBlackKey, tBlackValue);
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}

	}

	public static class MyCombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int[] sum = new int[4];

			for (Text val : values) {
				String[] strs = val.toString().split(SEPARATOR);
				for (int i = 0; i < sum.length; i++) {
					sum[i] += Integer.valueOf(strs[i]);
				}
			}
			Text t = new Text(String.valueOf(sum[0]) + SEPARATOR + String.valueOf(sum[1]) + SEPARATOR
					+ String.valueOf(sum[2]) + SEPARATOR + String.valueOf(sum[3]));
			context.write(key, t);

		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int[] sum = new int[4];

			for (Text val : values) {
				String[] strs = val.toString().split(SEPARATOR);
				for (int i = 0; i < sum.length; i++) {
					sum[i] += Integer.valueOf(strs[i]);
				}
			}
			double radio1 = (double) sum[1] / (double) sum[3];
			double radio2 = (double) sum[2] / ((double) (sum[0] - sum[3]));
			Text t = new Text(
					String.valueOf(sum[0]) + SEPARATOR + (Double.isNaN(radio1) ? "na" : String.valueOf(radio1))
							+ SEPARATOR + (Double.isNaN(radio2) ? "na" : String.valueOf(radio2)));
			context.write(key, t);

		}
	}

	public static void main(String[] args) throws Exception {

		String outputPath = "hdfs://" + host + ":9000/out2";

		Configuration conf = new Configuration();

		HadoopUtils.deleteOutputDirectory(conf, new Path(outputPath));

		// Following two lines are used for hdfs i/o normally, when developing
		// remoting.
		conf.set("mapred.jop.tracker", "hdfs://" + host + ":9001");
		conf.set("fs.default.name", "hdfs://" + host + ":9000");

		tempFilePath = "hdfs://" + host + ":9000/temp" + String.valueOf(System.currentTimeMillis());

		Job job = Job.getInstance(conf, "pgn count");
		job.setJarByClass(PGNCount2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(MyCombiner.class);
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
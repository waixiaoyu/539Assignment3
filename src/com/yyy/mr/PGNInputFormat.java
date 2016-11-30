package com.yyy.mr;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

import chesspresso.game.Game;

public class PGNInputFormat<K, V> extends InputFormat<IntWritable, Game> {

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RecordReader<IntWritable, Game> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new PGNRecordReader();
	}

	public static class PGNRecordReader extends RecordReader<IntWritable, Game> {
		// data
		private LineReader in; // ������
		private boolean more = true;// ��ʾ��������û������

		private IntWritable key = null;
		private IntWritable value = null;

		// ���������浱ǰ��ȡ��λ�ã����ļ��е�λ�ã�
		private long start;
		private long end;
		private long pos;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public IntWritable getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Game getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

	}

}

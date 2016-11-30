package com.yyy.mr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class WholeFileInputFormat extends CombineFileInputFormat<NullWritable, Text> {

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		if (!(split instanceof CombineFileSplit)) {
			throw new IllegalArgumentException("split must be a CombineFileSplit");
		}
		return new CombineFileRecordReader<NullWritable, Text>((CombineFileSplit) split, context,
				WholeFileRecordReader.class);
	}
}

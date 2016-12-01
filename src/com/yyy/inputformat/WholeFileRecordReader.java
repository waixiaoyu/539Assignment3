package com.yyy.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.log4j.Logger;

public class WholeFileRecordReader extends RecordReader<NullWritable, Text> {
	private static final Logger LOG = Logger.getLogger(WholeFileRecordReader.class);

	/** The path to the file to read. */
	private Path mFileToRead;
	/** The length of this file. */
	private long mFileLength;

	/** The Configuration. */
	private Configuration mConf;

	/** Whether this FileSplit has been processed. */
	private boolean mProcessed;
	/** Single Text to store the file name of the current file. */
	// private final Text mFileName;
	/**
	 * Single Text to store the value of this file (the value) when it is read.
	 */
	private Text mFileText;

	public WholeFileRecordReader(CombineFileSplit fileSplit, TaskAttemptContext context, Integer pathToProcess) {
		mProcessed = false;
		mFileToRead = fileSplit.getPath(pathToProcess);
		mFileLength = fileSplit.getLength(pathToProcess);
		mConf = context.getConfiguration();

		assert 0 == fileSplit.getOffset(pathToProcess);
		if (LOG.isDebugEnabled()) {
			LOG.debug("FileToRead is: " + mFileToRead.toString());
			LOG.debug("Processing path " + pathToProcess + " out of " + fileSplit.getNumPaths());

			try {
				FileSystem fs = FileSystem.get(mConf);
				assert fs.getFileStatus(mFileToRead).getLen() == mFileLength;
			} catch (IOException ioe) {
				// oh well, I was just testing.
			}
		}

		// mFileName = new Text();
		mFileText = new Text();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!mProcessed) {
			if (mFileLength > (long) Integer.MAX_VALUE) {
				throw new IOException("File is longer than Integer.MAX_VALUE.");
			}
			byte[] contents = new byte[(int) mFileLength];

			FileSystem fs = mFileToRead.getFileSystem(mConf);
			FSDataInputStream in = null;
			try {
				// Set the contents of this file.
				in = fs.open(mFileToRead);
				IOUtils.readFully(in, contents, 0, contents.length);
				mFileText.set(contents, 0, contents.length);

			} finally {
				IOUtils.closeStream(in);
			}
			mProcessed = true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return mFileText;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (mProcessed) ? (float) 1.0 : (float) 0.0;
	}

	@Override
	public void close() throws IOException {
		mFileText.clear();
	}
}

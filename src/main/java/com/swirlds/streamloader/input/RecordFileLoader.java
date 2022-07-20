package com.swirlds.streamloader.input;

import com.swirlds.streamloader.data.RecordFile;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;

public interface RecordFileLoader {
	/**
	 * Start a thead that loads data files and calls callback
	 */
	void startLoadingRecordFiles(final ArrayBlockingQueue<Future<RecordFile>> recordFileQueue);
}

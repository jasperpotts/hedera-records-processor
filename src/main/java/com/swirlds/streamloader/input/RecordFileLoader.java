package com.swirlds.streamloader.input;

import com.swirlds.streamloader.data.RecordFile;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface RecordFileLoader {
	/**
	 * Start a thead that loads data files and calls callback
	 */
	void startLoadingRecordFiles(Consumer<RecordFile> recordFileConsumer);
}

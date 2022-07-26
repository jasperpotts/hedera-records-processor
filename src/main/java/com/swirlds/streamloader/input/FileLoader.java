package com.swirlds.streamloader.input;

import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.util.PipelineConsumer;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Interface for classes that can load record and account balances files
 */
public interface FileLoader {
	/**
	 * Start a thead that loads data files and puts results into queue
	 */
	void startLoadingRecordFileUrls(final PipelineConsumer<URL> consumer);

	/**
	 * Load the genesis balances file so that we have starting set of balances
	 *
	 * @return Map of account num and token type to balance
	 */
	ObjectLongHashMap<BalanceKey> loadInitialBalances();
}

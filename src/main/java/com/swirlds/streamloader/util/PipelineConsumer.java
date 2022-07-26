package com.swirlds.streamloader.util;

/**
 * Similar to java.util.Consumer but with added ability to notify after last item was sent
 *
 * @param <I> Type for data items to be consumed
 */
public interface PipelineConsumer<I> {
	/**
	 * Called to consume a new data item
	 *
	 * @param data the new data item
	 * @param isLast True if this is the last data item to be consumed
	 */
	void accept(final I data, final boolean isLast);
}

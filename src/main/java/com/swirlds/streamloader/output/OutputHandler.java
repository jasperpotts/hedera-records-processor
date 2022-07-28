package com.swirlds.streamloader.output;


import com.swirlds.streamloader.util.PipelineConsumer;

import java.util.List;

public interface OutputHandler<D> extends AutoCloseable, PipelineConsumer<List<D>> {
	@Override
	default void accept(final List<D> data, final boolean isLast) {
		accept(data);
	}

	void accept(final List<D> data);
}

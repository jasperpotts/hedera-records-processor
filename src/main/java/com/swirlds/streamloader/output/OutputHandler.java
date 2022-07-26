package com.swirlds.streamloader.output;


import com.swirlds.streamloader.data.JsonRow;
import com.swirlds.streamloader.util.PipelineConsumer;

import java.util.List;
import java.util.function.Consumer;

public interface OutputHandler extends AutoCloseable, PipelineConsumer<List<JsonRow>> {
	@Override
	default void accept(final List<JsonRow> data, final boolean isLast) {
		accept(data);
	}

	void accept(final List<JsonRow> data);
}

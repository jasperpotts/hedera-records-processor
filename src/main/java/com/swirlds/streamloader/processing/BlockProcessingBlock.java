package com.swirlds.streamloader.processing;

import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;

import static com.swirlds.streamloader.util.PrettyStatusPrinter.printStatusUpdate;

/**
 * Single threaded pipeline block that connects record files and computes block numbers and previous hashes
 */
public class BlockProcessingBlock extends PipelineBlock.Sequential<RecordFile, RecordFileBlock> {
	private long blockNumber = 0; // blocks start at 0
	private RecordFile prevRecordFile;

	public BlockProcessingBlock(PipelineLifecycle pipelineLifecycle) {
		super("block-processor", pipelineLifecycle);
	}

	@Override
	public RecordFileBlock processDataItem(final RecordFile recordFile) {
		final RecordFileBlock recordFileBlock = new RecordFileBlock(
				blockNumber,
				recordFile.hashOfPrevFile().orElseGet(() -> prevRecordFile == null ? null : prevRecordFile.hashOfThisFile()),
				recordFile
		);
		prevRecordFile = recordFile;
		blockNumber ++;

		// print status update
		printStatusUpdate(recordFile.endConsensusTime());
		return recordFileBlock;
	}
}

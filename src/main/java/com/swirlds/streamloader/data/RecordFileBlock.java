package com.swirlds.streamloader.data;

public record RecordFileBlock(
		long blockNumber,
		byte[] prevBlockHash,
		RecordFile recordFile
) {


}

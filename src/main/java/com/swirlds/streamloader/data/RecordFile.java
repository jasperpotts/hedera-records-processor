package com.swirlds.streamloader.data;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public record RecordFile(
		boolean isLastFile,
		ByteBuffer data,
		long fileNumber,
		long sizeBytes,
		byte[] hashOfThisFile,
		Future<byte[]> hashOfPrevFile,
		String fileName
) {
}
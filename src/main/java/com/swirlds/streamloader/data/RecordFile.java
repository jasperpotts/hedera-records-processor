package com.swirlds.streamloader.data;

import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionRecord;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * Record object for a RecordFile
 *
 * @param data record file bytes
 * @param fileVersion the version number of the record file, 1,2,5 or 6
 * @param hapiVersion the hapi API version as a string in the form "0.3.0"
 * @param sizeBytes the number of bytes the record file is
 * @param hashOfThisFile the sha384 hash of the whole record file
 * @param hashOfPrevFile the sha384 hash of the whole previous record file, empty optional for first file
 * @param fileName the record file name
 * @param transactions array of protobuf transactions
 * @param transactionRecords array of protobuf transaction records
 * @param startRunningHash the running hash after last transaction of previous file or empty bytes for first record file
 * @param endRunningHash the running hash after last transaction of this record file
 * @param startConsensusTime The consensus time of first transaction in file
 * @param endConsensusTime The consensus time of last transaction in file
 */
@SuppressWarnings("jol")
public record RecordFile(
		ByteBuffer data,
		long fileVersion,
		String hapiVersion,
		long sizeBytes,
		byte[] hashOfThisFile,
		Optional<byte[]> hashOfPrevFile,
		String fileName,
		Transaction[] transactions,
		TransactionRecord[] transactionRecords,
		byte[] startRunningHash,
		byte[] endRunningHash,
		long startConsensusTime,
		long endConsensusTime
) {


}

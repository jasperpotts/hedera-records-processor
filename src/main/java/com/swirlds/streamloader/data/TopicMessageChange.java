package com.swirlds.streamloader.data;

/**
 * Records the change of a single TopicMessage instance
 *
 * @param consensusTimeStamp the consensus time for when the change happened
 * @param message the message (as a byte array)
 * @param runningHash running hash of the message
 * @param sequenceNumber number of messages (so far) on this topic
 * @param runningHashVersion TBD
 * @param chunkNumber for fragmented topic messages, which chunk is the current one
 * @param chunkTotal for fragmented topic messages, total number of chunks for the message
 * @param payerAccountId the AccountNumber part of the AccountID of the sender
 * @param topicId the topidId of the message
 * @param initialTransactionId TBD
 * @param nonce the nonce from the overall messageBody
 * @param scheduled whether or the overall messageBody was scheduled
 * @param consensusStartTimeStamp a second timestamp for the start of this message.
 */
public record TopicMessageChange(
		long consensusTimeStamp,
		byte[] message,
		byte[] runningHash,
		int sequenceNumber,
		int runningHashVersion,
		int chunkNumber,
		int chunkTotal,
		long payerAccountId,
		int topicId,
		String initialTransactionId,
		int nonce,
		boolean scheduled,
		long consensusStartTimeStamp
) {
	@Override
	public String toString() {
		return "TopicMessageChange{" +
				"consensusTimeStamp=" + consensusTimeStamp +
				", message=" + message +
				", runningHash=" + runningHash +
				", sequenceNumber=" + sequenceNumber +
				", runningHashVersion=" + runningHashVersion +
				", chunkNumber=" + chunkNumber +
				", chunkTotal=" + chunkTotal +
				", payerAccountId=" + payerAccountId +
				", topicId=" + topicId +
				", initialTransactionId=" + initialTransactionId +
				", nonce=" + nonce +
				", scheduled=" + scheduled +
				", consensusStartTimeStamp=" + consensusStartTimeStamp +
				'}';
	}
}

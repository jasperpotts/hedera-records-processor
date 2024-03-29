package com.swirlds.streamloader.data;

import java.nio.ByteBuffer;

/**
 * Records the change of a single NFT instance
 *
 * @param consensusTimeStamp the consensus time for when the change happened
 * @param accountNum the AccountNumber of the AccountID that owns the NFT
 * @param deleted whether the NFT is deleted
 * @param metadata String (up to 100 bytes) associated with this NFT instance
 * @param serialNumber the first of two longs that identifies the NFT instance
 * @param tokenId the second of two longs that identifies the NFT instance
 * @param delegatingSpender TBD
 * @param spender TBD
 */
public record NftChange(
		long consensusTimeStamp,
		long accountNum,
		boolean deleted,
		ByteBuffer metadata,
		long serialNumber,
		long tokenId,
		long delegatingSpender,
		long spender
) {
	@Override
	public String toString() {
		return "NftChange{" +
				"consensusTimeStamp=" + consensusTimeStamp +
				", accountNum=" + accountNum +
				", deleted=" + deleted +
				", metadata=" + metadata +
				", serialNumber=" + serialNumber +
				", tokenId=" + tokenId +
				", delegatingSpender=" + delegatingSpender +
				", spender=" + spender +
				'}';
	}
}

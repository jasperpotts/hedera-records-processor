package com.swirlds.streamloader.data;

/**
 * Records the change of a single NFT instance
 *
 * @param consensusTimeStamp the consensus time for when the change happened
 * @param modifiedTimeStamp the modification time for when the change happened
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
		long modifiedTimeStamp,
		long accountNum,
		boolean deleted,
		String metadata,
		long serialNumber,
		long tokenId,
		long delegatingSpender,
		long spender
) {
	@Override
	public String toString() {
		return "NftChange{" +
				"consensusTimeStamp=" + consensusTimeStamp +
				", modifiedTimeStamp=" + modifiedTimeStamp +
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

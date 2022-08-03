package com.swirlds.streamloader.data;

/**
 * Records the change of a single Allownce instance
 *
 * @param consensusTimeStamp the consensus time for when the change happened
 * @param entityId the AccountNumber part of the Entity type of the Allowance
 * @param payerAccountId the AccountNumber part of the AccountID with giving the Allowance
 * @param allowanceType description of the type of Allowance this is
 * @param amount number of units in the allowance
 * @param isApproval TBD
 * @param errata TBD
 * @param tokenId the AccountNumber part of the tokenID of the Allowance.
 */
public record AllowanceChange(
		long consensusTimeStamp,
		long entityId,
		long payerAccountId,
		String allowanceType,
		long amount,
		boolean isApproval,
		String errata,
		long tokenId
) {
	@Override
	public String toString() {
		return "AllowanceChange{" +
				"consensusTimeStamp=" + consensusTimeStamp +
				", entityId=" + entityId +
				", payerAccountId=" + payerAccountId +
				", allowanceType=" + allowanceType +
				", amount=" + amount +
				", isApproval=" + isApproval +
				", errata=" + errata +
				", tokenId=" + tokenId +
				'}';
	}
}

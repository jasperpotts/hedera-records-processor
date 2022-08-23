package com.swirlds.streamloader.data;

/**
 * Records the change of a single Allownce instance
 *
 * @param consensusTimeStamp the consensus time for when the change happened
 * @param owner the AccountNumber part of the owner of the Allowance
 * @param spender the AccountNumber part of the AccountID pf the spender
 * @param allowanceType description of the type of Allowance this is
 * @param amount number of units in the allowance
 * @param isApproval TBD
 * @param tokenId the AccountNumber part of the tokenID of the Allowance.
 * @param timestampStart the nanosecond where the allowance starts.
 * @param timestampEnd the nanosecond where the allowance ends.
 * @param payerAccountId the AccountNumber part of the payerAccount.
 */
public record AllowanceChange(
		long consensusTimeStamp,
		long owner,
		long spender,
		String allowanceType,
		long amount,
		boolean isApproval,
		long tokenId,
		long timestampStart,
		long timestampEnd,
		long payerAccountId
) {
	@Override
	public String toString() {
		return "AllowanceChange{" +
				"consensusTimeStamp=" + consensusTimeStamp +
				", owner=" + owner +
				", spender=" + spender +
				", allowanceType=" + allowanceType +
				", amount=" + amount +
				", isApproval=" + isApproval +
				", tokenId=" + tokenId +
				", timestampStart=" + timestampStart +
				", timestampEnd=" + timestampEnd +
				", payerAccountId=" + payerAccountId +
				'}';
	}
}

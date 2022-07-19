package com.swirlds.streamloader.data;

/**
 * Records the change of balance of one account
 *
 * @param accountNum
 * @param balanceChangeTinyBar
 * @param consensusTimeStamp
 */
public record BalanceChange(
		long accountNum,
		long balanceChangeTinyBar,
		long consensusTimeStamp
) {
	@Override
	public String toString() {
		return "BalanceChange{" +
				"accountNum=" + accountNum +
				", balanceChangeTinyBar=" + balanceChangeTinyBar +
				", consensusTimeStamp=" + consensusTimeStamp +
				'}';
	}
}

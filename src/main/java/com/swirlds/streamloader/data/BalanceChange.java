package com.swirlds.streamloader.data;

/**
 * Records the change of balance of one account
 *
 * @param accountNum The entity num for account effected
 * @param tokenTypeEntityNum  the entity num of the token this balance is for 0 = HBAR
 * @param balanceChange the delta of balance, positive or negative
 * @param consensusTimeStamp the consensus time for when the change happened
 */
public record BalanceChange(
		long accountNum,
		long tokenTypeEntityNum,
		long balanceChange,
		long consensusTimeStamp
) {
	public static final long HBAR_TOKEN_TYPE = 0;
	@Override
	public String toString() {
		return "BalanceChange{" +
				"accountNum=" + accountNum +
				", tokenTypeEntityNum=" + tokenTypeEntityNum +
				", balanceChange=" + balanceChange +
				", consensusTimeStamp=" + consensusTimeStamp +
				'}';
	}
}

package com.swirlds.streamloader.input;

import com.swirlds.streamloader.data.BalanceChange;
import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.util.PipelineConsumer;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class GoogleStorageFileLoaderTest {

	@Test
	void startLoadingRecordFileUrls() {
		FileLoader recordFileLoader = new GoogleStorageFileLoader(
				GoogleStorageFileLoader.HederaNetwork.MAINNET,
				"0.0.3"
		);

		final CountDownLatch countDownLatch = new CountDownLatch(100);

		recordFileLoader.startLoadingRecordFileUrls(new PipelineConsumer<URL>() {
			@Override
			public void accept(final URL url, final boolean isLast) {
				System.out.println("isLast = "+isLast+" -- url = " + url);
				countDownLatch.countDown();
				assertFalse(isLast);
			}
		});

		assertDoesNotThrow(() -> {
			final var result = countDownLatch.await(20, TimeUnit.SECONDS);
			assertTrue(result);
		});
	}

	@Test
	void loadInitialBalances() {
		FileLoader recordFileLoader = new GoogleStorageFileLoader(
				GoogleStorageFileLoader.HederaNetwork.MAINNET,
				"0.0.3"
		);
		final ObjectLongHashMap<BalanceKey> balances = recordFileLoader.loadInitialBalances();
		System.out.println("balances.size() = " + balances.size());
		for (int i = 3; i < 20; i++) {
			balances.get(new BalanceKey(i, BalanceChange.HBAR_TOKEN_TYPE));
			System.out.println("balances.get(new BalanceKey("+i+", BalanceChange.HBAR_TOKEN_TYPE)) = " + balances.get(
					new BalanceKey(i, BalanceChange.HBAR_TOKEN_TYPE)));
		}
		assertEquals(387737948847L, balances.get(new BalanceKey(3, BalanceChange.HBAR_TOKEN_TYPE)));
		assertEquals(16770669109L, balances.get(new BalanceKey(4, BalanceChange.HBAR_TOKEN_TYPE)));
		assertEquals(15368, balances.size());
		assertFalse(balances.isEmpty());
	}
}
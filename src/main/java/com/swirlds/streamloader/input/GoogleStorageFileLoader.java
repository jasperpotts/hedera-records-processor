package com.swirlds.streamloader.input;

import com.google.cloud.storage.Blob;
import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.util.GoogleStorageHelper;
import com.swirlds.streamloader.util.PipelineConsumer;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.ByteBuffer;

import static com.swirlds.streamloader.input.BalancesLoader.INITIAL_BALANCE_FILE_NAME;
import static com.swirlds.streamloader.input.BalancesLoader.loadBalances;


public class GoogleStorageFileLoader implements FileLoader {

	static {
		// hack URL so that it accepts gs: as valid protocol
		URL.setURLStreamHandlerFactory(protocol -> "gs".equals(protocol) ? new URLStreamHandler() {
			protected URLConnection openConnection(URL url) {
				return new URLConnection(url) {
					public void connect() {}
				};
			}
		} : null);
	}
	// create gcp client
	public enum HederaNetwork {
		DEMO("hedera-demo-streams"),
		MAINNET("hedera-mainnet-streams"),
		TESTNET("hedera-stable-testnet-streams-2020-08-27"),
		PREVIEWNET("hedera-preview-testnet-streams");

		private final String bucketName;

		HederaNetwork(final String bucketName) {
			this.bucketName = bucketName;
		}

		public String getBucketName() {
			return bucketName;
		}
	}

	private final HederaNetwork network;
	private final String nodeID;

	public GoogleStorageFileLoader(final HederaNetwork network, final String nodeID) {
		this.network = network;
		this.nodeID = nodeID;
	}

	@Override
	public void startLoadingRecordFileUrls(final PipelineConsumer<URL> consumer) {
		// create pool of thread to do file downloading
		final Thread directoryLister = new Thread(() -> {
			final var directoryIterator = GoogleStorageHelper.listDirectory(network.getBucketName(),"recordstreams/record" + nodeID + "/");
			while(directoryIterator.hasNext()) {
				// get next blob to download
				final Blob recordFileBlob = directoryIterator.next();
				// only process records files
				if (recordFileBlob.getName().endsWith(".rcd")) {
					// add a task to download and parse
					try {
						final URL url = new URL(recordFileBlob.getBlobId().toGsUtilUri());
						consumer.accept(url, !directoryIterator.hasNext());
					} catch (MalformedURLException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}, "directory-lister");
		// start threads
		directoryLister.start();
	}

	@Override
	public ObjectLongHashMap<BalanceKey> loadInitialBalances() {
		try {
			final ByteBuffer accountBalances = GoogleStorageHelper.downloadBlob(
					new URL("gs://"+network.getBucketName()+"/accountBalances/balance"+ nodeID +"/"+ INITIAL_BALANCE_FILE_NAME));
			return loadBalances(accountBalances,INITIAL_BALANCE_FILE_NAME);
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}
}

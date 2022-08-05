package com.swirlds.streamloader.util;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import javax.json.Json;
import javax.json.stream.JsonParser;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class GoogleStorageHelper {
	private static final String gcpProjectName;
	private static final Storage storage;
	private static final Storage.BlobSourceOption blobOptions;

	static {
		// get the project name from credentials file
		final String googleCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
		if (googleCredentials == null || googleCredentials.length() == 0) {
			throw new RuntimeException("You need to set \"GOOGLE_APPLICATION_CREDENTIALS\" environment variable");
		}
		try {
			final JsonParser parser = Json.createParser(Files.newBufferedReader(Path.of(googleCredentials)));
			parser.next();
			var object = parser.getObject();
			gcpProjectName = object.getString("project_id",null);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (gcpProjectName == null) {
			throw new IllegalStateException(
					"Could not talk to GCP as can not read project id from GOOGLE_APPLICATION_CREDENTIALS");
		}
		storage = StorageOptions.newBuilder().setProjectId(gcpProjectName).build().getService();
		blobOptions = Storage.BlobSourceOption.userProject(gcpProjectName);
	}


	public static ByteBuffer downloadBlob(URL url) {
		BlobId blobId = BlobId.fromGsUtilUri(url.toString());
		for (int i = 0; i < 3; i++) {
			try {
				return ByteBuffer.wrap(storage.readAllBytes(blobId, blobOptions));
			} catch (StorageException se) {
				System.out.println("StorageException TRY "+(i+1)+" to download " + url);
				se.printStackTrace();
			}
		}
		throw new RuntimeException("Failed after 3 tries to download: "+url);
	}

	public static void uploadBlob(String bucketName, String path, byte[] data) {
		System.out.println("\nUploading file to google : gs://"+ bucketName+"/"+path);
		final BlobId blobId = BlobId.of(bucketName, path);
		final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		for (int i = 0; i < 3; i++) {
			try {
				storage.create(blobInfo, data);
			} catch (StorageException se) {
				System.out.println("StorageException TRY "+(i+1)+" to upload " + blobId.toGsUtilUri());
				se.printStackTrace();
			}
		}
	}

	public static Iterator<Blob> listDirectory(final String bucketName, String gcpDirectoryPath) {
		// List the file in the bucket
		final var blobs = storage.list(bucketName,
				Storage.BlobListOption.prefix(gcpDirectoryPath),
				Storage.BlobListOption.currentDirectory(),
				Storage.BlobListOption.userProject(gcpProjectName),
				Storage.BlobListOption.pageSize(10_000));
		return blobs.iterateAll().iterator();
	}
}

package com.swirlds.streamloader.util;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import javax.json.Json;
import javax.json.stream.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardOpenOption.READ;

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
		uploadBlob(bucketName,path,data,0,data.length);
	}
	public static void uploadBlob(String bucketName, String path, byte[] data, int offset, int length) {
		System.out.println("\nUploading file to google : gs://"+ bucketName+"/"+path);
		final BlobId blobId = BlobId.of(bucketName, path);
		final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		for (int i = 0; i < 3; i++) {
			try {
				storage.create(blobInfo, data, offset, length);
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

	private static final ThreadLocal<ByteBuffer> inputBuffers = new ThreadLocal<>();
	private static final ThreadLocal<ByteBuffer> outputBuffers = new ThreadLocal<>();

	private static ByteBuffer getBuffer(ThreadLocal<ByteBuffer> threadLocal, int size, boolean direct) {
		ByteBuffer buf = threadLocal.get();
		if (buf == null || buf.capacity() < size) {
			buf = direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
			threadLocal.set(buf);
		}
		return buf.clear();
	}

	public static void uploadFile(String bucketName, Path filePath, String filePathInBucket) {
		try {
			// read whole file into a ByteBuffer
			final int fileSize = (int)Files.size(filePath);
			final ByteBuffer inputBuffer = getBuffer(inputBuffers, fileSize, false);
			try (final var channel = Files.newByteChannel(filePath, READ)) {
				channel.read(inputBuffer);
			}
			inputBuffer.flip();
			// upload
			uploadBlob(bucketName, filePathInBucket, inputBuffer.array());
		} catch (IOException e) {
			Utils.failWithError(e);
		}
	}

	public static void compressAndUploadFile(String bucketName, Path filePath, String filePathInBucket) {
		try {
			// read whole file
			final int fileSize = (int)Files.size(filePath);
			final ByteBuffer inputBuffer = getBuffer(inputBuffers, fileSize, false);
			try (final var channel = Files.newByteChannel(filePath, READ)) {
				channel.read(inputBuffer);
			}
			inputBuffer.flip();
			// Compress the bytes
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
				gzip.write(inputBuffer.array());
			} catch (IOException e) {
        			Utils.failWithError(e);
    			}
			// upload
			GoogleStorageHelper.uploadBlob(bucketName, filePathInBucket, baos.toByteArray());
		} catch (IOException e) {
			Utils.failWithError(e);
		}
	}

}

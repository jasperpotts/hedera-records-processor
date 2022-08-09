package com.swirlds.streamloader.util;

import org.jetbrains.annotations.NotNull;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Simple output stream wrapper that counts bytes written
 */
public class ByteCounterOutputStream extends FilterOutputStream {
	private long count = 0;

	/**
	 * Creates an output stream filter built on top of the specified
	 * underlying output stream.
	 *
	 * @param out
	 * 		the underlying output stream to be assigned to
	 * 		the field {@code this.out} for later use, or
	 *                {@code null} if this instance is to be
	 * 		created without an underlying stream.
	 */
	public ByteCounterOutputStream(final OutputStream out) {
		super(out);
	}

	@Override
	public void write(final int b) throws IOException {
		super.write(b);
		count ++;
	}

	@Override
	public void write(@NotNull final byte[] b) throws IOException {
		super.write(b);
		count += b.length;
	}

	@Override
	public void write(@NotNull final byte[] b, final int off, final int len) throws IOException {
		super.write(b, off, len);
		count += len;
	}

	public long getCount() {
		return count;
	}
}

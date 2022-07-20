package com.swirlds.streamloader.util;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
	private final ByteBuffer buf;

	public ByteBufferInputStream(ByteBuffer buf) {
		this.buf = buf;
	}

	@Override
	public int read() throws IOException {
		if (!buf.hasRemaining()) {
			return -1;
		}
		return buf.get() & 0xFF;
	}

	@Override
	public int read(@NotNull final byte[] b, final int off, final int len) throws IOException {
		if (!buf.hasRemaining()) return -1;
		final var read = Math.min(len, buf.remaining());
		buf.get(b, off, len);
		return read;
	}
}
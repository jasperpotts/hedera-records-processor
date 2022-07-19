package com.swirlds.streamloader.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Simple Future that is completed from the start
 */
public class PreCompletedFuture<T> implements Future<T> {
	private final T value;

	public PreCompletedFuture(final T value) {
		this.value = value;
	}

	@Override
	public boolean cancel(final boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return value;
	}

	@Override
	public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return value;
	}
}

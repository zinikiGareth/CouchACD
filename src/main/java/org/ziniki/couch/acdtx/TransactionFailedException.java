package org.ziniki.couch.acdtx;

import java.util.List;

@SuppressWarnings("serial")
public class TransactionFailedException extends RuntimeException {
	private final List<Throwable> errors;

	public TransactionFailedException(List<Throwable> errors) {
		this.errors = errors;
	}
	
	public List<Throwable> getErrors() {
		return errors;
	}
}

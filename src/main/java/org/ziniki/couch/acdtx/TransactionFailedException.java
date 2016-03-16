package org.ziniki.couch.acdtx;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class TransactionFailedException extends RuntimeException {
	private final List<Throwable> errors;

	public TransactionFailedException(List<Throwable> errors) {
		this.errors = errors;
	}
	
	public TransactionFailedException(String string) {
		errors = new ArrayList<Throwable>();
		RuntimeException ex = new RuntimeException(string);
		errors.add(ex);
	}

	public List<Throwable> getErrors() {
		return errors;
	}
}

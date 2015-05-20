package org.ziniki.couch.acdtx;

@SuppressWarnings("serial")
public class InvalidTxStateException extends RuntimeException {

	public InvalidTxStateException(String msg) {
		super(msg);
	}

}

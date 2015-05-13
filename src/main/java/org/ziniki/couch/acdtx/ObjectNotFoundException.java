package org.ziniki.couch.acdtx;

@SuppressWarnings("serial")
public class ObjectNotFoundException extends RuntimeException {
	private final String id;

	public ObjectNotFoundException(String id) {
		this.id = id;
	}
}

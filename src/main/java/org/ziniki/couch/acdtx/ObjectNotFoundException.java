package org.ziniki.couch.acdtx;

@SuppressWarnings("serial")
public class ObjectNotFoundException extends RuntimeException {
	private final String id;

	public ObjectNotFoundException(String id) {
		super("object " + id + " not found");
		this.id = id;
	}
	
	public String id() {
		return id;
	}
}

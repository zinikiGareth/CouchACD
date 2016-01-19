package org.ziniki.couch.acdtx;

import com.couchbase.client.java.document.JsonDocument;

public class ReadDocument {
	String id;
	Throwable err;
	int hash;
	JsonDocument doc;
	boolean missing;
	
	public ReadDocument(String id) {
		this.id = id;
	}

	public void setDocument(JsonDocument t) {
		this.hash = t.content().hashCode();
		this.doc = t;
	}

	public void error(Throwable err) {
		this.err = err;
	}

	public boolean isEmpty() {
		return doc == null && err == null && !missing;
	}
}

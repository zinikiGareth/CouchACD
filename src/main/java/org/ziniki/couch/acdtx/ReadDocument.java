package org.ziniki.couch.acdtx;

import com.couchbase.client.java.document.JsonDocument;

public class ReadDocument {
	int hash;
	JsonDocument doc;

	public ReadDocument(int hashCode, JsonDocument t) {
		this.hash = hashCode;
		this.doc = t;
	}
}

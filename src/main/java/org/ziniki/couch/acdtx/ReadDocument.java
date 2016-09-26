package org.ziniki.couch.acdtx;

import java.util.HashSet;
import java.util.Set;

import com.couchbase.client.java.document.JsonDocument;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

public class ReadDocument {
	final String id;
	int hash;
	private boolean resolved;
	private Throwable err;
	private JsonDocument doc;
	private boolean missing;
	protected final Set<Subscriber<? super JsonDocument>> subscribers = new HashSet<>();
	
	public ReadDocument(String id) {
		this.id = id;
	}

	public void setDocument(JsonDocument t) {
		this.hash = t.content().hashCode();
		this.doc = t;
		resolve();
	}

	public void error(Throwable err) {
		this.err = err;
		resolve();
	}

	public void missing() {
		missing = true;
		resolve();
	}
	
	public boolean isReady() {
		return missing || doc != null;
	}

	private void resolve() {
		synchronized (subscribers) {
			this.resolved = true;
			for (Subscriber<? super JsonDocument> s : subscribers) {
				if (err != null)
					s.onError(err);
				else
					s.onNext(doc);
				s.onCompleted();
			}
		}
	}

	public Observable<JsonDocument> observer() {
		return Observable.create(new OnSubscribe<JsonDocument>() {
			@Override
			public void call(Subscriber<? super JsonDocument> s) {
				synchronized (subscribers) {
					if (resolved) {
						if (err != null)
							s.onError(err);
						else
							s.onNext(doc);
						s.onCompleted();
					} else
						subscribers.add(s);
				}
			}
		});
	}
	
	public Object getVersion(String versionField) {
		return doc.content().get(versionField);
	}
	
	public void assertSameDoc(JsonDocument other) {
		if (other != this.doc)
			throw new InvalidTxStateException("Internal error: object " + id + " requested dirty twice in the same TX with different physical documents");
	}
	
	@Override
	public String toString() {
		return "resolved = " + resolved + " missing = " + missing + " doc = " + doc + " error = " + err;
	}
}

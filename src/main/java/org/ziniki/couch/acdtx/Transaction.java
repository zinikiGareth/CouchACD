package org.ziniki.couch.acdtx;

import java.util.ArrayList;
import java.util.List;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Needs an id generator
 * Needs to store the tx object
 * Use getAndLock with 30s expiry when we start editing an object
 * Provide get/dirty methods as well as commit
 *
 * <p>
 * &copy; 2015 Ziniki Infrastructure Software, LLC.  All rights reserved.
 *
 * @author Gareth Powell
 *
 */
public class Transaction {
	private final AsyncBucket bucket;
	private final List<Throwable> errors = new ArrayList<Throwable>();
	private boolean rollbackOnly = false;

	public Transaction(Bucket bucket) {
		this.bucket = bucket.async();
	}
	
	public Transaction(AsyncBucket bucket) {
		this.bucket = bucket;
	}
	
	public Observable<JsonDocument> get(String id) {
		Observable<JsonDocument> ret = bucket.get(id).defaultIfEmpty(null);
		return ret.filter(new Func1<JsonDocument, Boolean>() {
			@Override
			public Boolean call(JsonDocument t) {
				if (t == null) {
					System.out.println("Saw error");
					errors.add(new ObjectNotFoundException(id));
					rollbackOnly = true;
					return false;
				} else
					return true;
			}
		});
	}
	
	public Observable<JsonDocument> newObject(String id, String type) {
		if (rollbackOnly) {
			// this isn't going anywhere anyway, just don't bother
			return Observable.from(new JsonDocument[] {});
		}
		JsonObject obj = JsonObject.create().put("type", type);
		JsonDocument doc = JsonDocument.create(id, obj);
		Observable<JsonDocument> ret = bucket.insert(doc);
		ret.subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
				System.out.println("new doc for " + id + " " + t.cas());
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				System.out.println("spotted error");
				rollbackOnly = true;
				errors.add(t);
			}
		});
		return ret;
	}
	
	public void commit() {
		if (rollbackOnly)
			throw new TransactionFailedException(errors);
		System.out.println("committed");
	}
}
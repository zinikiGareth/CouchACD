package org.ziniki.couch.acdtx;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Turn this into something that can actually handle transactions
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
	
	public void get(String id) {
		Observable<JsonDocument> ret = bucket.get(id);
		Observable<JsonDocument> mine = ret.defaultIfEmpty(null);
		ret.timeout(1000, TimeUnit.MILLISECONDS);
		mine.onErrorReturn((Func1<Throwable, ? extends JsonDocument>) new Func1<Throwable, JsonDocument>() {
			public JsonDocument call(Throwable t) {
				rollbackOnly = true;
				errors.add(t);
				return null;
			}
		}).subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument arg0) {
				System.out.println("hello " + id + ": " + arg0);
			}
		});
	}
	
	public void newObject(String id, String type) {
		if (rollbackOnly) {
			// this isn't going anywhere anyway, just don't bother
			return;
		}
		JsonObject obj = JsonObject.create().put("type", type);
		JsonDocument doc = JsonDocument.create(id, obj);
		Observable<JsonDocument> mine = bucket.insert(doc);
		mine.onErrorReturn((Func1<Throwable, ? extends JsonDocument>) new Func1<Throwable, JsonDocument>() {
			public JsonDocument call(Throwable t) {
				rollbackOnly = true;
				errors.add(t);
				return null;
			}
		}).subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
				System.out.println("new doc for " + id + " " + t.cas());
			}
		});
	}
	
	public void commit() {
		if (rollbackOnly)
			throw new TransactionFailedException(errors);
	}
}
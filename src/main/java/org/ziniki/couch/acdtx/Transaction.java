package org.ziniki.couch.acdtx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import rx.Notification;
import rx.Observable;
import rx.functions.Action0;
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
	private final AllDoneLatch latch = new AllDoneLatch();
	private final Map<String, ReadDocument> alreadyRead = new HashMap<String, ReadDocument>();
	private final Map<String, JsonDocument> dirtied = new HashMap<String, JsonDocument>();

	private boolean rollbackOnly = false;

	public Transaction(Bucket bucket) {
		this.bucket = bucket.async();
	}
	
	public Transaction(AsyncBucket bucket) {
		this.bucket = bucket;
	}
	
	public void hold() {
		latch.another();
	}
	
	public void release() {
		latch.done();
	}
	
	public Observable<JsonDocument> get(String id) {
		if (alreadyRead.containsKey(id)) {
			return Observable.just(alreadyRead.get(id).doc);
		}
		latch.another();
		Observable<JsonDocument> ret = bucket.get(id).defaultIfEmpty(null);
		return ret.filter(new Func1<JsonDocument, Boolean>() {
			@Override
			public Boolean call(JsonDocument t) {
				if (t == null) {
					errors.add(new ObjectNotFoundException(id));
					rollbackOnly = true;
					return false;
				} else {
					alreadyRead.put(id, new ReadDocument(t.content().hashCode(), t));
					return true;
				}
			}
		}).doOnCompleted(new Action0() {
			@Override
			public void call() {
				latch.done();
			}
		});

	}
	
	public void dirty(JsonDocument doc) {
		if (rollbackOnly) {
			// this isn't going anywhere anyway, just don't bother
			return;
		}
		if (!alreadyRead.containsKey(doc.id())) {
			rollbackOnly = true;
			return;
		}
		
		latch.another();
		bucket.getAndLock(doc.id(), 30).subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
				try {
				if (!assertUnchanged(t.id(), t.content())) {
					rollbackOnly = true;
					errors.add(new ResourceChangedException("Resource " + doc.id() + " changed while attempting to lock"));
					latch.done();
					return;
				}
				dirtied.put(doc.id(), JsonDocument.create(doc.id(), doc.content(), t.cas()));
				latch.done();
				} catch (Throwable t1) { t1.printStackTrace(); }
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				t.printStackTrace();
				rollbackOnly = true;
				errors.add(t);
				latch.done();
			}
		});
	}

	public Observable<JsonDocument> newObject(String id, String type) {
		if (rollbackOnly) {
			// this isn't going anywhere anyway, just don't bother
			return Observable.from(new JsonDocument[] {});
		}
		latch.another();
		
		// Write an empty object to the store to make sure that we grab it and obtain a CAS
		JsonObject obj = JsonObject.empty();
		JsonDocument doc = JsonDocument.create(id, obj);
		bucket.insert(doc)
			.subscribe(new Action1<JsonDocument>() {
				public void call(JsonDocument t) {
					latch.done();
					dirtied.put(id, JsonDocument.create(id, obj, t.cas()));
				}
			}, new Action1<Throwable>() {
				public void call(Throwable t) {
					latch.done();
					rollbackOnly = true;
					errors.add(t);
				}
			});
		
		// Return the user the document we originally created
		return Observable.just(doc);
	}
	
	public Observable<Integer> commit() {
		// TODO: should be in "prepare()" - but call that from here if it's not already been called
		if (rollbackOnly)
			throw new TransactionFailedException(errors);
		boolean allDone = latch.await(5, TimeUnit.SECONDS);
		if (!allDone) {
			errors.add(0, new TransactionTimeoutException());
			throw new TransactionFailedException(errors);
		}

		// OK, this is commit() proper
		List<Observable<?>> all = new ArrayList<Observable<?>>();
		for (JsonDocument d : dirtied.values()) {
			all.add(bucket.unlock(d));
			all.add(bucket.upsert(d));
		}
		return Observable.merge(all).count();
	}

	// Assert according to our rules that the document has not changed since
	// we previously read it
	// If we were given a key field that will always change (e.g. version or timestamp)
	// then we can just compare that; otherwise we compare the entire objects
	private boolean assertUnchanged(String id, JsonObject newContent) {
		ReadDocument mine = alreadyRead.get(id);

		// TODO: having a field be sufficient
		return mine != null && mine.hash == newContent.hashCode();
	}

}
package org.ziniki.couch.acdtx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * A Transaction is created by the TransactionFactory to represent one
 * unit of work.  In addition to commit and rollback, this object needs
 * to be inserted into the workflow as follows:
 * <ul>
 * <li>It needs to be responsible for creating objects using newObject()
 * <li>It needs to be responsible for obtaining objects to be used in the transaction, using get()
 * <li>It needs to be notified if an object has been changed in the transaction (at least once, but every time is also OK)
 * </ul>
 * The documents returned from this library must be the ones used and updated.
 * Do not copy or create documents outside of this library if you want them to be part of the transaction.
 * <p>
 * Use of this library does not stop you doing anything you want directly with the Couchbase API; but it cannot form
 * part of the transaction.
 * <p>
 * &copy; 2015 Ziniki Infrastructure Software, LLC.  All rights reserved.
 *
 * @author Gareth Powell
 *
 */
public class Transaction {
	private final TransactionFactory factory;
	private final String txid;
	private final AsyncBucket bucket;
	private final List<Throwable> errors = new ArrayList<Throwable>();
	private final AllDoneLatch latch = new AllDoneLatch();
	private final Map<String, ReadDocument> alreadyRead = new HashMap<String, ReadDocument>();
	private final Map<String, JsonDocument> dirtied = new HashMap<String, JsonDocument>();
	private final JsonDocument txRecord;

	public enum TxState { OPEN, ROLLBACKONLY, PREPARING, PREPARED, COMMITTING, COMMITTED };
	
	private TxState state = TxState.OPEN;

	Transaction(TransactionFactory factory, String txid, AsyncBucket bucket) {
		this.factory = factory;
		this.txid = txid;
		this.bucket = bucket;
		JsonObject txo = JsonObject.create().put("id", txid).put("dirty", JsonObject.create());
		txRecord = JsonDocument.create(txid, txo);
	}
	
	public String id() {
		return txid;
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
					state = TxState.ROLLBACKONLY;
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
		if (state == TxState.ROLLBACKONLY) {
			// this isn't going anywhere anyway, just don't bother
			return;
		}
		String id = doc.id();
		if (!alreadyRead.containsKey(id)) {
			errors.add(new InvalidTxStateException("cannot dirty an unread object"));
			state = TxState.ROLLBACKONLY;
			return;
		}

		if (dirtied.containsKey(id)) {
			// we already have it locked; don't need to do that again
			// we have to assume that the user always uses the same object; avoiding that would involve us in race issues
			return;
		}

		latch.another();
		bucket.getAndLock(id, 30).subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
				try {
					// CAS -1 means we could not obtain the lock
					if (t.cas() == -1 || !assertUnchanged(t.id(), t.content())) {
						state = TxState.ROLLBACKONLY;
						errors.add(new ResourceChangedException("Resource " + id + " changed while attempting to lock"));
						latch.done();
						return;
					}
					recordAs(id, doc.content(), t.cas());
					dirtied.put(id, JsonDocument.create(id, doc.content(), t.cas()));
				} catch (Throwable t1) {
					t1.printStackTrace();
					state = TxState.ROLLBACKONLY;
					errors.add(t1);
				}
				finally { latch.done(); }
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				t.printStackTrace();
				state = TxState.ROLLBACKONLY;
				errors.add(t);
				latch.done();
			}
		});
	}

	public Observable<JsonDocument> newObject(String id, String type) {
		if (state == TxState.ROLLBACKONLY) {
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
					recordAs(id, obj, t.cas());
				}
			}, new Action1<Throwable>() {
				public void call(Throwable t) {
					latch.done();
					state = TxState.ROLLBACKONLY;
					errors.add(t);
				}
			});
		
		// Return the user the document we originally created
		return Observable.just(doc);
	}

	protected JsonObject recordAs(String id, JsonObject obj, long cas) {
		JsonObject recordAs = JsonObject.create();
		recordAs.put("doc", obj);
		recordAs.put("cas", cas);
		((JsonObject) txRecord.content().get("dirty")).put(id, recordAs);
		return recordAs;
	}

	public Observable<Throwable> prepare() {
		System.out.println("In prepare(), state = " + state);
		if (state == TxState.ROLLBACKONLY) {
			unlockDirties();
			return Observable.just(new TransactionFailedException(errors));
		}
		if (state != TxState.OPEN)
			throw new InvalidTxStateException(state.toString());
		state = TxState.PREPARING;
		latch.another();
		txRecord.content().put("state", "prepared");
		bucket.insert(JsonDocument.create(factory.lockPrefix()+txid, 15, JsonObject.create()));
		bucket.insert(txRecord).subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
				System.out.println("inserted " + t);
				latch.done();
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				errors.add(0, t);
				state = TxState.ROLLBACKONLY;
				latch.done();
			}
		});
		return latch.onReleased(5, TimeUnit.SECONDS).map(new Func1<Boolean, Throwable>() {
			public Throwable call(Boolean t) {
				if (!t)
					errors.add(0, new TransactionTimeoutException());
				if (state == TxState.ROLLBACKONLY)
					return new TransactionFailedException(errors);
				state = TxState.PREPARED;
				return null;
			}
		});
	}
	
	private Observable<Throwable> doCommit() {
		System.out.println("In doCommit");
		state = TxState.COMMITTING;
		List<Observable<?>> all = new ArrayList<Observable<?>>();
		for (JsonDocument d : dirtied.values()) {
//			all.add(bucket.unlock(d));
			all.add(bucket.replace(d));
		}
		return Observable.merge(all).count().map(new Func1<Integer, Throwable>() {
			@Override
			public Throwable call(Integer t) {
				state = TxState.COMMITTED;
				txRecord.content().put("state", "committed");
				bucket.replace(txRecord);
				return null;
			}
		});
	}

	public Observable<Throwable> commit() {
		Func1<Throwable, Observable<Throwable>> checkThenCommit = new Func1<Throwable, Observable<Throwable>>() {
			@Override
			public Observable<Throwable> call(Throwable t) {
				System.out.println("in CTC, t = " + t);
				if (t != null)
					return Observable.just(t);
				return doCommit();
			}
		};
		if (state == TxState.OPEN || state == TxState.ROLLBACKONLY)
			return prepare().flatMap(checkThenCommit);
		else if (state == TxState.PREPARED)
			return doCommit();
		else
			throw new InvalidTxStateException(state.toString());
	}
	
	public void rollback() {
		if (state != TxState.OPEN && state != TxState.ROLLBACKONLY && state != TxState.PREPARED)
			throw new InvalidTxStateException(state.toString());
		state = TxState.ROLLBACKONLY;
		bucket.remove(txRecord);
		unlockDirties();
	}

	private void unlockDirties() {
		for (JsonDocument x : dirtied.values())
			bucket.unlock(x);
	}

	// Assert according to our rules that the document has not changed since
	// we previously read it
	// If we were given a key field that will always change (e.g. version or timestamp)
	// then we can just compare that; otherwise we compare the entire objects
	private boolean assertUnchanged(String id, JsonObject newContent) {
		ReadDocument mine = alreadyRead.get(id);

		String versionField = factory.versionField();
		if (versionField == null)
			return mine != null && mine.hash == newContent.hashCode();
		return mine.doc.content().getBoolean(versionField).equals(newContent.get(versionField));
	}

}
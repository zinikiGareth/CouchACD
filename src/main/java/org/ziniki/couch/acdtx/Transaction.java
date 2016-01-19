package org.ziniki.couch.acdtx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ziniki.couch.acdtx.AllDoneLatch.Latch;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;

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
	public static Logger logger = LoggerFactory.getLogger("CouchACDTx");
	private final TransactionFactory factory;
	private final String txid;
	private final AsyncBucket bucket;
	private final List<Throwable> errors = new ArrayList<Throwable>();
	private AllDoneLatch latch = new AllDoneLatch();
	private final Set<String> brandNew = new HashSet<String>();
	private final Set<String> requestedDirty = new HashSet<String>();
	private final Map<String, Observable<JsonDocument>> currentlyReading = new HashMap<String, Observable<JsonDocument>>();
	private final Map<String, ReadDocument> alreadyRead = new HashMap<String, ReadDocument>();
	private final Map<String, JsonDocument> dirtied = new HashMap<String, JsonDocument>();
	private final JsonDocument txRecord;

	public enum TxState { OPEN, ROLLBACKONLY, PREPARING, PREPARED, COMMITTING, COMMITTED };
	
	private TxState state = TxState.OPEN;
	private Boolean sync = new Boolean(true);
	private int request = 0;

	public Transaction(TransactionFactory factory, String txid, AsyncBucket bucket) {
//		logger.info("Creating ACDTx " + txid);
		this.factory = factory;
		this.txid = txid;
		this.bucket = bucket;
		JsonObject txo = JsonObject.create().put("id", txid).put("dirty", JsonObject.create());
		txRecord = JsonDocument.create(txid, txo);
	}
	
	public String id() {
		return txid;
	}

	public Latch hold() {
		int reqId;
		synchronized (sync) { reqId = request++; }
		return latch.another(txid + "_H_" + reqId);
	}
	
	public Observable<JsonDocument> getOrNull(String id) {
		ReadDocument holdingPen = new ReadDocument(id); 
		synchronized (alreadyRead) {
			if (id.endsWith("_contents_0")) {
				System.out.println("Have: " + alreadyRead.keySet());
				System.out.println("Looking for " + id + ": " + alreadyRead.containsKey(id));
			}
			if (alreadyRead.containsKey(id)) {
				ReadDocument ct = alreadyRead.get(id);
				JsonDocument send = ct.doc;
				System.out.println("Sending along " + send);
				if (ct.missing || send != null)
					return Observable.just(send);
				else {
					return currentlyReading.get(id);
				}
			}
			alreadyRead.put(id, holdingPen);
		}

		int reqId;
		synchronized (sync) { reqId = request++; }
		Latch l = latch.another(txid + "_G_" + reqId);
		bucket.get(id).defaultIfEmpty(null).subscribe(doc -> {
			synchronized (holdingPen) {
				if (doc == null)
					holdingPen.missing = true;
				else
					holdingPen.setDocument(doc);
				holdingPen.notifyAll();
				l.release();
			}
		}, err -> {
			synchronized (holdingPen) {
				holdingPen.error(err);
				holdingPen.notifyAll();
				l.release();
			}
		});

		OnSubscribe<JsonDocument> readit = new OnSubscribe<JsonDocument>() {
			public void call(Subscriber<? super JsonDocument> s) {
				synchronized (holdingPen) {
					while (holdingPen.isEmpty() && !s.isUnsubscribed()) {
						SyncUtils.waitFor(holdingPen, 150);
					}
					if (s.isUnsubscribed())
						return;
					if (holdingPen.err != null)
						s.onError(holdingPen.err);
					else {
						s.onNext(holdingPen.doc);
						s.onCompleted();
					}
				}
			}
		};
		List<Latch> subscriptions = new ArrayList<Latch>();
		Observable<JsonDocument> ret = Observable.create(readit).
			doOnSubscribe(() -> { synchronized(subscriptions) { Latch q = latch.another(l.toString() + "_" + subscriptions.size()); subscriptions.add(q); /*System.err.println("S " + q);*/ }}).
			doOnUnsubscribe(() -> { synchronized(subscriptions) {
				for (int i=0;i<subscriptions.size();i++) {
					Latch m = subscriptions.get(i);
					if (m != null) {
//						System.err.println("U " + m);
						m.release();
						subscriptions.set(i, null);
						synchronized (holdingPen) { holdingPen.notifyAll(); }
						return;
					}
				}
				System.out.println("Not enough subscriptions to close");
				System.exit(51);
			}
			});
		currentlyReading.put(id, ret);
		return ret;
	}
	
	public Observable<JsonDocument> get(String id) {
		return getOrNull(id).filter(new Func1<JsonDocument, Boolean>() {
			@Override
			public Boolean call(JsonDocument t) {
				if (t == null) {
					System.out.println("Failed to find " + id);
					ObjectNotFoundException ret = new ObjectNotFoundException(id);
					error(ret);
					throw ret;
//					return false;
				} else {
					return true;
				}
			}
		});
	}
	
	public synchronized void dirty(JsonDocument doc) {
		if (state == TxState.ROLLBACKONLY) {
			// this isn't going anywhere anyway, just don't bother
			return;
		}
		String id = doc.id();
		if (!alreadyRead.containsKey(id)) {
			error(new InvalidTxStateException("cannot dirty an unread object: " + id));
			return;
		}
		
		if (requestedDirty.contains(id)) {
			// we already have it locked; don't need to do that again
			// we have to assume that the user always uses the same object; avoiding that would involve us in race issues
			return;
		}
		requestedDirty.add(id);

		int reqId;
		synchronized (sync) { reqId = request++; }
		Latch l = latch.another(txid + "_D_" + reqId);
		bucket.getAndLock(id, 30).subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
				synchronized(Transaction.this) {
//					logger.info("Make object dirty: " + id);
					if (t == null)
						throw new RuntimeException("Found null in dirty(" + id +")");
					try {
						// CAS -1 means we could not obtain the lock
						if (t.cas() == -1 || !assertUnchanged(t.id(), t.content())) {
							error(new ResourceChangedException("Resource " + id + " changed while attempting to lock"));
							return;
						}
						recordAs(id, doc.content(), t.cas());
						dirtied.put(id, JsonDocument.create(id, doc.content(), t.cas()));
					} catch (Throwable t1) {
						t1.printStackTrace();
						error(t1);
					}
					finally { l.release(); }
				}
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				logger.info("Encountered error trying to dirty " + id, t);
				t.printStackTrace();
				error(t);
				l.release();
			}
		});
	}

	public JsonDocument newObject(String id) {
		if (state == TxState.ROLLBACKONLY) {
			// this isn't going anywhere anyway, just don't bother
			return JsonDocument.create(id, JsonObject.create());
		}

		logger.info("Creating new object " + id);
		
		
		// Write an empty object to the store to make sure that we grab it and obtain a CAS
		JsonDocument doc = push(id, JsonObject.empty());

		// It is automatically dirtied in this tx
		brandNew.add(id);
		requestedDirty.add(id);

		// It has already been "read"
		ReadDocument ret = new ReadDocument(id);
		ret.setDocument(doc);
		alreadyRead.put(id, ret);
		System.out.println("doc = " + ret.doc);
		
		return doc;
	}

	public JsonDocument push(String id, JsonObject obj) {
		JsonDocument doc = JsonDocument.create(id, obj);
		if (state == TxState.ROLLBACKONLY) {
			// this isn't going anywhere anyway, just don't bother
			return doc;
		}
		int reqId;
		synchronized (sync) { reqId = request++; }
		Latch l = latch.another(txid + "_P_" + reqId);
//		logger.info("Calling insert for id " + id);
		bucket.insert(doc).subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
//				logger.info("Putting object " + id + " in dirty state");
				dirtied.put(id, JsonDocument.create(id, obj, t.cas()));
				try {
					recordAs(id, obj, t.cas());
					l.release();
				} catch (RuntimeException ex) {
					logger.info("Exception recording " + txid + "_P_" + reqId);
					throw ex;
				}
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				logger.info("Insert for id " + id + " failed", t);
				l.release();
				error(t);
			}
		});
	
		// Return the user the original document
		return doc;
	}

	protected JsonObject recordAs(String id, JsonObject obj, long cas) {
		if (state == TxState.PREPARING)
			throw new RuntimeException("Invalid call to recordAs " + id);
		JsonObject recordAs = JsonObject.create();
		recordAs.put("doc", obj);
		recordAs.put("cas", cas);
		((JsonObject) txRecord.content().get("dirty")).put(id, recordAs);
		return recordAs;
	}

	public void await() {
		if (!latch.onReleased(15, TimeUnit.SECONDS).toBlocking().first())
			throw new TransactionTimeoutException();
	}

	public void relatch() {
		latch = new AllDoneLatch();
	}

	public Observable<Throwable> prepare() {
//		logger.info("In prepare(" + txid +"), state = " + state);
		if (state == TxState.ROLLBACKONLY) {
			for (Throwable t : errors)
				logger.error("Aborting because of", t);
			return latch.onReleased(5, TimeUnit.SECONDS).map(new Func1<Boolean, Throwable>() {
				@Override
				public Throwable call(Boolean b) {
					unlockDirties();
					return new TransactionFailedException(errors);
				}
			});
		}
		if (state != TxState.OPEN)
			return latch.onReleased(5, TimeUnit.SECONDS).map(new Func1<Boolean, Throwable>() {
				@Override
				public Throwable call(Boolean t) {
					unlockDirties();
					return new InvalidTxStateException(state.toString());
				}
			});
		Boolean worked = latch.onReleased(5, TimeUnit.SECONDS).toBlocking().first();
		if (!worked) {
			unlockDirties();
			return Observable.just(new TransactionTimeoutException());
		}
		AllDoneLatch platch = new AllDoneLatch();
		state = TxState.PREPARING;
//		logger.info("In prepare(" + txid + "), latch released, state = " + state);
		int reqId;
		synchronized (sync) { reqId = request++; }
		Latch l = platch.another(txid + "_X_" + reqId);
		txRecord.content().put("state", "prepared");
		bucket.insert(JsonDocument.create(factory.lockPrefix()+txid, 15, JsonObject.create()));
		bucket.insert(txRecord).subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
//				logger.info("inserted tx: " + t);
				l.release();
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				logger.info("error creating " + txid);
				t.printStackTrace();
				error(t);
				l.release();
			}
		});
//		logger.info("In prepare(" + txid + "), wrote txRecord, state = " + state);
		return platch.onReleased(5, TimeUnit.SECONDS).map(new Func1<Boolean, Throwable>() {
			public Throwable call(Boolean t) {
				if (!t)
					error(new TransactionTimeoutException());
				if (state == TxState.ROLLBACKONLY)
					return new TransactionFailedException(errors);
				state = TxState.PREPARED;
				return null;
			}
		});
	}
	
	private Observable<Throwable> doCommit() {
//		logger.info("In doCommit(" + txid +")");
		state = TxState.COMMITTING;
		List<Observable<?>> all = new ArrayList<Observable<?>>();
		for (JsonDocument d : dirtied.values()) {
			all.add(bucket.replace(d));
//			logger.info("Replaced " + d.id() + " with " + d);
		}
//		logger.info("In doCommit(" + txid +"), initiated " + dirtied.size() + " writes");
		return Observable.merge(all).count().map(new Func1<Integer, Throwable>() {
			@Override
			public Throwable call(Integer t) {
//				logger.info("Replaced " + t + " objects");
				state = TxState.COMMITTED;
				txRecord.content().put("state", "committed");
				bucket.replace(txRecord);
				// TODO: remove the lock record
				return null;
			}
		});
	}

	public Observable<Throwable> commit() {
		Func1<Throwable, Observable<Throwable>> checkThenCommit = new Func1<Throwable, Observable<Throwable>>() {
			@Override
			public Observable<Throwable> call(Throwable t) {
				logger.info("in CTC, t = " + t);
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
		try {
			bucket.remove(txRecord.id()).isEmpty().toBlocking().first();
		} catch (DocumentDoesNotExistException ex) {
			// That's OK ... didn't get to prepare
		}
		unlockDirties();
	}

	private void unlockDirties() {
		if (dirtied.isEmpty() && brandNew.isEmpty())
			return;
		logger.info("Unlocking dirties");
		List<Observable<Boolean>> os = new ArrayList<Observable<Boolean>>();
		for (JsonDocument x : dirtied.values()) {
			logger.info("Unlocking " + x.id());
			os.add(bucket.unlock(x).onErrorReturn(err -> true));
		}
		for (String x : brandNew) {
			logger.info("Deleting " + x);
			os.add(bucket.remove(x).map(r -> true).onErrorReturn(err -> true));
		}
		Observable.merge(os).toBlocking().last();
	}

	// Assert according to our rules that the document has not changed since
	// we previously read it
	// If we were given a key field that will always change (e.g. version or timestamp)
	// then we can just compare that; otherwise we compare the entire objects
	private boolean assertUnchanged(String id, JsonObject newContent) {
		ReadDocument mine = alreadyRead.get(id);

		String versionField = factory.versionField();
		Object oldV = mine.doc.content().get(versionField);
		System.err.println("id = " + id + " vf = " + versionField + " oldV = " + oldV + " mine = " + mine + " mh = " + (mine == null?"N/A":mine.hash) + " nc = " + newContent.hashCode() + " mine = " + (mine == null ? "N/A" : mine.doc.content()) + " new = " + newContent);
		if (versionField == null || oldV == null)
			return mine != null && mine.hash == newContent.hashCode();
		Object newV = newContent.get(versionField);
		return oldV.equals(newV);
	}

	public void error(Throwable t) {
		state = TxState.ROLLBACKONLY;
		errors.add(t);
		logger.info("Marked tx " + txid + " rollback only with error " + t);
	}
}
package org.ziniki.couch.acdtx;

import java.util.HashSet;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

public class TransactionCleaner extends Thread {
	// The intent is that this should probably run in a thread on every server that is using this library
	// Multiple should be able to run in parallel, since they need to obtain a lock on the document
	
	/* We need a view something like the following:

function (doc, meta) {
  if (meta.type != 'json' || !meta.id)
    return;
  if (meta.id.substring(0,2) == 'tx' && doc.state)
    emit(meta.id, doc.state);
  else if (meta.id.substring(0,7) == 'lock_tx')
    emit(meta.id, "locked");
}

	 * Obviously, it needs to be "adjusted" to take into account the user's naming preferences for transaction objects
	 */
	
	public static void main(String[] argv) {
		new TransactionCleaner("default", "dev_fred", "bert").start();
	}

	private boolean done = false;
	private final CouchbaseCluster cluster;
	private final Bucket bucket;
	private final ViewQuery query;
	
	TransactionCleaner(String bucket, String dName, String vName) {
		this.cluster = CouchbaseCluster.create();
		this.bucket = cluster.openBucket(bucket);
		query = ViewQuery.from(dName, vName);
	}
	
	@Override
	public void run() {
		while (!done) {
			ViewResult vr = this.bucket.query(query);
			System.out.println(vr.totalRows());
			Set<String> toDealWith = new HashSet<String>();
			Set<String> stillLocked = new HashSet<String>();
			for (ViewRow r : vr.allRows()) {
				System.out.println(r.id());
				if (r.id().startsWith("lock_")) {
					if (bucket.exists(r.id())) {
						stillLocked.add(r.id());
						toDealWith.remove(r.id());
					}
				} else if (r.id().startsWith("tx")) {
					if (!stillLocked.contains(r.id()))
						toDealWith.add(r.id());
				}
			}
			toDealWith.removeAll(stillLocked);
			System.out.println("Need to roll back " + toDealWith);
			for (String id : toDealWith) {
				JsonDocument txr = bucket.getAndLock(id, 30);
				if (txr == null || txr.cas() == -1)
					continue;
				JsonObject content = txr.content();
				System.out.println(content);
				JsonObject dirty = content.getObject("dirty");
				for (String sid : dirty.getNames()) {
					Long cas = dirty.getObject(sid).getLong("cas");
					bucket.unlock(sid, cas);
				}
				bucket.remove(txr);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
				// don't really care
			}
		}
	}
		
	public void dispose() {
		try {
			done = true;
			this.join();
		} catch (InterruptedException ex) {
		}
		bucket.close();
		cluster.disconnect();
	}
}

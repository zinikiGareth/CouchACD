package test.ziniki;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ziniki.couch.acdtx.InvalidTxStateException;
import org.ziniki.couch.acdtx.Transaction;
import org.ziniki.couch.acdtx.TransactionFactory;
import org.ziniki.couch.acdtx.TransactionFailedException;

import rx.Observable;
import rx.functions.Action1;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class ReallyBasicTests {
	private static Bucket bucket;
	private static Cluster cluster;
	private static TransactionFactory factory;
	private static int id = 0;

	@BeforeClass
	public static void start() throws Exception {
		cluster = CouchbaseCluster.create();
		bucket = cluster.openBucket("default");
		bucket.bucketManager().flush();
		factory = new TransactionFactory(bucket.async()) {
			@Override
			public String nextId() {
				return "tx"+(++id);
			}
		};
		JsonObject obj = JsonObject.create().put("type", "a").put("date", new Date().toString());
		JsonDocument doc = JsonDocument.create("fred", obj);
		bucket.upsert(doc);
	}
	
	@AfterClass
	public static void cleanup() throws Exception {
		factory.dispose();
		bucket.close();
		cluster.disconnect();
	}

	private void checkForObservedException(Observable<Throwable> opRet) throws Exception {
		Throwable t = opRet.toBlocking().single();
		if (t != null) {
			if (t instanceof Exception)
				throw (Exception)t;
			else
				throw new InvocationTargetException(t);
		}
	}

	@Test
	public void testWeCanCreateAnObject() throws Exception {
		Transaction tx = factory.open();
		tx.newObject("joe", "user");
		checkForObservedException(tx.commit());
		assertNotNull(bucket.get(tx.id()));
		assertNotNull(bucket.get("joe"));
	}

	@Test(expected=TransactionFailedException.class)
	public void testWeCannotCreateAnObjectWhichAlreadyExists() throws Exception {
		Transaction tx = factory.open();
		tx.newObject("fred", "user");
		checkForObservedException(tx.commit());
		assertNull(bucket.get("tx"));
	}

	@Test
	public void testWeCanReadAnObjectWhichExists() throws Exception {
		Transaction tx = factory.open();
		tx.get("fred").subscribe();
		checkForObservedException(tx.commit());
		assertNotNull(bucket.get("tx1"));
	}

	@Test(expected=TransactionFailedException.class)
	public void testWeRollbackWhenAnObjectDoesNotExist() throws Exception {
		Transaction tx = factory.open();
		tx.get("bert").subscribe();
		checkForObservedException(tx.commit());
		assertNull(bucket.get("tx"));
	}

	@Test
	public void testWeCanChangeAnObjectIfTheresNoRollback() throws Exception {
		Transaction tx = factory.open();
		
		Observable<JsonDocument> foo = tx.get("fred");
		foo.subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument fred) {
				fred.content().put("version", 2);
				tx.dirty(fred);
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				t.printStackTrace();
			}
		});
		checkForObservedException(tx.commit());
		assertNotNull(bucket.get(tx.id()));
		JsonDocument readFred = bucket.get("fred");
		assertNotNull(readFred);
		Integer v = readFred.content().getInt("version");
		assertNotNull(v);
		assertEquals(2, (int)v);
	}
	
	@Test
	public void testWeCanRollbackATxByHandAsItWere() throws Exception {
		Transaction tx = factory.open();
		
		Observable<JsonDocument> foo = tx.get("fred");
		foo.subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument fred) {
				fred.content().put("version", 2);
				tx.dirty(fred);
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				t.printStackTrace();
			}
		});
		tx.rollback();
		assertNull(bucket.get("tx4"));
		JsonDocument readFred = bucket.get("fred");
		assertNotNull(readFred);
		assertNull(readFred.content().getInt("version"));
	}
	
	@Test
	public void testWeCanStillRollbackATxByHandAfterWeCallPrepare() throws Exception {
		Transaction tx = factory.open();
		
		Observable<JsonDocument> foo = tx.get("fred");
		foo.subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument fred) {
				fred.content().put("version", 2);
				tx.dirty(fred);
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				t.printStackTrace();
			}
		});
		checkForObservedException(tx.prepare());
		tx.rollback();
		assertNull(bucket.get("tx4"));
		JsonDocument readFred = bucket.get("fred");
		assertNotNull(readFred);
		assertNull(readFred.content().getInt("version"));
	}
	
	@Test(expected=InvalidTxStateException.class)
	public void testWeCannotStillRollbackATxByHandAfterCommit() throws Exception {
		Transaction tx = factory.open();
		
		Observable<JsonDocument> foo = tx.get("fred");
		foo.subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument fred) {
				fred.content().put("version", 2);
				tx.dirty(fred);
			}
		}, new Action1<Throwable>() {
			public void call(Throwable t) {
				t.printStackTrace();
			}
		});
		checkForObservedException(tx.commit());
		tx.rollback();
	}
}

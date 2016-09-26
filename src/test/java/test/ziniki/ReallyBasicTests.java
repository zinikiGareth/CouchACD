package test.ziniki;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ziniki.couch.acdtx.BaseTransactionFactory;
import org.ziniki.couch.acdtx.InvalidTxStateException;
import org.ziniki.couch.acdtx.ObjectNotFoundException;
import org.ziniki.couch.acdtx.Transaction;
import org.ziniki.couch.acdtx.TransactionFailedException;
import org.ziniki.couch.acdtx.TransactionTimeoutException;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import rx.Observable;
import rx.functions.Func1;

public class ReallyBasicTests {
	public static Logger logger = LoggerFactory.getLogger("Tests");
	private static Bucket bucket;
	private static Cluster cluster;
	private static BaseTransactionFactory factory;
	private static int id = 0;

	@BeforeClass
	public static void start() throws Exception {
		System.out.println("Flushing bucket");
		cluster = CouchbaseCluster.create();
		bucket = cluster.openBucket("default");
		bucket.bucketManager().flush();
		System.out.println("Bucket flushed");
		factory = new BaseTransactionFactory(bucket.async()) {
			@Override
			public String nextId() {
				return "tx"+(++id);
			}
		};
		JsonObject obj = JsonObject.create().put("type", "a").put("date", new Date().toString());
		JsonDocument doc = JsonDocument.create("fred", obj);
		bucket.upsert(doc);
		System.out.println("Inserted fred");
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
		tx.newObject("joe");
		checkForObservedException(tx.commit());
		assertNotNull(bucket.get(tx.id()));
		assertNotNull(bucket.get("joe"));
	}

	@Test
	public void testWeCanCreateAnObjectThenDirtyItInTheSameTx() throws Exception {
		Transaction tx = factory.open();
		JsonDocument doc = tx.newObject("henry");
		tx.dirty(doc);
		checkForObservedException(tx.commit());
		assertNotNull(bucket.get(tx.id()));
		assertNotNull(bucket.get("henry"));
	}

	@Test(expected=TransactionFailedException.class)
	public void testWeCannotCreateAnObjectWhichAlreadyExists() throws Exception {
		Transaction tx = factory.open();
		tx.newObject("fred");
		checkForObservedException(tx.commit());
		assertNull(bucket.get("tx"));
	}

	@Test
	public void testWeCanReadAnObjectWhichExists() throws Exception {
		Transaction tx = factory.open();
		tx.get("fred").toBlocking().single();
		checkForObservedException(tx.commit());
		assertNotNull(bucket.get(tx.id()));
	}

	@Test(expected=ObjectNotFoundException.class)
	public void testWeRollbackWhenAnObjectDoesNotExist() throws Exception {
		Transaction tx = factory.open();
		tx.get("bert").toBlocking().single();
		checkForObservedException(tx.commit());
	}

	@Test
	public void testWeCanChangeAnObjectIfTheresNoRollback() throws Exception {
		Transaction tx = factory.open();
		
		tx.get("fred").map(new Func1<JsonDocument, Void>() {
			public Void call(JsonDocument fred) {
				fred.content().put("version", 2);
				tx.dirty(fred);
				return null;
			}
		}).toBlocking().single();
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
		logger.info("Creating tx " + tx.id());
		
		tx.get("fred").map(new Func1<JsonDocument, Void>() {
			public Void call(JsonDocument fred) {
				System.out.println("obtained fred " + fred.id());
				fred.content().put("version", 2);
				tx.dirty(fred);
				return null;
			}
		}).toBlocking().single();
		logger.info("Rolling back tx " + tx.id());
		tx.rollback();
		logger.info("Rolled back tx " + tx.id());
		assertNull(bucket.get(tx.id()));
		logger.info("Checked for null " + tx.id());
		JsonDocument readFred = bucket.get("fred");
		assertNotNull(readFred);
		assertNull(readFred.content().getInt("version"));
	}
	
	@Test
	public void testWeCanStillRollbackATxByHandAfterWeCallPrepare() throws Exception {
		Transaction tx = factory.open();
		String id = tx.id();
		System.out.println("Creating tx " + id);
		tx.get("fred").map(new Func1<JsonDocument, Void>() {
			public Void call(JsonDocument fred) {
				fred.content().put("version", 2);
				tx.dirty(fred);
				return null;
			}
		}).toBlocking().single();
		checkForObservedException(tx.prepare());
		tx.rollback();
		assertNull(bucket.get(id));
		JsonDocument readFred = bucket.get("fred");
		assertNotNull(readFred);
		assertNull(readFred.content().getInt("version"));
	}
	
	@Test(expected=InvalidTxStateException.class)
	public void testWeCannotStillRollbackATxByHandAfterCommit() throws Exception {
		Transaction tx = factory.open();
		
		tx.get("fred").map(new Func1<JsonDocument, Void>() {
			public Void call(JsonDocument fred) {
				fred.content().put("version", 2);
				tx.dirty(fred);
				return null;
			}
		}).toBlocking().single();
		checkForObservedException(tx.commit());
		tx.rollback();
	}
	
	@Test(expected=TransactionTimeoutException.class)
	public void testPrepareWillFailIfWeNeverUnlatch() throws Throwable {
		Transaction tx = factory.open();
		tx.hold();
		Throwable t = tx.commit().toBlocking().first();
		if (t != null)
			throw t;
	}
}

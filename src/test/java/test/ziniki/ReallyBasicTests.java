package test.ziniki;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ziniki.couch.acdtx.Transaction;
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

	@BeforeClass
	public static void start() throws Exception {
		cluster = CouchbaseCluster.create();
		bucket = cluster.openBucket("default");
		bucket.bucketManager().flush();
		JsonObject obj = JsonObject.create().put("type", "a").put("date", new Date().toString());
		JsonDocument doc = JsonDocument.create("fred", obj);
		bucket.upsert(doc);
	}
	
	@AfterClass
	public static void cleanup() throws Exception {
		bucket.close();
		cluster.disconnect();
	}

	private void throwEx(Observable<Throwable> opRet) throws Exception {
		Throwable t = opRet.toBlocking().single();
		if (t != null) {
			if (t instanceof Exception)
				throw (Exception)t;
			else
				throw new InvocationTargetException(t);
		}
	}

	@Test(expected=TransactionFailedException.class)
	public void testWeCannotCreateAnObjectWhichAlreadyExists() throws Exception {
		Transaction tx = new Transaction(bucket);
		tx.newObject("fred", "user");
		throwEx(tx.commit());
	}

	@Test
	public void testWeCanReadAnObjectWhichExists() throws Exception {
		Transaction tx = new Transaction(bucket);
		tx.get("fred").subscribe();
		throwEx(tx.commit());
	}

	@Test(expected=TransactionFailedException.class)
	public void testWeRollbackWhenAnObjectDoesNotExist() throws Exception {
		Transaction tx = new Transaction(bucket);
		tx.get("bert").subscribe();
		throwEx(tx.commit());
	}

	@Test
	public void testWeCanChangeAnObjectIfTheresNoRollback() throws Exception {
		Transaction tx = new Transaction(bucket);
		
		Observable<JsonDocument> foo = tx.get("fred");
		foo.subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument fred) {
				fred.content().put("version", 2);
				tx.dirty(fred);
			}
		});
		throwEx(tx.commit());
		JsonDocument readFred = bucket.get("fred");
		Integer v = readFred.content().getInt("version");
		assertNotNull(v);
		assertEquals(2, (int)v);
	}
}

package test.ziniki;

import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ziniki.couch.acdtx.Transaction;
import org.ziniki.couch.acdtx.TransactionFailedException;

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
		System.out.println("Closing");
		bucket.close();
		System.out.println("Disconnecting");
		cluster.disconnect();
		System.out.println("Done");
	}

	@Test(expected=TransactionFailedException.class)
	public void testWeCannotCreateAnObjectWhichAlreadyExists() throws Exception {
		Transaction tx = new Transaction(bucket);
		tx.newObject("fred", "user");
		tx.commit();
	}

	@Test
	public void testWeCanReadAnObjectWhichExists() throws Exception {
		Transaction tx = new Transaction(bucket);
		tx.get("fred").subscribe(new Action1<JsonDocument>() {
			public void call(JsonDocument t) {
				System.out.println("succesfully read object");
			}
		});
		tx.commit();
	}

	@Test(expected=TransactionFailedException.class)
	public void testWeRollbackWhenAnObjectDoesNotExist() throws Exception {
		Transaction tx = new Transaction(bucket);
		tx.get("bert").subscribe();
		tx.commit();
	}
}

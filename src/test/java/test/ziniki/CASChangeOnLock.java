package test.ziniki;

import static org.junit.Assert.assertNotNull;

import java.util.Date;

import org.junit.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryLockFailureException;

public class CASChangeOnLock {

	@Test
	public void testCASChangesOnLock() {
		CouchbaseCluster cluster = CouchbaseCluster.create();
		Bucket bucket = cluster.openBucket("default");
		bucket.bucketManager().flush();
		JsonObject obj = JsonObject.create().put("type", "a").put("date", new Date().toString());
		JsonDocument doc = JsonDocument.create("fred", obj);
		JsonDocument written = bucket.upsert(doc);
		System.out.println(written.cas());
		JsonDocument ldoc = bucket.getAndLock("fred", 30);
		System.out.println(ldoc.cas());
		ldoc.content().put("hello", "world");
		Exception tmpEx = null;
		try {
			JsonDocument ndoc = bucket.getAndLock("fred", 30);
			System.out.println(ndoc.cas());
		} catch (TemporaryLockFailureException ex) {
			tmpEx = ex;
		}
		assertNotNull(tmpEx);
		bucket.replace(ldoc);
		{
			JsonDocument ndoc = bucket.getAndLock("fred", 30);
			System.out.println(ndoc.cas());
		}
		bucket.close();
		cluster.disconnect();
	}

}

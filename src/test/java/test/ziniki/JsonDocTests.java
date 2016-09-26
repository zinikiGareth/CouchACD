package test.ziniki;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import com.couchbase.client.java.document.json.JsonObject;

public class JsonDocTests {

	@Test
	public void testTwoEmptyJsonDocumentsAreTheSame() {
		JsonObject o1 = JsonObject.empty();
		JsonObject o2 = JsonObject.empty();
		assertEquals(o1, o2);
	}
	
	@Test
	public void testTwoSimpleJsonDocumentsAreTheSame() {
		JsonObject o1 = JsonObject.empty().put("version", 1);
		JsonObject o2 = JsonObject.empty().put("version", 1);
		assertEquals(o1, o2);
	}
	
	@Test
	public void testTwoSimpleJsonDocumentsAreDifferentByVersionNumber() {
		JsonObject o1 = JsonObject.empty().put("version", 1);
		JsonObject o2 = JsonObject.empty().put("version", 2);
		assertNotEquals(o1, o2);
	}

}

package org.ziniki.couch.acdtx;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;

/** The TransactionFactory is the main entry point into the transaction library.
 * Users must create a new transaction factory and give it a bucket to work with.
 * <p>
 * In order to instantiate this class, the nextId method must be overridden
 * and implemented with a scheme which for the user's data set generates unique
 * transaction IDs.
 *
 * <p>
 * &copy; 2015 Ziniki Infrastructure Software, LLC.  All rights reserved.
 *
 * @author Gareth Powell
 *
 */
public abstract class TransactionFactory {
	private final AsyncBucket bucket;

	public TransactionFactory(AsyncBucket bucket) {
		this.bucket = bucket;
	}
	
	public TransactionFactory(Bucket bucket) {
		this(bucket.async());
	}
	
	public Transaction open() {
		return new Transaction(this, nextId(),  bucket);
	}

	/** Return a new, unique ID for the transaction.
	 * 
	 * @return a string which is a unique transaction ID
	 */
	public abstract String nextId();

	/** A prefix which can be prepended to any txid to make
	 * another, distinct, unique, valid entry for creating a lock/rollback record for the transaction
	 * @return
	 */
	public String lockPrefix() {
		return "lock_";
	}
	
	/** If the objects have a unique version field, then this can
	 * be used as a "quick check" of whether an object has changed
	 * between calls to "get()" and "dirty()".  It is not possible
	 * to do this directly with Couchbase (as the CAS always changes on
	 * getAndLock()).
	 * <p>
	 * If this method returns null, the entire object will be checked, field
	 * by field.
	 * @return a quick check, present in every object, different on every write, "version" field in the objects
	 * being stored
	 */
	public String versionField() {
		return null;
	}

	/** Clean up any resources
	 * No resources are used internally, but implementations may
	 * have obligations to clean up with regard to ID generation
	 */
	public void dispose() {
	}
	
}

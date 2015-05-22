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

	/** Clean up any resources
	 * No resources are used internally, but implementations may
	 * have obligations to clean up with regard to ID generation
	 */
	public void dispose() {
	}
	
}

package org.ziniki.couch.acdtx;

public interface TransactionFactory {

	Transaction open();

	/** Return a new, unique ID for the transaction.
	 * 
	 * @return a string which is a unique transaction ID
	 */
	String nextId();

	/** A prefix which can be prepended to any txid to make
	 * another, distinct, unique, valid entry for creating a lock/rollback record for the transaction
	 * @return
	 */
	String lockPrefix();

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
String versionField();
}
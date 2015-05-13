# ACD Transaction Library for Couchbase

The objective of this library is to provide a sub-transactional library providing
atomicity, consistency and durability for transactions in Couchbase.

Full isolation is too hard to achieve, but some level of isolation is also provided, at least
across transactions that succeed.  In particular, there is a window during the processing of
commit where newly written values are neither complete nor will reading them cause a rollback
of a subsequent transaction.

This library uses optimistic reads and the "getAndLock" functionality to protect writes.

In order to provide any kind of guarantees at all, all clients must (preferably) use this
library or at least respect CAS writes.
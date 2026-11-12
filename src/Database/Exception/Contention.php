<?php

namespace Utopia\Database\Exception;

/**
 * Thrown when the engine aborts a statement over a lock conflict with a concurrent transaction: a deadlock, a lock
 * wait timeout, a serialization failure, a lock that is not available or a busy database. The engine may have rolled
 * the whole transaction back with it, so nothing the transaction wrote is stored and running it again can succeed.
 *
 * The library throws it too when a concurrent write changes what a transaction read before the transaction writes
 * it, as when deleteDocuments() deletes fewer documents of a batch than still matched its filters; the transaction
 * is rolled back, and running it again can succeed.
 */
class Contention extends Transaction
{
}

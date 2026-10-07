<?php

namespace Utopia\Database;

/**
 * Defines the set of optional behavioral capabilities that a database adapter may support.
 *
 * Feature availability (method contracts) is expressed via Feature interfaces on the adapter class and checked
 * with Adapter::hasFeature(), not capabilities and not instanceof, which is false on a proxy such as Pool.
 */
enum Capability
{
    case Aggregations;
    case AlterLock;
    case AttributeResizing;
    case Caching;
    case DefinedAttributes;
    case Hostname;
    case IndexArray;
    case IndexArrayCast;
    case IndexFulltext;
    case IndexFulltextMultiple;
    case IndexFulltextWildcard;
    case IndexIdentical;
    case IndexKey;
    case IndexObject;
    case IndexSpatialNull;
    case IndexSpatialOrder;
    case IndexTrigram;
    case IndexTtl;
    case IndexUnique;
    case IntegerBooleans;
    case Joins;
    case NonUtfCharacters;
    case Objects;
    case Operators;
    case OrderRandom;
    case SchemaIntrospection;
    case Schemas;
    case SpatialAxisOrder;
    /**
     * A nested transaction that fails rolls back to its savepoint and leaves the enclosing
     * transaction open. Without it a nested call runs inside the enclosing transaction and
     * its writes stay there when it fails.
     */
    case TransactionNested;
    case TransactionRetries;
    case UnsignedBigInt;
    case UpdateLock;
    case UpsertOnUniqueIndex;
    case Vectors;
}

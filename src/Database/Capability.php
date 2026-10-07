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
    case AlterLock;
    case AtomicTransactions;
    case AttributeResizing;
    case BatchCreateAttributes;
    case BatchOperations;
    case BoundaryInclusive;
    case Caching;
    case CacheSkipOnFailure;
    case CastIndexArray;
    case Casting;
    case DefinedAttributes;
    case Fulltext;
    case FulltextWildcard;
    case IdenticalIndexes;
    case Index;
    case IndexArray;
    case IntegerBooleans;
    case JSONOverlaps;
    case MultiDimensionDistance;
    case MultipleFulltextIndexes;
    /**
     * A nested transaction that fails rolls back to its savepoint and leaves the enclosing
     * transaction open. Without it a nested call runs inside the enclosing transaction and
     * its writes stay there when it fails.
     */
    case NestedTransactions;
    case NumericCasting;
    case ObjectIndexes;
    case Objects;
    case Operators;
    case OptionalSpatial;
    case OrderRandom;
    case PCRE;
    case POSIX;
    case QueryContains;
    case Regex;
    case Schemas;
    case SpatialAxisOrder;
    case SpatialIndexNull;
    case SpatialIndexOrder;
    case TTLIndexes;
    case TransactionRetries;
    case TrigramIndex;
    case UniqueIndex;
    case UpdateLock;
    case UpsertOnUniqueIndex;
    case UnsignedBigInt;
    case Vectors;
    case Joins;
    case Aggregations;
    case StatisticalAggregates;
    case BitwiseAggregates;
}

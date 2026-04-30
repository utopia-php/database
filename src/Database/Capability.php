<?php

namespace Utopia\Database;

/**
 * Defines the set of optional behavioral capabilities that a database adapter may support.
 *
 * Feature availability (method contracts) is expressed via Feature interfaces
 * on the adapter class and checked with instanceof, not capabilities.
 */
enum Capability
{
    case AlterLock;
    case AttributeResizing;
    case BatchCreateAttributes;
    case BatchOperations;
    case BoundaryInclusive;
    case CacheSkipOnFailure;
    case CastIndexArray;
    case Casting;
    case DefinedAttributes;
    case Fulltext;
    case FulltextWildcard;
    case Hostname;
    case IdenticalIndexes;
    case Index;
    case IndexArray;
    case IntegerBooleans;
    case JSONOverlaps;
    case MultiDimensionDistance;
    case MultipleFulltextIndexes;
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
    case Reconnection;
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
    case Upserts;
    case UpsertOnUniqueIndex;
    case Vectors;
    case Joins;
    case Aggregations;
    case Subqueries;
    case CTEs;
    case WindowFunctions;
}

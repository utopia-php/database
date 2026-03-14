<?php

namespace Utopia\Database;

/**
 * Defines the set of optional capabilities that a database adapter may support.
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
    case ConnectionId;
    case DefinedAttributes;
    case Fulltext;
    case FulltextWildcard;
    case Hostname;
    case IdenticalIndexes;
    case Index;
    case IndexArray;
    case IntegerBooleans;
    case InternalCasting;
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
    case Relationships;
    case SchemaAttributes;
    case Schemas;
    case Spatial;
    case SpatialAxisOrder;
    case SpatialIndexNull;
    case SpatialIndexOrder;
    case TTLIndexes;
    case Timeouts;
    case TransactionRetries;
    case TrigramIndex;
    case UTCCasting;
    case UniqueIndex;
    case UpdateLock;
    case Upserts;
    case Vectors;
    case Joins;
    case Aggregations;
}

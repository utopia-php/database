<?php

namespace Utopia\Database;

enum Capability: string
{
    case Schemas = 'schemas';
    case Attributes = 'attributes';
    case SchemaAttributes = 'schemaAttributes';
    case Index = 'index';
    case IndexArray = 'indexArray';
    case CastIndexArray = 'castIndexArray';
    case UniqueIndex = 'uniqueIndex';
    case FulltextIndex = 'fulltextIndex';
    case FulltextWildcardIndex = 'fulltextWildcardIndex';
    case Casting = 'casting';
    case QueryContains = 'queryContains';
    case Timeouts = 'timeouts';
    case Relationships = 'relationships';
    case UpdateLock = 'updateLock';
    case BatchOperations = 'batchOperations';
    case AttributeResizing = 'attributeResizing';
    case GetConnectionId = 'getConnectionId';
    case Upserts = 'upserts';
    case Vectors = 'vectors';
    case CacheSkipOnFailure = 'cacheSkipOnFailure';
    case Reconnection = 'reconnection';
    case Hostname = 'hostname';
    case BatchCreateAttributes = 'batchCreateAttributes';
    case SpatialAttributes = 'spatialAttributes';
    case Object = 'object';
    case ObjectIndexes = 'objectIndexes';
    case SpatialIndexNull = 'spatialIndexNull';
    case Operators = 'operators';
    case OptionalSpatialAttributeWithExistingRows = 'optionalSpatialAttributeWithExistingRows';
    case SpatialIndexOrder = 'spatialIndexOrder';
    case SpatialAxisOrder = 'spatialAxisOrder';
    case BoundaryInclusiveContains = 'boundaryInclusiveContains';
    case DistanceBetweenMultiDimensionGeometryInMeters = 'distanceBetweenMultiDimensionGeometryInMeters';
    case MultipleFulltextIndexes = 'multipleFulltextIndexes';
    case IdenticalIndexes = 'identicalIndexes';
    case OrderRandom = 'orderRandom';
    case InternalCasting = 'internalCasting';
    case UTCCasting = 'utcCasting';
    case IntegerBooleans = 'integerBooleans';
    case AlterLocks = 'alterLocks';
    case NonUtfCharacters = 'nonUtfCharacters';
    case TrigramIndex = 'trigramIndex';
    case PCRERegex = 'pcreRegex';
    case POSIXRegex = 'posixRegex';
    case Regex = 'regex';
    case TTLIndexes = 'ttlIndexes';
    case TransactionRetries = 'transactionRetries';
    case NestedTransactions = 'nestedTransactions';
}

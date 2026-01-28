<?php

namespace Utopia\Database;

/**
 * Enum representing database adapter capabilities.
 * Used to check if an adapter supports a specific feature.
 */
enum Capability: string
{
    case Schemas = 'schemas';
    case Attributes = 'attributes';
    case SchemaAttributes = 'schema_attributes';
    case Index = 'index';
    case IndexArray = 'index_array';
    case CastIndexArray = 'cast_index_array';
    case UniqueIndex = 'unique_index';
    case FulltextIndex = 'fulltext_index';
    case FulltextWildcardIndex = 'fulltext_wildcard_index';
    case MultipleFulltextIndexes = 'multiple_fulltext_indexes';
    case TrigramIndex = 'trigram_index';
    case IdenticalIndexes = 'identical_indexes';
    case Casting = 'casting';
    case NumericCasting = 'numeric_casting';
    case InternalCasting = 'internal_casting';
    case UTCCasting = 'utc_casting';
    case QueryContains = 'query_contains';
    case JSONOverlaps = 'json_overlaps';
    case Timeouts = 'timeouts';
    case Relationships = 'relationships';
    case UpdateLock = 'update_lock';
    case BatchOperations = 'batch_operations';
    case AttributeResizing = 'attribute_resizing';
    case GetConnectionId = 'get_connection_id';
    case Upserts = 'upserts';
    case Vectors = 'vectors';
    case CacheSkipOnFailure = 'cache_skip_on_failure';
    case Reconnection = 'reconnection';
    case Hostname = 'hostname';
    case BatchCreateAttributes = 'batch_create_attributes';
    case SpatialAttributes = 'spatial_attributes';
    case SpatialIndexNull = 'spatial_index_null';
    case SpatialIndexOrder = 'spatial_index_order';
    case SpatialAxisOrder = 'spatial_axis_order';
    case OptionalSpatialAttributeWithExistingRows = 'optional_spatial_attribute_with_existing_rows';
    case BoundaryInclusiveContains = 'boundary_inclusive_contains';
    case DistanceBetweenMultiDimensionGeometryInMeters = 'distance_between_multi_dimension_geometry_in_meters';
    case Object = 'object';
    case ObjectIndexes = 'object_indexes';
    case Operators = 'operators';
    case OrderRandom = 'order_random';
    case AlterLocks = 'alter_locks';
    case NonUtfCharacters = 'non_utf_characters';
    case IntegerBooleans = 'integer_booleans';
    case PCRERegex = 'pcre_regex';
    case POSIXRegex = 'posix_regex';
    case Regex = 'regex';
}

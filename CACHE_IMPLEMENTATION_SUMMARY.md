# Find Method Caching Implementation Summary

## Overview
This implementation adds efficient caching to the `find` method in the Database class using xxh3 hashing for consistent cache keys and version tracking for O(1) cache invalidation.

## Key Features

### 1. xxh3 Hash Function
- **Purpose**: Generate consistent and efficient hash keys for complex query parameters
- **Implementation**: PHP's built-in xxh3 hash algorithm via `hash()` function (PHP 8.1+)
- **Fallback**: SHA256 for PHP versions < 8.1
- **Location**: `generateCacheHash()` method in Database.php
- **Benefits**: Extremely fast, well-tested hashing with excellent distribution characteristics

### 2. Version Tracking for O(1) Invalidation
- **Purpose**: Enable aggressive cache invalidation without expensive cache scanning
- **Implementation**: Each collection has a version string that changes on any modification
- **Format**: `{microtime}-{random_hex}` for sub-second precision and uniqueness
- **Storage**: Version strings are cached persistently with 1-year TTL
- **Benefits**: O(1) invalidation time complexity with sub-second granularity

### 3. Find Method Caching
- **Cache Key Generation**: Uses xxh3 hash of all query parameters plus collection version
- **Cache Storage**: Results are stored as arrays and converted back to Document objects on retrieval
- **Cache Validation**: Version-based keys ensure stale data is never returned
- **Safety**: Only caches results without relationships to avoid incomplete data

### 4. Aggressive Invalidation
- **Trigger Points**: Any document create, update, or delete operation
- **Method**: Changes collection version, making all cached queries invalid instantly
- **Granularity**: Sub-second precision prevents cache inconsistencies during rapid operations
- **Priority**: Correctness over performance (as requested)
- **Implementation**: Updated `purgeCachedDocument()` and `purgeCachedCollection()` methods

## Code Changes Made

### Constants Added
```php
// Hash algorithm for cache keys
private const CACHE_HASH_ALGO = 'xxh3';
```

### New Properties
```php
/**
 * Collection version tracking for cache invalidation
 */
protected array $collectionVersions = [];
```

### New Methods
1. `generateCacheHash(string $data): string` - xxh3 hash implementation using PHP's built-in hash function
2. `getFindCacheKey(...)` - Generate cache keys for find queries
3. `getCollectionVersion(string $collectionId): string` - Get/initialize collection version
4. `incrementCollectionVersion(string $collectionId): void` - Change version for invalidation
5. `getCollectionVersionKey(string $collectionId): string` - Generate version cache key

### Modified Methods
1. `find()` - Added cache check/save logic with version validation
2. `purgeCachedCollection()` - Added version increment for invalidation
3. `purgeCachedDocument()` - Added version increment for aggressive invalidation

## Cache Key Structure
```
{cacheName}-cache-{hostname}:{namespace}:{tenant}:find:{collectionId}:{queryHash}:v{version}
```

Example:
```
default-cache-:::find:users:7a8b9c2d1e3f4567:v1691234567.123456-a1b2c3d4
```

## Performance Characteristics

### Cache Hit Performance
- **Time Complexity**: O(1) for cache lookup
- **Space Complexity**: O(n) where n is the number of documents in result set
- **Network**: Single cache read operation

### Cache Miss Performance
- **Additional Overhead**: xxh3/sha256 hash calculation (~O(k) where k is query string length)
- **Cache Write**: Single operation after query execution
- **No degradation**: Database query performance unchanged

### Invalidation Performance
- **Time Complexity**: O(1) for version change
- **Space Complexity**: O(1) additional storage per collection
- **Granularity**: Sub-second precision with microsecond accuracy
- **Immediate**: All cached queries become invalid instantly

## Usage Example

```php
// First call - cache miss, queries database
$results1 = $database->find('users', [
    Query::equal('status', 'active'),
    Query::limit(25)
]);

// Second call with same parameters - cache hit
$results2 = $database->find('users', [
    Query::equal('status', 'active'), 
    Query::limit(25)
]);

// After any document update/create/delete in 'users' collection
$database->updateDocument('users', 'user_id', $updatedDoc);

// Next call - cache miss (version changed), queries database
// Works correctly even for rapid successive operations within the same second
$results3 = $database->find('users', [
    Query::equal('status', 'active'),
    Query::limit(25)
]);
```

## Safety Features

1. **Relationship Exclusion**: Results with populated relationships are not cached to avoid incomplete data
2. **Error Handling**: Cache failures gracefully fallback to database queries
3. **Version Consistency**: Impossible to serve stale data due to version-based keys
4. **Aggressive Invalidation**: Any collection change invalidates ALL cached queries for that collection

## Configuration

- **TTL**: Uses existing `Database::TTL` constant (24 hours)
- **Version Storage**: 1-year TTL for version numbers
- **Cache Backend**: Uses existing cache infrastructure
- **Hash Algorithm**: xxh3 (PHP 8.1+) with SHA256 fallback for older versions

## Monitoring

The implementation includes logging for cache operations:
- Cache read failures are logged as warnings
- Cache write failures are logged as warnings
- No performance impact from logging failures

This implementation prioritizes data correctness over cache hit rates, ensuring that stale data is never returned while providing significant performance improvements for repeated queries.
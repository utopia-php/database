# Database Abstraction Layer

The goal of this library is to serve as an abstraction layer on top of multiple database adapters and allow storage and query of JSON based documents and relationships.

## Adapters

This library will abstract multiple database technologies using the adapter's design patterns. Below is a list of various adapters we should consider to support:
* MariaDB
* MySQL
* Postgres
* SQLite
* MongoDB
* Redis
* Memory

## Data Types

This library will support storing and fetching of all common JSON simple and complex [data types](https://restfulapi.net/json-data-types/).

### Simple Types

* String
* Integer
* Float
* Boolean
* Null

### Complex Types
* Array
* Object
* Relationships
  * Reference (collection / document)
  * References (Array of - collection / document)

Databases that don't support the storage of complex data types should store them as strings and parse them correctly when fetched.

## Persistency

Each database adapter should support the following action for fast storing and retrieval of collections of documents.

**Databases** (Schemas for MariaDB)
* create(string $name)
* update(string $name, string $new)
* exists(string $database)
* delete(string $name)

**Collections** (Tables for MariaDB)
* createCollection(string $collection, array $attributes = [], array $indexes = [])
* collectionExists(string $database, string $collection)
* deleteCollection(string $collection)

**Attributes** (Table columns for MariaDB)
* createAttribute(string $collection, Attribute $attribute)
* updateAttribute(string $collection, string $key, Attribute $attribute)
* renameAttribute(string $collection, string $old, string $new)
* deleteAttribute(string $collection, string $key)

**Indices** (Table indices for MariaDB)
* createIndex(string $collection, Index $index)
* renameIndex(string $collection, string $old, string $new)
* deleteIndex(string $collection, string $key)

**Documents** (Table rows columns for MariaDB)
* getDocument(Document $collection, string $id)
* createDocument(Document $collection, Document $document)
* updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions)
* deleteDocument(Document $collection, string $id)

**Limits**
* limits(): Adapter\Limits

## Queries

Each database adapter should allow querying simple and advanced queries in consideration of underline limitations.

Method for quering data:
* find(Document $collection, array $queries)
* count(Document $collection, array $queries)

### Supported Query Operations
* Equal (==)
* Not Equal (!=)
* Less Than (<)
* Less or equal (<=)
* Bigger Than (>)
* Bigger or equal (>=)
* Containes / In
* Is Null
* Is Empty

### Joins / Relationships

## Paging

Each database adapter should support two methods for paging. The first method is the classic `Limit and Offset`. The second method is `After` paging, which allows better performance at a larger scale.

## Orders

Multi-column support, order type, use native types

## Features

> Each database collection should hold parallel tables (if needed) with row-level metadata, used to abstract features that are not enabled in all the adapters.

### Row-level Security

### GEO Queries

### Free Search

### Filters

Allow to apply custom filters on specific pre-chosen fields. Avaliable filters:

* Encryption
* JSON (might be redundent with object support)
* Hashing (md5,bcrypt)

## Caching

The library should support memory caching using internal or external memory devices for all read operations. Write operations should actively clean or update the cache.

## Encoding

All database adapters should support UTF-8 encoding and accept emoji characters.

## Documentation

* Data Types
* Operations
* Security
* Benchmarks
* Performance Tips
* Known Limitations

## Tests

* Check for SQL Injections

## Examples (MariaDB)

**Collections Metadata**

```sql
CREATE TABLE IF NOT EXISTS `collections` (
  `_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `_metadata` text() DEFAULT NULL,
  PRIMARY KEY (`id`),
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**Documents**

```sql
CREATE TABLE IF NOT EXISTS `documents_[NAME]` (
  `_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `_uid` varchar(128) NOT NULL AUTO_INCREMENT,
  `custom1` text() DEFAULT NULL,
  `custom2` text() DEFAULT NULL,
  `custom3` text() DEFAULT NULL,
  PRIMARY KEY (`_id`),
  UNIQUE KEY `_index1` (`$id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**Documents Authorization**

```sql
CREATE TABLE IF NOT EXISTS `documents_[NAME]_authorization` (
  `_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `_document` varchar(128) DEFAULT NULL,
  `_role` varchar(128) DEFAULT NULL,
  `_action` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`_id`),
  KEY `_index1` (`_document`)
  KEY `_index1` (`_document`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
``` 

## Optimization Tools

https://www.eversql.com/
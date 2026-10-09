# Add new Database Adapter

To get started with implementing a new adapter, start by reviewing the [specification](/SPEC.md) to understand the goals of this library. The specification defines the NoSQL-inspired API methods and contains all of the functions a new adapter must support, including types, queries, paging, indexes, and especially emojis ❤️.

An adapter describes what it supports in four ways:

- **Mandatory methods.** The abstract methods of `Utopia\Database\Adapter` are the whole contract; every adapter implements them. They cover databases, collections, attributes, indexes, documents, transactions, schema introspection, limits and the driver.
- **Optional features.** A group of methods an adapter may or may not provide is an interface in `src/Database/Adapter/Feature/`. Implement the ones your database supports. Callers check them with `$adapter->hasFeature(Feature\Upserts::class)`.
- **Capabilities.** A behaviour flag is a `Utopia\Database\Capability` case. Override `capabilities()` and return the cases your adapter supports (start from `parent::capabilities()`); callers check them with `$adapter->supports(Capability::IndexFulltext)`.
- **Limits.** `limits()` returns one `Adapter\Limits` value with every size and count the library validates against.

### Mandatory methods

| Group | Abstract methods on `Adapter` |
|---|---|
| Databases | `create(string $name)`, `update(string $name, string $new)`, `exists(string $database)`, `list()`, `delete(string $name)` |
| Collections | `createCollection(string $collection, array $attributes = [], array $indexes = [])`, `collectionExists(string $database, string $collection)`, `deleteCollection(string $collection)`, `analyzeCollection(string $collection)`, `getSizeOfCollection()`, `getSizeOfCollectionOnDisk()` |
| Attributes | `createAttribute(string $collection, Attribute $attribute)`, `createAttributes(string $collection, array $attributes)`, `updateAttribute(string $collection, string $key, Attribute $attribute)`, `renameAttribute()`, `deleteAttribute(string $collection, string $key)`, `getColumnType(Attribute $attribute): ?string`, `getAttributeWidth()`, `getCountOfAttributes()` |
| Indexes | `createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = [])`, `renameIndex()`, `deleteIndex(string $collection, string $key)`, `getCountOfIndexes()` |
| Introspection | `getSchemaAttributes(string $collection): array` (`list<Schema\Column>`), `getSchemaIndexes(string $collection): array` (`list<Schema\Index>`) |
| Documents | `getDocument()`, `createDocument()`, `createDocuments()`, `updateDocument()`, `updateDocuments()`, `increaseDocumentAttribute()`, `deleteDocument()`, `deleteDocuments()`, `getSequences()`, `find()`, `count()`, `sum()`. Every one takes the collection `Document` as its first argument |
| Transactions | `startTransaction()`, `commitTransaction()`, `rollbackTransaction()` |
| Other | `limits(): Adapter\Limits`, `getDriver(): object` |

`attributes` and `indexes` are lists of the `Attribute` and `Index` value objects; read their typed properties (`$attribute->type`, `$index->orders`).

An engine without schema introspection returns `[]` from `getSchemaAttributes()` and `getSchemaIndexes()`, and `null` from `getColumnType()`, and does not declare `Capability::SchemaIntrospection`. An engine that can read its columns and indexes back declares it: the library then compares the schema with the metadata to adopt or replace orphan columns and indexes. Report each column's canonical native type, the same spelling `getColumnType()` returns for an attribute.

`update()` renames a database. Refuse it under shared tables (`hasSharedTables()`), and roll back a rename that fails part-way.

### Limits

```php
public function limits(): Limits
{
    return new Limits(
        string: 1_073_741_824,
        varchar: 16_381,
        integer: 4_294_967_295,
        bigInteger: Database::MAX_BIG_INT,
        attributes: 1_017,
        indexes: 64,
        defaultAttributes: \count(Database::INTERNAL_ATTRIBUTE_KEYS),
        defaultIndexes: \count(Database::INTERNAL_INDEXES),
        indexLength: 768,
        uidLength: 255,
        documentSize: 65_535,
        minDateTime: new \DateTime('0000-01-01'),
        maxDateTime: new \DateTime('9999-12-31'),
        idType: ColumnType::Integer,
        keywords: [],
        internalIndexKeys: [],
    );
}
```

A limit of 0 means none. `idType` is the type of the `$sequence` column. `keywords` are names the engine reserves, which attribute keys may not use.

### Optional features

| Interface | Implement it when |
|---|---|
| `Feature\Casting` (`castBefore()`, `castAfter()` over a page of documents, `castDatetime()`) | The engine returns native PHP types. Without it the library casts every value read |
| `Feature\Connection` (`ping()`, `reconnect()`, `id()`, `hostname()`) | The adapter holds a connection |
| `Feature\Timeouts` (`setTimeout()`, `clearTimeout()`, `getTimeout()`) | The engine can bound a statement's run time. The `Adapter\Timeout` trait keeps the per-event state |
| `Feature\Relationships` (`createRelationship()`, `updateRelationship()`, `deleteRelationship()`) | The adapter stores relationship attributes and junctions |
| `Feature\Upserts` (`upsertDocument(Document $collection, Change $change)`, `upsertDocuments(Document $collection, array $changes, ?string $increase = null)`) | The engine can insert or update in one statement |
| `Feature\Spatial` (`encode()`, `decode()`) | The engine stores point, linestring and polygon attributes |
| `Feature\Schemaless` (`setSchemaless()`, `isSchemaless()`) | The engine can store attributes the collection does not declare |
| `Feature\QueryBuilder` (`builder()`, `schema()`), `Feature\RawQuery` (`rawQuery()`, `rawMutation()`) | The engine runs utopia-php/query builder statements or raw statements |

`Adapter\Pool` forwards every optional feature to the adapter it borrows without implementing the interface, so the library always asks `hasFeature()`, never `instanceof`.

### File Structure

Below are outlined the most useful files for adding a new database adapter:

```bash
.
├── src # Source code
│   └── Database
│       ├── Adapter/ # Where your new adapter goes!
│       │   ├── Feature/ # Optional feature interfaces
│       │   ├── Limits.php # The adapter's limits
│       │   ├── SQL.php # Shared base of the SQL adapters
│       │   └── SQL/Hook/ # Permission, tenant and join hooks of the SQL adapters
│       ├── Adapter.php # Parent class and mandatory contract
│       ├── Attribute.php # Attribute value object
│       ├── Capability.php # Behaviour flags an adapter reports
│       ├── Database.php # Database class - calls individual adapter methods
│       ├── Document.php # Document class
│       ├── Hook/ # Permission, tenancy and relationship hooks
│       ├── Index.php # Index value object
│       └── Query.php # Query class - holds query attributes, methods, and values
└── tests
    ├── e2e
    │   └── Adapter/ # One test class per adapter, extending Base.php
    │       ├── Base.php # Parent class that pulls in the Scopes test traits
    │       └── Scopes/ # The shared tests every adapter runs
    └── unit/
```

### Extend the Adapter

Create your `NewDB.php` file in `src/Database/Adapter/` and extend the parent class, implementing the optional feature interfaces your database supports:

```php
<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Capability;

class NewDB extends Adapter implements Feature\Connection, Feature\Upserts
{
    public function capabilities(): array
    {
        return [
            ...parent::capabilities(),
            Capability::IndexFulltext,
            Capability::TransactionNested,
        ];
    }
}
```

A SQL database extends `Utopia\Database\Adapter\SQL` instead, which already implements `Feature\Connection`, `Feature\QueryBuilder`, `Feature\RawQuery`, `Feature\Relationships` and `Feature\Upserts`. It builds its statements with a utopia-php/query builder, so implement `builder()` to return the builder for your dialect, plus the other abstract methods of `SQL`: `getColumnNames()`, `getMaxPointSize()`, `getOperatorSql()` and the upsert conflict expressions (`getConflictTenantExpression()`, `getConflictIncrementExpression()`, `getConflictTenantIncrementExpression()`). The builder implements `Utopia\Database\Builder\Scoping` with the `Utopia\Database\Builder\ScopesCollections` trait and is scoped with the adapter's `scope()`, so that its `from()` takes a collection id: `return (new MyBuilder())->scope($this->scope());`. Override `qualifyTable()` if your tables are stored under other names.

Only include dependencies strictly necessary for the database, preferably official PHP libraries, if available.

### Testing with Docker

The existing test suite is helpful when developing a new database adapter. To get started with testing, add a stanza to `docker-compose.yml` with your new database, using existing adapters as examples. Use official Docker images from trusted sources, and provide the necessary `environment` variables for startup. Then, create `tests/e2e/Adapter/NewDBTest.php` extending the `Base.php` test class and implement `getDatabase()`. The tests that do not apply to your adapter skip themselves by its capabilities and features. The `docker compose` commands for testing are in [CONTRIBUTING.md](/CONTRIBUTING.md#tests).

### Tips and Tricks

- Keep it simple :)
- Databases are namespaced, so name them with `$this->getNamespace()` from the parent Adapter class.
- Create indexes for `$id` and `$permissions` fields by default for query performance.
- Filter new IDs with `$this->filter($id);`
- Prioritize code performance.
- Report only the capabilities your adapter honours: `Database` and the query validators refuse queries that need a capability the adapter does not report (for example joins without `Capability::Joins`).
- `find()`, `count()` and `sum()` receive the permission type to check; only return documents the caller's roles may read. The SQL adapters filter with the hooks in `src/Database/Adapter/SQL/Hook` (`Permission\Filter`, `Tenant\Filter`), MongoDB with `Hook\Mongo\Permission` and `Hook\Mongo\Tenant`.
- Compare types, index types and orders with the enums (`Utopia\Query\Schema\ColumnType`, `IndexType`, `Utopia\Query\OrderDirection`, ...), not strings.
- Declare `Capability::TransactionNested` only when a failed nested transaction rolls back to its savepoint and leaves the enclosing transaction open.

#### SQL Databases
- Treat Collections as tables and Documents as rows, with attributes as columns. NoSQL databases are more straight-forward to translate.
- For row-level permissions, create a pair of tables: one for data and one for permissions (`{collection}_perms`). `Hook\Permissions`, registered on the `Database`, writes the permission rows through the adapter's `Hook\WriteContext`. The MariaDB adapter demonstrates the implementation.

#### NoSQL Databases
- NoSQL databases may not need to implement the attribute functions. See the MongoDB adapter as an example: it implements `Feature\Schemaless`, which `Database::setSchemaless()` switches, and reports `Capability::DefinedAttributes` only while it enforces the collection's attributes.
- An engine that returns native types implements `Feature\Casting`, as MongoDB does; the library then leaves value casting to the adapter.

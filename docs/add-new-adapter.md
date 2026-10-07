# Add new Database Adapter

To get started with implementing a new adapter, start by reviewing the [specification](/SPEC.md) to understand the goals of this library. The specification defines the NoSQL-inspired API methods and contains all of the functions a new adapter must support, including types, queries, paging, indexes, and especially emojis ❤️.

An adapter describes what it supports in three ways:

- **Required methods.** The mandatory contract is the abstract methods of `Utopia\Database\Adapter` (databases, collections, attributes, indexes, documents and transactions), so every adapter implements them. The document methods take the collection `Document`. SQL-only helpers such as `quote()` and `execute()` live on `Adapter\SQL`.
- **Optional features.** A group of methods an adapter may or may not provide is an interface in `src/Database/Adapter/Feature/`: `ColumnTypes`, `ConnectionId`, `InternalCasting`, `QueryBuilder`, `RawQuery`, `Relationships`, `SchemaAttributes`, `SchemaIndexes`, `Spatial`, `Timeouts`, `Upserts` and `UTCCasting`. Implement the ones your database supports. Callers check them with `$adapter->hasFeature(Feature\Upserts::class)`.
- **Capabilities.** A behaviour flag is a `Utopia\Database\Capability` case. Override `capabilities()` and return the cases your adapter supports (start from `parent::capabilities()`); callers check them with `$adapter->supports(Capability::Fulltext)`. The base adapter reports `Index`, `IndexArray` and `UniqueIndex`.

Limits are the `getLimitFor*()`, `getMax*()` and `getDocumentSizeLimit()` methods of `Adapter`.

### File Structure

Below are outlined the most useful files for adding a new database adapter:

```bash
.
├── src # Source code
│   └── Database
│       ├── Adapter/ # Where your new adapter goes!
│       │   ├── Feature/ # Optional feature interfaces
│       │   └── SQL.php # Shared base of the SQL adapters
│       ├── Adapter.php # Parent class for individual adapters
│       ├── Capability.php # Behaviour flags an adapter reports
│       ├── Database.php # Database class - calls individual adapter methods
│       ├── Document.php # Document class
│       ├── Hook/ # Permission, tenancy and relationship hooks
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

class NewDB extends Adapter implements Feature\Upserts
{
    public function capabilities(): array
    {
        return [
            ...parent::capabilities(),
            Capability::Fulltext,
        ];
    }
}
```

A SQL database extends `Utopia\Database\Adapter\SQL` instead. It builds its statements with a utopia-php/query builder, so implement `createBuilder()` to return the builder for your dialect, plus the other abstract methods of `SQL` (`getColumnNames()`, `getOperatorSQL()` and the upsert conflict expressions).

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
- `find()`, `count()` and `sum()` receive the permission type to check; only return documents the caller's roles may read. The SQL adapters filter with the hooks in `src/Database/Hook` (`PermissionFilter`, `TenantFilter`), MongoDB with `Hook\Mongo\PermissionFilter` and `Hook\Mongo\TenantFilter`.
- Compare types, index types and orders with the enums (`Utopia\Query\Schema\ColumnType`, `IndexType`, `Utopia\Query\OrderDirection`, ...), not strings.

#### SQL Databases
- Treat Collections as tables and Documents as rows, with attributes as columns. NoSQL databases are more straight-forward to translate.
- For row-level permissions, create a pair of tables: one for data and one for permissions (`{collection}_perms`). `Hook\Permissions`, registered on the `Database`, writes the permission rows through the adapter's write hooks. The MariaDB adapter demonstrates the implementation.

#### NoSQL Databases
- NoSQL databases may not need to implement the attribute functions. See the MongoDB adapter as an example: it supports a schemaless mode, reported through `Capability::DefinedAttributes` and `setSupportForAttributes()`.

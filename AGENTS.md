# Utopia Database

PHP database abstraction library with a unified API across MariaDB, MySQL, PostgreSQL, SQLite, MongoDB, Redis and an in-memory adapter.

## Commands

| Command | Purpose |
|---------|---------|
| `composer build` | Build Docker containers |
| `composer start` | Start all database containers in background |
| `composer test` | Run tests in Docker (ParaTest, 4 parallel processes) |
| `composer lint` | Check formatting (Pint, PSR-12) |
| `composer format` | Auto-format code |
| `composer check` | Static analysis (PHPStan, max level, 2GB) |

Run a single test:
```bash
docker compose exec tests vendor/bin/phpunit --configuration phpunit.xml tests/e2e/Adapter/MariaDBTest.php
docker compose exec tests vendor/bin/phpunit --configuration phpunit.xml tests/unit/Validator/SomeTest.php
```

## Stack

- PHP 8.5+, Docker Compose for test databases
- ParaTest (parallel PHPUnit), Pint (PSR-12), PHPStan (max level)
- Test databases: MariaDB 10.11, MySQL 8.0.43, PostgreSQL 16, SQLite, MongoDB 8.0.14
- Redis 8.2.1 for caching tests

## Project layout

- **src/Database/** -- core library (PSR-4 namespace `Utopia\Database\`)
  - `Database.php` -- main API class (uses trait composition for organization)
  - `Adapter.php` -- base adapter class all engines extend; its abstract methods are the mandatory adapter contract
  - `Adapter/` -- engine implementations: MariaDB, MySQL, Postgres, SQLite, Mongo, Memory, Redis, Pool, ReadWritePool; `Limits` and `Profile` value objects
  - `Adapter/SQL.php` -- shared SQL adapter base (MariaDB, MySQL, Postgres, SQLite extend this); `Adapter/SQL/Hook/` holds the SQL permission, tenant, join and column hooks and the `WriteContext` implementation
  - `Adapter/Feature/` -- optional feature interfaces only: Casting, Connection, QueryBuilder, RawQuery, Relationships, Schemaless, Spatial, Timeouts, Upserts
  - `Capability.php` -- behaviour flags an adapter reports through `supports()`
  - `Document.php` -- JSON document model (extends ArrayObject)
  - `Collection.php` -- the collection definition (a `Document`, built with `Collection::create()`)
  - `Attribute.php`, `Index.php`, `Relationship.php` -- `final readonly` schema value objects with a factory per type; `AttributeUpdate`, `CollectionUpdate`, `RelationshipUpdate` are the update models
  - `Schema/` -- `Column` and `Index`, the physical schema read back from an engine
  - `Mirror.php`, `Mirror/` -- database mirroring/replication, its write `Filter` and `Failure`
  - `Query.php` -- query builder extension
  - `Trait/` -- Database.php composition: Attributes, Collections, Databases, Documents, Indexes, Relationships, Transactions
  - `Hook/` -- hook interfaces (Lifecycle, Named, Selective, Attachable, Decorator, Transform, Write, WriteContext) and hooks (Permissions, Relationships, Tenancy, Interceptor); `Hook/Mongo/` holds the MongoDB read hooks
  - `Event.php`, `Event/` -- the event enum, one typed event class per event under Database/, Collection/, Attribute/, Index/, Document/, Permission/, plus Domain and DispatcherHook
  - `Filter.php`, `Filter/` -- built-in filter names, and the Codec, Callback and Registry for instance filters
  - `Cache/` -- `find()` query cache (`Query`, `Invalidator`) and cache bookkeeping
  - `Profiler.php`, `Profiler/` -- query profiler and its `Log` entries
  - `State/` -- per-coroutine state (Value, Frame) and Snapshot
  - `Validator/` -- input validators, with `Authorization/`, `Queries/` (Base, Indexed, Document, Documents) and `Query/` (one per query type, `Joined/`)
  - `Id.php`, `Permission.php`, `Role.php` -- id, permission and role helpers
  - `Exception/` -- typed exceptions; `Schema`, `Query` and `Transaction` group related ones

- **tests/unit/** -- unit tests for value objects, validators, hooks, events and adapters
- **tests/e2e/Adapter/** -- E2E tests against real databases
  - `Base.php` -- abstract test class all adapter tests extend
  - `Scopes/` -- test trait mixins (DocumentTests, AttributeTests, CollectionTests, PermissionTests, RelationshipTests, SpatialTests, VectorTests, etc.)
  - Each adapter test (MariaDBTest, PostgresTest, etc.) extends Base and gets all scope traits

## Key patterns

**Multi-adapter:** Single `Database` class with engine-specific `Adapter` implementations. SQL adapters share `SQL.php` base; MongoDB has its own.

**Document model:** Documents are ArrayObject subclasses with reserved attributes: `$id`, `$sequence`, `$createdAt`, `$updatedAt`, `$collection`, `$permissions`.

**Hook system:** Pluggable hooks for permissions, relationships, tenancy filtering, and lifecycle events, registered with `Database::addHook()`. A database registers neither `Hook\Permissions` nor `Hook\Relationships` on its own.

**Schema API:** Collection-first methods take value objects: `createCollection(Collection::create(...))`, `createAttribute($collection, Attribute::string(...))`, `createIndex($collection, Index::key(...))`, `createRelationship($collection, Relationship::oneToMany(...))`, and updates take `AttributeUpdate`, `CollectionUpdate`, `RelationshipUpdate`.

**Adapter model:** Ask `supports(Capability::X)` for behaviour flags and `hasFeature(Feature\X::class)` for optional features (never `instanceof`: `Pool` forwards features without implementing them). Limits come from `limits()`; `Database::profile()` snapshots them for the validators.

**Custom document types:**
```php
$database->setDocumentType('users', User::class);
$user = $database->getDocument('users', 'id123'); // Returns User instance
```

**Trait composition:** `Database.php` splits its API across the traits in `Trait/`. Each trait groups related operations (documents, attributes, indexes, etc.).

**Connection pooling:** `Pool` adapter wraps multiple connections. `ReadWritePool` distributes reads and writes to separate pools.

**Query builder:** Integrates with `utopia-php/query`. Queries grouped by type: filters, selections, aggregations, ordering, pagination.

## Testing patterns

- E2E tests extend `Base.php` which provides setUp/tearDown for real database connections
- Test functionality split into trait mixins in `Scopes/` -- each adapter test includes all relevant traits
- Unit tests in `tests/unit/` for value objects, validators, hooks, events and adapters with test doubles
- Tests check for `ext-swoole` and skip if missing

## Docker services

```bash
composer build && composer start   # Start all databases
```

Services (activated via Docker Compose profiles):
- `mariadb` (host port 8703), `mysql` (8706), `postgres` (8701), `mongo` (9706)
- `redis` (8708) for caching
- Mirror variants for replication tests
- `adminer` (port 8700, debug profile) for database UI

## Load testing

```bash
bin/load --adapter=mariadb      # Populate test data
bin/index --adapter=mariadb     # Create indexes
bin/query --adapter=mariadb     # Run queries
bin/compare                     # Visualize at localhost:8708
```

## Conventions

- PSR-12 via Pint, PSR-4 autoloading
- One class per file, filename matches class name
- Full type hints on all parameters and returns, readonly properties for immutable data
- Imports: alphabetical, single per statement, grouped by const/class/function
- Constants: UPPER_SNAKE_CASE
- Methods: camelCase with verb prefixes (get*, set*, create*, update*, delete*)

## Cross-repo context

Changes to the query builder, the `Adapter` contract or the `Pool` extension points may break appwrite, Appwrite Cloud and utopia-php/migration, which subclass `Adapter\Pool`. Run their tests after such changes.

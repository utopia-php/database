# Utopia Database

PHP database abstraction library with a unified API across MariaDB 10.5, MySQL 8.0, PostgreSQL 13+, SQLite 3.38+, and MongoDB.

## Commands

| Command | Purpose |
|---------|---------|
| `composer build` | Build Docker containers |
| `composer start` | Start all database containers in background |
| `composer test` | Run tests in Docker (ParaTest, 4 parallel processes) |
| `composer lint` | Check formatting (Pint, PSR-12) |
| `composer format` | Auto-format code |
| `composer check` | Static analysis (PHPStan, max level, 2GB) |
| `composer coverage` | Check test coverage (90% minimum required) |

Run a single test:
```bash
docker compose exec tests vendor/bin/phpunit --configuration phpunit.xml tests/e2e/Adapter/MariaDBTest.php
docker compose exec tests vendor/bin/phpunit --configuration phpunit.xml tests/unit/Validator/SomeTest.php
```

## Stack

- PHP 8.4+, Docker Compose for test databases
- ParaTest (parallel PHPUnit), Pint (PSR-12), PHPStan (max level)
- Test databases: MariaDB 10.11, MySQL 8.0.43, PostgreSQL 16, SQLite, MongoDB 8.0.14
- Redis 8.2.1 for caching tests

## Project layout

- **src/Database/** -- core library (PSR-4 namespace `Utopia\Database\`)
  - `Database.php` -- main API class (uses trait composition for organization)
  - `Adapter.php` -- base adapter class all engines extend
  - `Adapter/` -- engine implementations: MariaDB, MySQL, Postgres, SQLite, Mongo, Pool, ReadWritePool
  - `Adapter/SQL.php` -- shared SQL adapter base (MariaDB, MySQL, Postgres, SQLite extend this)
  - `Adapter/Feature/` -- capability interfaces for adapter features
  - `Document.php` -- JSON document model (extends ArrayObject)
  - `Mirror.php` -- database mirroring/replication
  - `Query.php` -- query builder extension
  - `Attribute.php` -- attribute type definitions
  - `Index.php` -- index management
  - `Relationship.php` -- relationship definitions
  - `Traits/` -- Database.php composition: Async, Attributes, Collections, Databases, Documents, Entities, Indexes, Relationships, Transactions
  - `Hook/` -- event hooks and interceptors: Lifecycle, Permissions, Relationships, TenantFilter, Transform, Read, Write, WriteContext, Interceptor, Decorator, PermissionFilter, MongoPermissionFilter, MongoTenantFilter, Tenancy
  - `ORM/` -- EntityManager, EntityMapper, EntityMetadata, EntityState, IdentityMap, MetadataFactory, UnitOfWork, ColumnMapping, EmbeddableMapping, RelationshipMapping, plus `Mapping/` subdirectory (PHP attribute annotations: Entity, Column, Id, HasMany, HasOne, BelongsTo, Embedded, Permissions, Tenant, etc.)
  - `Schema/` -- Introspector, SchemaDiff, SchemaChange, SchemaChangeType, DiffResult
  - `Validator/` -- input validators (19 top-level + subdirectories)
  - `Helpers/` -- ID, Permission, Role utilities
  - `Exception/` -- 18 exception types (Authorization, Duplicate, Limit, Query, Timeout, etc.)

- **tests/unit/** -- unit tests for validators, helpers, etc.
- **tests/e2e/Adapter/** -- E2E tests against real databases
  - `Base.php` -- abstract test class all adapter tests extend
  - `Scopes/` -- test trait mixins (DocumentTests, AttributeTests, CollectionTests, PermissionTests, RelationshipTests, SpatialTests, VectorTests, etc.)
  - Each adapter test (MariaDBTest, PostgresTest, etc.) extends Base and gets all scope traits

## Key patterns

**Multi-adapter:** Single `Database` class with engine-specific `Adapter` implementations. SQL adapters share `SQL.php` base; MongoDB has its own.

**Document model:** Documents are ArrayObject subclasses with reserved attributes: `$id`, `$sequence`, `$createdAt`, `$updatedAt`, `$collection`, `$permissions`.

**Hook system:** Pluggable hooks for permissions, relationships, tenancy filtering, and lifecycle events. Hooks registered on the Database instance.

**Custom document types:**
```php
$database->setDocumentType('users', User::class);
$user = $database->getDocument('users', 'id123'); // Returns User instance
```

**Trait composition:** `Database.php` splits its API across 9 traits in `Traits/` for organization. Each trait groups related operations (documents, attributes, indexes, entities, etc.).

**Connection pooling:** `Pool` adapter wraps multiple connections. `ReadWritePool` distributes reads and writes to separate pools.

**Query builder:** Integrates with `utopia-php/query`. Queries grouped by type: filters, selections, aggregations, ordering, pagination.

## Testing patterns

- E2E tests extend `Base.php` which provides setUp/tearDown for real database connections
- Test functionality split into trait mixins in `Scopes/` -- each adapter test includes all relevant traits
- Unit tests in `tests/unit/` for validators and helpers
- Tests check for `ext-swoole` and skip if missing

## Docker services

```bash
composer build && composer start   # Start all databases
```

Services (activated via Docker Compose profiles):
- `mariadb` (port 3306), `mysql` (port 3307), `postgres` (port 5432), `mongo` (port 27017)
- `redis` (port 6379) for caching
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

Changes to the Query builder or Adapter interface may break appwrite. Run `composer test` in both repos after adapter changes.

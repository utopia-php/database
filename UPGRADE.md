# Upgrading from 7.x to 8.0

This guide lists the changes you may need to make when you move from utopia-php/database 7.x (last release 7.3.12)
to 8.0. [CHANGELOG.md](CHANGELOG.md) lists everything that is new in 8.0.

- [Before you start](#before-you-start)
- [Register the permission and relationship hooks](#register-the-permission-and-relationship-hooks)
- [Constants are now enums](#constants-are-now-enums)
- [Queries](#queries)
- [Schema: typed models](#schema-typed-models)
- [Lifecycle events are hooks](#lifecycle-events-are-hooks)
- [Relationships](#relationships)
- [Documents](#documents)
- [Coroutines](#coroutines)
- [Errors](#errors)
- [Caches](#caches)
- [Adapters](#adapters)
- [Mirror](#mirror)
- [Validators and helpers](#validators-and-helpers)
- [Rules for features new in 8.0](#rules-for-features-new-in-80)
- [Known limitations](#known-limitations)

## Before you start

- PHP 8.5 or later is required, as for 7.x.
- Two new dependencies are installed with the library. `utopia-php/query` 0.6 provides the query, schema and
  builder types that 8.0 uses in its signatures (`Utopia\Query\Method`, `Utopia\Query\Schema\ColumnType`, ...).
  `utopia-php/async` 0.2 is used by `Mirror` replication and the relationship hook. Its
  `Utopia\Async\Serializer::unserialize()` no longer decodes closure payloads: code of your own that calls it on
  closure payloads switches to `Serializer::unserializeTrusted()` (see utopia-php/async's UPGRADE.md).
- Many constants became enums, and many signatures now take or return enum cases. PHP never treats an enum case as
  equal to a string, so a comparison like `$query->getMethod() === 'equal'` is now always `false` and raises no
  error. Run PHPStan at level 4 or higher on your code after upgrading: it reports these comparisons.

## Register the permission and relationship hooks

Document permissions and relationships are now hooks, and a `Database` registers neither on its own. Register both
right after you create the `Database`:

```php
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;

$database->addHook(new Permissions());
$database->addHook(new Relationships($database));
```

- `Hook\Permissions` writes, moves and deletes the rows of each collection's permissions table (`_perms`) when a
  document's `$permissions` change. What depends on those rows differs by engine:
  - MariaDB, MySQL and SQLite check document-level permissions in `find()`, `count()` and `sum()` against these
    rows. Without the hook they write no new rows and neither revoke nor delete existing ones, so these reads keep
    following the rows written before (for example by 7.x): a permission removed with `updateDocument()` still makes
    the document readable through `find()` while `getDocument()` refuses it, a deleted document's rows stay, and a
    document created again with the same id is readable by the roles the deleted one granted. A document whose
    permissions were written without the hook is returned by `find()` only through a collection-level permission.
  - PostgreSQL checks the `_permissions` column of the document's own row, as in 7.x, so its reads follow the
    current `$permissions` with or without the hook. The hook still maintains the `_perms` rows there.
  - MongoDB, Memory and Redis keep permissions with the document and do not need it.
- `Hook\Relationships` populates related documents on reads and runs nested writes, the `onDelete` rules
  (`Cascade`, `SetNull`, `Restrict`) and the relationship permission checks. Without it, a read returns a
  relationship attribute's stored value (the related document's id) instead of the related document, a nested
  related document cannot be written, and deleting a document leaves the documents related to it unchanged: no
  cascade runs, no key is set to null and `Restrict` does not block the delete. `Database::getRelationshipHook()`
  returns the registered hook.

## Constants are now enums

Every removed string constant maps to an enum case with the same backing value, except `Database::VAR_BIGINT` (see
[Attribute types](#attribute-types)). Stored metadata and query strings do not change.

### `Database`

| 7.x | 8.0 |
|---|---|
| `VAR_STRING`, `VAR_VARCHAR`, `VAR_TEXT`, `VAR_MEDIUMTEXT`, `VAR_LONGTEXT`, `VAR_INTEGER`, `VAR_BOOLEAN`, `VAR_DATETIME`, `VAR_ID`, `VAR_UUID7`, `VAR_OBJECT`, `VAR_VECTOR`, `VAR_RELATIONSHIP`, `VAR_POINT`, `VAR_LINESTRING`, `VAR_POLYGON` | `Utopia\Query\Schema\ColumnType::String`, `Varchar`, `Text`, `MediumText`, `LongText`, `Integer`, `Boolean`, `Datetime`, `Id`, `Uuid7`, `Object`, `Vector`, `Relationship`, `Point`, `Linestring`, `Polygon` |
| `VAR_FLOAT` (`'double'`) | `ColumnType::Double`. `ColumnType::Float` (`'float'`) is a separate, new type |
| `VAR_BIGINT` (`'bigint'`) | `ColumnType::BigInteger`, whose value is `'biginteger'` (see [Attribute types](#attribute-types)) |
| `STRING_TYPES` | No replacement: list the string cases (`String`, `Varchar`, `Text`, `MediumText`, `LongText`) |
| `SPATIAL_TYPES` | `Attribute::isSpatialType($type)` |
| `ATTRIBUTE_FILTER_TYPES` | `ATTRIBUTE_FILTER_COLUMN_TYPES`, which holds `ColumnType` cases (see [Attribute types](#attribute-types)) |
| `INDEX_KEY`, `INDEX_UNIQUE`, `INDEX_FULLTEXT`, `INDEX_SPATIAL`, `INDEX_OBJECT`, `INDEX_TRIGRAM`, `INDEX_TTL`, `INDEX_HNSW_EUCLIDEAN`, `INDEX_HNSW_COSINE`, `INDEX_HNSW_DOT` | `Utopia\Query\Schema\IndexType::Key`, `Unique`, `Fulltext`, `Spatial`, `Object`, `Trigram`, `Ttl`, `HnswEuclidean`, `HnswCosine`, `HnswDot` |
| `ORDER_ASC`, `ORDER_DESC` | Index orders: `Utopia\Query\Schema\Order::Asc`, `Desc`. Adapter order types: `Utopia\Query\OrderDirection::Asc`, `Desc` |
| `ORDER_RANDOM` | `Utopia\Query\OrderDirection::Random`, or `Query::orderRandom()` |
| `PERMISSION_CREATE`, `PERMISSION_READ`, `PERMISSION_UPDATE`, `PERMISSION_DELETE`, `PERMISSION_WRITE` | `Utopia\Database\PermissionType::Create`, `Read`, `Update`, `Delete`, `Write` |
| `PERMISSIONS` | `[PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete]` |
| `RELATION_ONE_TO_ONE`, `RELATION_ONE_TO_MANY`, `RELATION_MANY_TO_ONE`, `RELATION_MANY_TO_MANY` | `Utopia\Database\RelationType::OneToOne`, `OneToMany`, `ManyToOne`, `ManyToMany` |
| `RELATION_MUTATE_CASCADE`, `RELATION_MUTATE_RESTRICT`, `RELATION_MUTATE_SET_NULL` | `Utopia\Query\Schema\ForeignKeyAction::Cascade`, `Restrict`, `SetNull` |
| `RELATION_SIDE_PARENT`, `RELATION_SIDE_CHILD` | `Utopia\Database\RelationSide::Parent`, `Child` |
| `CURSOR_AFTER`, `CURSOR_BEFORE` | `Utopia\Query\CursorDirection::After`, `Before` |
| `EVENT_*` (all 33) | `Utopia\Database\Event` cases: `EVENT_DOCUMENT_CREATE` is `Event::DocumentCreate`, `EVENT_ALL` is `Event::All`, and so on. The values are unchanged (`Event::DocumentCreate->value === 'document_create'`) |
| `COLLECTION` (protected) | `Database::collectionDefinition()` |

### `Query`

- The 48 `TYPE_*` constants are replaced by `Utopia\Query\Method` cases with the same values. Most names map
  directly (`TYPE_EQUAL` is `Method::Equal`, `TYPE_CURSOR_AFTER` is `Method::CursorAfter`); these four do not:
  `TYPE_GREATER` is `Method::GreaterThan`, `TYPE_GREATER_EQUAL` is `Method::GreaterThanEqual`, `TYPE_LESSER` is
  `Method::LessThan` and `TYPE_LESSER_EQUAL` is `Method::LessThanEqual`. `Query::TYPE_ELEM_MATCH` is kept.
- `Query::TYPES` is removed. `Query::VECTOR_TYPES` is replaced by `Method::isVector()`; `Method` also has
  `isFilter()`, `isSpatial()`, `isNested()`, `isAggregate()` and `isJoin()`.
- `Query::LOGICAL_TYPES` is public and holds `Method` cases.

### `Operator`

- The `TYPE_*` constants are replaced by `Utopia\Database\OperatorType` cases with the same values
  (`TYPE_INCREMENT` is `OperatorType::Increment`, `TYPE_ARRAY_APPEND` is `OperatorType::ArrayAppend`, ...).
- `Operator::TYPES` is replaced by `OperatorType::cases()`. The protected `NUMERIC_TYPES`, `ARRAY_TYPES`,
  `STRING_TYPES`, `BOOLEAN_TYPES` and `DATE_TYPES` are replaced by `OperatorType::isNumeric()`, `isArray()`,
  `isString()`, `isBoolean()` and `isDate()`.

### `Document`

`Document::SET_TYPE_ASSIGN`, `SET_TYPE_APPEND` and `SET_TYPE_PREPEND` are replaced by
`Utopia\Database\SetType::Assign`, `Append` and `Prepend`.

### Other constants

- `Validator\Query\Select::INTERNAL_ATTRIBUTES` (protected) is removed. `Database::internalAttributes()` returns the
  internal attributes as `Attribute` models.
- `Adapter\SQL::VECTOR_DISTANCE_COLUMN` (protected) is replaced by `Utopia\Database\Storage::DISTANCE`.

### Arguments that take enum cases

These methods take or return enum cases where 7.x used the constants' strings:

| Method | Argument or return value |
|---|---|
| `Database::find()`, `iterate()`, `foreach()` | `PermissionType $forPermission = PermissionType::Read` |
| `Database::getQueryCacheField()` | `PermissionType $forPermission = PermissionType::Read` |
| `Database::setTimeout()`, `clearTimeout()` | `Event $event = Event::All` |
| `Database::updateRelationship()` | `?ForeignKeyAction $onDelete` |
| `Database::updateAttribute()` | `ColumnType\|string\|null $type` |
| `Document::setAttribute()` | `SetType $type = SetType::Assign` |
| `Document::getPermissionsByType()` | `PermissionType $type` |
| `Query::getMethod()` | returns a `Method` case (see [Queries](#queries)) |
| `Operator::__construct()`, `setMethod()` | `OperatorType $method` |
| `Operator::getMethod()` | returns an `OperatorType` case |

## Queries

- `Utopia\Database\Query` now extends `Utopia\Query\Query`. Every 7.x method still exists, and the query wire format
  (`Query::parse()`, JSON) is unchanged.
- `Query::getMethod()` returns a `Utopia\Query\Method` case instead of a string, and `Operator::getMethod()` returns
  an `OperatorType` case. Compare with cases: `$query->getMethod() === Method::Equal`. A comparison with a string is
  always `false` and raises no error (see [Before you start](#before-you-start)). `setMethod()`, `isMethod()` and the
  constructor accept a `Method` case or its string value.
- `Query::groupByType()` returns a `Utopia\Query\Builder\ParsedQuery` object instead of an array, and it no longer
  carries the order attributes and order types. `Query::groupForDatabase()` returns the 7.x array shape (`filters`,
  `selections`, `limit`, `offset`, `orderAttributes`, `orderTypes`, `cursor`, `cursorDirection`), plus the new
  `aggregations`, `groupBy`, `having`, `joins` and `distinct` keys. Its `orderTypes` are `OrderDirection` cases, and
  its `cursorDirection` is a `CursorDirection` case.
- `Query::cursorAfter()` and `Query::cursorBefore()` accept `mixed` instead of `Document`. Pass the cursor document,
  as before.
- `Query::parse()`, `parseQuery()` and `parseQueries()` take a trailing `bool $allowRaw = false`. Raw queries are
  refused unless it is `true`, so leave it `false` for input you do not control.
- `Query::orderAsc()` and `Query::orderDesc()` take an optional `?Utopia\Query\NullsPosition $nulls`.
- The factory methods return `static` instead of `Query`.
- `Query::contains()` is deprecated, and every call raises `E_USER_DEPRECATED`. Use `containsString()` for substring
  matching on string attributes and `containsAny()` for array attributes. Queries whose method is `contains` (for
  example parsed from JSON) keep working.
- `Query::DEFAULT_ALIAS`, the alias of the queried collection in generated SQL, is now `table_main` (it was
  `main`). Raw SQL fragments or selections that named `main.<column>` have to use `Query::DEFAULT_ALIAS`. A join may
  not use it as its alias, in any letter case.
- `notContains` on an array attribute excludes documents whose array is NULL (or missing, on MongoDB) on every
  adapter. SQLite and MongoDB used to include them. To match those documents as well, combine it with
  `Query::isNull()` in `Query::or()`.
- On SQLite, `contains`, `containsAny`, `containsAll` and `notContains` on array attributes compare elements by value
  (strings, integers, doubles, booleans), and the adapter reports `Capability::QueryContains`.
- On MongoDB, `startsWith()` and `endsWith()` are anchored: `startsWith('foo')` no longer returns `barfoo`, and
  `endsWith('foo')` no longer returns `foobar`. Both remain case-sensitive on MongoDB (on MariaDB and MySQL they
  follow the column collation). A caller that relied on the substring behaviour should use `containsString()`.
- `orderRandom()` is validated against `Capability::OrderRandom`. On MongoDB, which does not support it, `find()`
  throws `Utopia\Database\Exception\Query` (`Random order is not supported by this adapter`) instead of a generic
  `Exception`.
- `distinct()` needs `Capability::Aggregations`. On Memory, Redis and MongoDB, `find()` refuses it with
  `Utopia\Database\Exception\Query` (`Distinct queries are not supported by this adapter`) also when validation is
  skipped; MongoDB returned duplicate rows then.

## Schema: typed models

The schema methods take model objects instead of long lists of scalar arguments. The models are
`Utopia\Database\Collection`, `Attribute`, `Index` and `Relationship`, and all four extend `Document`.

```php
// 7.x
$database->createCollection('movies', $attributes, $indexes, [Permission::read(Role::any())], true);
$database->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true);
$database->createIndex('movies', 'idx_year', Database::INDEX_KEY, ['year'], [], [Database::ORDER_DESC]);
$database->createRelationship('movies', 'reviews', Database::RELATION_ONE_TO_MANY, true, 'reviews', 'movie', Database::RELATION_MUTATE_CASCADE);

// 8.0
$database->createCollection(new Collection(
    id: 'movies',
    attributes: $attributes,
    indexes: $indexes,
    permissions: [Permission::read(Role::any())],
    documentSecurity: true,
));
$database->createAttribute('movies', Attribute::integer('year', required: true));
$database->createIndex('movies', Index::key('idx_year', ['year'], orders: [Order::Desc]));
$database->createRelationship(new Relationship(
    collection: 'movies',
    relatedCollection: 'reviews',
    type: RelationType::OneToMany,
    twoWay: true,
    key: 'reviews',
    twoWayKey: 'movie',
    onDelete: ForeignKeyAction::Cascade,
));
```

| 7.x | 8.0 |
|---|---|
| `createCollection(string $id, array $attributes = [], array $indexes = [], ?array $permissions = null, bool $documentSecurity = true): Document` | `createCollection(Collection $collection): Collection`. `Collection` accepts `Attribute` and `Index` models, `Document`s or arrays |
| `getCollection(string $id): Document` | `getCollection(string $id): Collection` |
| `createAttribute(string $collection, string $id, string $type, int $size, bool $required, mixed $default = null, bool $signed = true, bool $array = false, ?string $format = null, array $formatOptions = [], array $filters = [])` | `createAttribute(string $collection, Attribute $attribute)`. Build the model with `new Attribute(key: ..., type: ColumnType::...)` or a factory: `Attribute::string()`, `varchar()`, `text()`, `mediumText()`, `longText()`, `integer()`, `bigInteger()`, `float()`, `double()`, `boolean()`, `datetime()`, `point()`, `linestring()`, `polygon()`, `vector()`, `id()`, `object()` |
| `updateAttribute(..., ?string $type = null, ...)` | `updateAttribute(..., ColumnType\|string\|null $type = null, ...)`. The other arguments are unchanged |
| `createIndex(string $collection, string $id, string $type, array $attributes, array $lengths = [], array $orders = [], int $ttl = 1)` | `createIndex(string $collection, Index $index)`. Build the model with `new Index(key: ..., type: IndexType::...)` or a factory: `Index::key()`, `unique()`, `fullText()`, `spatial()`, `object()`, `trigram()`, `ttl()`, `hnswEuclidean()`, `hnswCosine()`, `hnswDot()`. Orders are `Order` cases or `null`; a string order throws `InvalidArgumentException` |
| `createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, ?string $id = null, ?string $twoWayKey = null, string $onDelete = Database::RELATION_MUTATE_RESTRICT)` | `createRelationship(Relationship $relationship)`. The `$id` argument is the model's `key`. `Relationship::oneToOne()`, `oneToMany()`, `manyToOne()` and `manyToMany()` build one per type |
| `updateRelationship(..., ?string $onDelete = null)` | `updateRelationship(..., ?ForeignKeyAction $onDelete = null)`: pass the case, not its `->value` |
| `checkAttribute(Document $collection, Document $attribute)` | `checkAttribute(Document $collection, Attribute $attribute)` |
| `updateAttributeMeta(): Document`, `updateIndexMeta(): Document` (protected) | Return `Attribute` and `Index` |

To turn stored 7.x metadata into models, use `Attribute::fromDocument()`, `Index::fromDocument()`,
`Relationship::fromDocument()` and `Collection::fromArray()`. `Attribute::normalizeType()` turns a stored type
string into a `ColumnType` case.

### Attribute types

- `Database::VAR_BIGINT` is replaced by `ColumnType::BigInteger`, whose value is `'biginteger'`. The type stored in
  collection metadata is still `'bigint'`, exactly as in 7.x: existing rows need no migration and new bigint
  attributes are written as `'bigint'` too. Do not compare or write stored type strings against
  `ColumnType::BigInteger->value`. Use the typed model (`$attribute->type === ColumnType::BigInteger`), normalise a
  raw string with `Attribute::normalizeType($type)` or `Attribute::tryNormalizeType($type)` (both accept `'bigint'`
  and `'biginteger'`), and write a stored type with `Attribute::persistedType($type)` (it returns `'bigint'` for
  `ColumnType::BigInteger` and the enum value for every other type). Error messages that name a type use the stored
  spelling too: a bigint default mismatch reads `Default value … does not match given type bigint`, as in 7.x.
- `Attribute::toDocument()`, `getAttribute('type')` on an `Attribute` model, the `attribute_create`,
  `attributes_create` and `attribute_update` event payloads and the documents returned by `updateAttribute*()` report
  bigint attributes as `'bigint'`.
- `Database::ATTRIBUTE_FILTER_TYPES` is renamed `Database::ATTRIBUTE_FILTER_COLUMN_TYPES` and holds `ColumnType`
  cases instead of type strings. Compare with the typed model
  (`in_array($attribute->type, Database::ATTRIBUTE_FILTER_COLUMN_TYPES, true)`), or normalise a stored string first
  with `Attribute::normalizeType()`.
- The column types an attribute can use are listed in `Attribute::TYPES`. `Attribute::availableTypes(objects:,
  spatial:, vectors:)` narrows the list to what an adapter supports.
- An empty `format` means no format. `Attribute` models store and report `null` for `format: ''`, including
  attributes read from metadata written by 7.x, which stored `''`. Compare a stored attribute's format with `null`.
  `$attribute->format` and `toDocument()` never return `''`.

### Collections and attributes

- `createCollection()` validates attribute types like `createAttribute()`: an unknown type throws
  `Utopia\Database\Exception` (`Unknown attribute type: <type>. Must be one of ...`) before anything is created, and
  object, spatial and vector attributes need the adapter to support them. In 7.x the SQL adapters failed with
  `Unknown type: <type>` and MongoDB created the collection.
- `createCollection()`, `createAttribute()` and `createAttributes()` no longer modify the `Attribute` and `Index`
  objects passed to them. Two adjustments are applied to copies and appear only in the stored metadata: the filters
  added for `datetime`, `object`, spatial and vector attributes, and the index `lengths` and `orders` adjusted for
  the adapter. Read them back with `getCollection()`. In 7.x `createCollection()` wrote these changes into the
  documents it was given. It is safe to pass shared definitions, such as the objects in a config array, directly.
- `updateAttribute()` now updates `id` attributes (7.x threw `Unknown attribute type: id`), and it refuses
  relationship attributes with `Cannot update relationship as an attribute`: use `updateRelationship()`.
- The default of a point, linestring or polygon attribute is validated like a value of that attribute: a point
  needs two numeric coordinates in range, a linestring at least two points, a polygon closed rings of at least four
  points. 7.x accepted any array.
- `createAttribute()` and `createAttributes()` refuse a varchar of size 0 or above the maximum varchar length, as
  `createCollection()` does and as 7.x did.
- **Index definitions need a known type.** `Validator\Index::isValid()` fails for an index document without
  `type`, with a type outside `IndexType`, or for a TTL index without `ttl`, as 7.x did. `Index::fromDocument()` no
  longer throws for an unknown stored type (it reads it as `key`), so stored metadata keeps parsing during query
  validation.
- **Index length accounting.** Big integer columns (big integer, id, and integer of size 8 or more) count 8 bytes
  toward the maximum index length, and a `text()`, `mediumText()` or `longText()` attribute with size 0 counts as the
  engine's maximum for its type. An index that relied on the old undercount has to give the text column a prefix
  length.
- **Existing indexes.** On adapters with schema index introspection, `createIndex()` compares an index that exists in
  the schema but not in the metadata with the request (columns, prefix lengths, key, unique, fulltext or spatial): a
  match is adopted and a mismatch is dropped and recreated, as in 7.3.12.
- `analyzeCollection()` refreshes planner statistics on PostgreSQL and SQLite (the collection's table and its
  permissions table) and returns `true`; it returned `false` there. Call it after bulk loads (migrations, imports):
  a freshly loaded collection otherwise plans joins with empty-table statistics, and SQLite never gathers
  statistics on its own.
- On PostgreSQL `getSizeOfCollection()` reports the table's relation size, which a delete does not reduce until the
  table is vacuumed; `analyzeCollection()` refreshes statistics only.

## Lifecycle events are hooks

`Database::on()`, `Database::before()`, `Mirror::on()` and `Adapter::before()` are removed, and so are the
`Database::EVENT_*` constants. Everything is registered with `Database::addHook()`, which dispatches on the hook's
type:

| Hook interface | Receives | Replaces |
|---|---|---|
| `Utopia\Database\Hook\Lifecycle` | every event, for side effects | `on()` |
| `Utopia\Database\Hook\Decorator` | each document a read or write returns, to modify it | nothing in 7.x |
| `Utopia\Database\Hook\Transform` | each SQL statement before it runs | `before()` |
| `Utopia\Database\Hook\Write` | row writes, such as `Hook\Permissions` | built into 7.x |
| `Utopia\Database\Hook\Relationships` | relationship resolution and mutation | built into 7.x |

### Lifecycle hooks (replaces `on()` and the `EVENT_*` constants)

A listener is now an object implementing `Utopia\Database\Hook\Lifecycle` and registered with
`Database::addHook()`. It receives every `Utopia\Database\Event`, so filter by event inside
`handle(Event $event, mixed $data)`. The event string values are unchanged
(`Event::DocumentCreate->value === 'document_create'`). To keep 7.x's by-name behaviour, also implement
`Utopia\Database\Hook\Named` (`getName(): string`):

- Registering a named hook replaces the lifecycle hook already registered under that name, in its position.
  Re-registering on every request or job no longer stacks listeners. Hooks without a name are appended every time
  they are added.
- A name is unique per `Database`, across all events. 7.x kept one listener per event and name, so
  `on(EVENT_DOCUMENT_CREATE, 'usage', ...)` and `on(EVENT_DOCUMENT_DELETE, 'usage', ...)` were two listeners. In 8.0
  several `Named` hooks with the same name keep only the one registered last: handle all of a name's events in one
  hook, or give each hook its own name.
- `silent()` can silence a named hook on its own (see below).

```php
// 7.x
$database->on(Database::EVENT_DOCUMENT_CREATE, 'calculate-usage', $listener);

// 8.0
final class Usage implements Lifecycle, Named
{
    public function getName(): string
    {
        return 'calculate-usage';
    }

    public function handle(Event $event, mixed $data): void
    {
        if ($event !== Event::DocumentCreate) {
            return;
        }
        // ...
    }
}

$database->addHook(new Usage());
```

On a `Mirror`, lifecycle hooks are registered on the source database. `Mirror::silent()` silences both the source
and the mirror.

### `silent()`

`silent(callable $callback, ?array $listeners = null)`:

- `null` silences every lifecycle hook, and every decorator, for the duration of the callback. This is unchanged.
- A list of names silences only the lifecycle hooks that implement `Named` with one of those names. Unnamed hooks and
  hooks with other names keep firing, and so do decorators. The list holds hook names, not event names:
  `silent($fn, ['document_create'])` silences a hook named `document_create`, not the event.
- Difference from 7.x when nesting: 7.x replaced the silenced set with the inner call's list, so
  `silent(fn () => $db->silent($fn, ['a']))` re-enabled every other listener inside. In 8.0, an inner `silent()`
  never narrows the silence around it.
- Silences apply to the calling coroutine and the coroutines it starts, not to other coroutines sharing the handle
  (see [Coroutines](#coroutines)).

### Hook failures

Whether a lifecycle hook's exception reaches the caller depends on the event, as in 7.x:

| Events | A hook throws an `\Exception` |
|---|---|
| `index_create`, `document_read`, `document_create`, `documents_create`, `document_update`, `documents_update`, `documents_upsert`, `document_increase`, `document_decrease`, `document_delete`, `documents_delete`, `document_find`, `document_count`, `document_sum`, and `document_purge` fired by a document write or `purgeCachedDocument()` | The exception reaches the caller; the remaining hooks do not run. |
| All other events: `database_*`, `collection_*`, `attribute_create`, `attributes_create`, `attribute_update`, `attribute_delete`, `index_rename`, `index_delete`, and `document_purge` fired by an attribute schema change | The exception is swallowed; the remaining hooks still run. |

An `\Error` (for example a `TypeError`) always reaches the caller. Differences from 7.x:

- 7.x also swallowed an `\Error` at the isolated events.
- At an isolated event, 7.x skipped the remaining listeners after the first failure. 8.0 runs them.
- `document_purge` from a document write now fires after the outermost transaction has committed, including for a
  write inside `withTransaction()`, and not at all when that transaction rolls back. In 7.x `updateDocument()` and
  `deleteDocument()` fired it inside the transaction, so a failing purge listener made the transaction retry and
  then roll back. Now the write stays committed and the call throws; if several documents were written, every
  `document_purge` is still delivered before the first listener exception reaches the caller.

A `Decorator`'s exception always reaches the caller.

### `document_purge`

It fires once per written document from `updateDocument()` (for both the old and the new `$id` when the id changes),
`updateDocuments()`, `upsertDocuments()`, `increaseDocumentAttribute()`, `decreaseDocumentAttribute()`,
`deleteDocument()` and `deleteDocuments()`, and from `purgeCachedDocument()`. The payload is
`Document(['$id' => $id, '$collection' => $collectionId])`. As in 7.x, `createDocument()` and `createDocuments()` do
not fire it. Attribute schema changes fire it for the collection's metadata document (`$collection` = `_metadata`).

A write fires it after the outermost transaction commits: inside `withTransaction()` the events of every write wait
for the outer commit, a rollback drops them, and a retried attempt announces once. Each event runs under the tenant
and the `silent()` scope in force when its document was written. If the cache invalidation after the commit fails,
the write throws with its data committed, after `document_purge` has fired. `purgeCachedDocument()` fires it at once.
On an adapter without savepoints (MongoDB), a nested `withTransaction()` that fails is not rolled back on its own:
when the caller catches the failure, the nested writes commit with the caller and their events fire after that
commit.

### `document_update` for related documents a delete changed

As in 7.4.0, `deleteDocument()` fires `document_update` (`Event::DocumentUpdate`) for each document on the other side
of a two-way relationship that the delete changed, after its own `document_delete`. A lifecycle hook that handles
`document_update` receives them with no change. The payload is the related `Document`:

- A document the delete wrote (set-null clearing its key) arrives as that write returned it, with the cleared key set
  to `null`. A document it did not write (the parent of a deleted one-to-many child, a many-to-many peer, a peer under
  `Restrict`) arrives as read off the deleted document, without the back-reference key. Read it back to use more
  than `$id` and `$collection`.
- One-way peers, documents a cascade removed anywhere down its chain, and the deleted document itself are not
  reported. `deleteDocuments()`, a cascade's own deletes and a delete inside `silent()` report nothing.
- They are read and written with permissions skipped and are not checked against the caller's read permission:
  treat them as privileged, like the documents `deleteDocuments()` and `upsertDocuments()` pass to `$onNext`.
- They fire when the delete's own transaction returns, like `document_delete`, not when an outer transaction
  commits. A delete whose commit fails and is retried reports only what the committed attempt changed.
- Difference from 7.4.0: when a hook throws, `document_delete` and every related `document_update` still fire, and
  the first exception reaches the caller afterwards. 7.4.0 stopped at the first failure.

### `attribute_create` from `createAttributes()`

`createAttributes()` fires `attribute_create` once per attribute, with that attribute's `Document` as payload (the
same shape `createAttribute()` sends). It then fires `attributes_create` once, with the list. In 7.x,
`createAttributes()` fired `attribute_create` once, with the array of attribute documents as payload, and nothing
fired `attributes_create`.

### `Event\DispatcherHook`

`Event\DispatcherHook` turns lifecycle events into domain event objects for listeners registered with
`on(string $eventClass, callable $listener)` and for an optional PSR-14 dispatcher:

| Event | Domain event |
|---|---|
| `document_create`, `document_update`, `document_delete` | `Event\Document\Created`, `Updated` (with the document), `Deleted` (with the id) |
| `documents_create`, `documents_update`, `documents_delete` | `Event\Documents\Created`, `Updated`, `Deleted`, with the collection and `count`, the number of documents the call wrote |
| `collection_create`, `collection_delete` | `Event\Collection\Created` (with the collection document), `Deleted` |

A bulk write never delivers a single-document event. `Event\Document\Updated` has no `$previous` property. Every
listener and the PSR-14 dispatcher run; the first `\Exception` among them is then rethrown, and whether it reaches the
caller follows the table under [Hook failures](#hook-failures). An `\Error` always reaches the caller at once.

### Query transforms (replaces `before()`)

A `before($event, $name, $callback)` transformation becomes a `Utopia\Database\Hook\Transform`. Its
`transform(Event $event, string $query): string` receives every statement with the event that runs it, so filter by
event inside it. It is registered under its class name, and `removeTransform(MyTransform::class)` removes it.

```php
// 7.x
$database->before(Database::EVENT_DOCUMENT_FIND, 'label', fn (string $sql) => '/* listing */ '.$sql);

// 8.0
final class Label implements Transform
{
    public function transform(Event $event, string $query): string
    {
        return $event === Event::DocumentFind ? '/* listing */ '.$query : $query;
    }
}

$database->addHook(new Label());
$database->removeTransform(Label::class);
```

### Subclasses of `Database`

- The protected `trigger(string $event, mixed $args = null)` is now `trigger(Event $event, mixed $data = null)`.
- `increaseDocumentAttribute()` and `decreaseDocumentAttribute()` accept numeric strings as well
  (`string|int|float`), for unsigned 64-bit values, so an override has to widen its parameter types.
- `Database` is now composed of the traits in `Utopia\Database\Traits`. Its public methods are unchanged by that.
- The protected `$listeners` and `$silentListeners` properties are gone. Registered lifecycle hooks are in the
  protected `$lifecycleHooks`; to silence or test for silence, use `silent()` and the protected
  `areEventsSilenced()`.

## Relationships

- A retried delete no longer skips its cascade. In 7.x, a cascade that threw (for example on a `Restricted` related
  document or a permission failure) left the relationship on the database's internal delete stack, and a later delete
  of the same document on the same `Database` instance then deleted it without cascading. The cascade now always
  runs.
- Cascade, set-null and link permission failures throw `Utopia\Database\Exception\Authorization` and roll back the
  write, as in 7.x. Related documents are now read and linked in chunks of
  `min(Database::RELATION_QUERY_CHUNK_SIZE, getMaxQueryValues())`; a cascade deletes them one at a time through
  `deleteDocument()`, so a related document the caller may not read is deleted with the rest or, under `Restrict`,
  blocks the delete.
- Linking an existing related document, at any nesting depth of a create or an update, needs update permission on
  that document, for every relationship type; without it the write throws `Utopia\Database\Exception\Authorization`
  and nothing is written. In 7.x a many-to-many link needed only read permission on the related document, because
  the link is a row in the junction collection rather than a column on the related document; it now needs update
  permission too, whether the value is an id or a document. A related document the same write creates needs no
  update permission. Relinking a document that is already linked needs only read permission, and removing a link
  needs no update permission on the unlinked document.
- `deleteDocuments()` applies `Cascade` and `Restrict` to every related document, also when its queries include a
  select (7.x skipped the cascade and ignored `Restrict` then) and also to related documents the caller cannot read.
  Each document a cascade reaches is deleted through `deleteDocument()`, so one the caller may not delete throws
  `Utopia\Database\Exception\Authorization` and rolls the batch back.

## Documents

- `new Document([...])` and `Document::setAttribute('$permissions', ...)` throw `Utopia\Database\Exception\Structure`
  (`Every permission must be of type string`) when a permission is not a string. In 7.x the entry was kept, and
  `createDocument()`/`updateDocument()` rejected it with the same message as a `Utopia\Database\Exception`, but only
  while validation was enabled. `setAttribute('$permissions', $value)` also throws
  (`$permissions must be of type array`) when `$value` is neither an array nor `null`, matching the constructor.
  Permissions set through the constructor or `setAttribute()` are de-duplicated and re-indexed, and
  `getPermissions()` always returns a list.
- Reading stored data never throws for a non-string permission: `Document::fromRow()`, which the SQL adapters use,
  and the new `Document::fromStorage()`, which the MongoDB, Memory and Redis adapters use, drop such entries at
  every nesting level. The same holds for a `json` attribute whose value is shaped like a document (7.x stored such
  values with non-string permissions), for documents rebuilt from the document and query caches, and for a mapped
  document type (`setDocumentType()`), which keeps its class. Otherwise `fromStorage()` builds a document like the
  constructor: nested arrays carrying `$id` or `$collection` become `Document`s, and a non-string `$id` or a
  `$permissions` value that is not an array still throws. Build caller input with the constructor, which rejects
  non-string permissions.
- `updateDocument()` validates only the values it changes: an attribute whose value equals the stored one is not
  validated again, so a value an older release accepted (for example a list in an `object` attribute) no longer
  blocks updates of other attributes. Writing such a value still fails. Collection definitions are always validated
  in full.
- `Document::setAttribute()` takes a `SetType` case, and `Document::getPermissionsByType()` takes a
  `PermissionType` case.
- **`createDocuments()` under `skipDuplicates()`** returns and counts only the documents it inserted, and hands only
  those to `onNext`. A document skipped because its id is already stored is neither counted nor emitted (7.x
  counted and emitted it). Of a batch that repeats an id, only the first copy is written. Permissions are written
  only for inserted documents: in 7.x the skipped copy's permissions were added to the stored document on MariaDB,
  MySQL and SQLite, so `find()`, `count()` and `sum()` could return it to roles its own permissions do not grant.
- **Fractional numbers on integer attributes.** `increaseDocumentAttribute()` and `decreaseDocumentAttribute()`
  throw `Utopia\Database\Exception\Type` before anything is written, on every adapter and also on schemaless
  collections that declare the attribute as an integer:
  - for a fractional change value (`Change value must be an integer.`). 7.x passed it to the engine, which rounded
    it on MariaDB and MySQL, failed on PostgreSQL, and stored a float in the integer attribute on SQLite, MongoDB,
    Memory and Redis. Pass an integer change value, or use a float attribute for fractional counters.
  - for a fractional `max` or `min` (`Max must be an integer.`, `Min must be an integer.`). Integer bounds are
    compared with exact integer arithmetic, so 64-bit and unsigned values never pass through a float. A whole-number
    float such as `102.0` is accepted and converted exactly. Pass a whole bound, for example `floor($max)` or
    `ceil($min)`, which admits the same integer values.

  Change values and bounds on float and double attributes may still be fractional.
- **Operator limits on integer attributes.** The `max` or `min` limit of `Operator::increment()`, `decrement()`,
  `multiply()`, `divide()` and `power()` on an integer or big integer attribute has to be a whole number: an
  integer, an integer string, or a float without a fractional part such as `9.0e18`. A fractional limit such as
  `102.4` is refused before anything is written with `Utopia\Database\Exception\Structure`
  (`Cannot apply <operator> operator: max/min limit must be a whole number for integer attribute '<key>', got
  <limit>`). With validation skipped that check does not run, and Memory and Redis refuse such a limit with
  `Utopia\Database\Exception\Operator` when the result leaves PHP's integer range. Limits on float and double
  attributes are unchanged.
- **Operators on upserts that create a document.** An upsert that creates a document applies every operator to the
  attribute's default, as it does for an existing document: `dateAddDays()` and `dateSubDays()` shift the date,
  `arrayFilter()` filters the array, and the maximum or minimum of increment, decrement, multiply, divide and power
  is honoured. Code that relied on the default being stored unchanged gets the operator's result.
- `Database::cursor($collection, $queries, $batchSize)` reads the matches in batches of `$batchSize`. A `limit()` in
  the queries caps how many documents it yields; an `offset()` or `cursorAfter()` in them positions the first batch
  only; `cursorBefore()` throws `Utopia\Database\Exception` (`Cursor before not supported in this method.`), as
  `iterate()` does.

## Coroutines

Under Swoole, several coroutines can share one `Database` and one `Authorization`. The scopes that 7.x applied to the
whole handle now apply to the calling coroutine and the coroutines it starts; sibling coroutines sharing the handle
or the `Authorization` do not see them:

- `Authorization::skip()`, `Authorization::withStatus()` and `Authorization::withRoles()`;
- `silent()`, `skipRelationships()`, `skipRelationshipsExistCheck()`, `skipFilters()`, `skipValidation()`,
  `withPreserveDates()`, `withPreserveSequence()`, `withTenant()`, `withRequestTimestamp()`, and `skipDuplicates()`
  on the database and on the adapter.

The plain setters (`Authorization::setStatus()`, `enable()`, `disable()`, `reset()`, `addRole()`, `removeRole()` and
`cleanRoles()`, and `setTenant()`, `enableValidation()`, `disableValidation()`, `enableFilters()`,
`disableFilters()`, `setPreserveDates()` and `setPreserveSequence()`) still change the shared value, or, inside such a
scope, the scope's value until it ends.

To run work started in another coroutine under the caller's state, take `$snapshot = $database->snapshot()` in the
caller and run the work inside `$database->withSnapshot($snapshot, $callback)`. A snapshot carries the authorization
status and roles, the relationship, silence and filter state, the tenant, the validation, preserve-dates,
preserve-sequence and skip-duplicates toggles, and the request timestamp. `Hook\Relationships::withEnabled()`,
`withCheckExist()` and `withSnapshot()` scope the hook's own flags the same way.

Relationship population reads its chunks of related ids concurrently only on `Adapter\Pool`, inside a coroutine and
outside a transaction; elsewhere it reads them one after another. Related documents are merged in chunk order.

## Errors

- **Unique index violations.** Every adapter now reports a unique index violation as
  `Utopia\Database\Exception\Unique` with the message `Document with the requested unique attributes already exists`
  (7.x: `Unique index violation`). The class and its hierarchy are unchanged: `Unique` extends `Duplicate`, and a
  conflicting document `$id` still throws a plain `Duplicate` with `Document already exists`. Match on the class,
  not the message: catch `Unique` before `Duplicate` to tell the two apart. `Exception\Unique` has no constructor of
  its own and never rewrites the message it is given. The message is `Exception\Unique::MESSAGE`.
- **`skipDuplicates()` on PostgreSQL** skips only a document whose id is stored, as in 7.x: a new id that collides
  on another unique index throws `Utopia\Database\Exception\Unique`. MariaDB, MySQL and SQLite cannot name the
  index to ignore and, as in 7.x, skip such a row without error.
- **Retries of metadata writes.** Schema calls that persist a collection definition (`createAttribute()`,
  `createIndex()`, their update, rename and delete siblings, `createRelationship()`) no longer retry a failure that
  fails the same way every time: `Authorization`, `Character`, `Duplicate` (and `Unique`), `Limit`, `NotFound`,
  `Order`, `Query`, `Relationship`, `Restricted`, `Structure` and `Type` are thrown on the first attempt. Other
  failures are still attempted up to three times.
- **Failed rollbacks of metadata writes.** When a definition cannot be persisted and the schema change's rollback
  fails too, the thrown `Utopia\Database\Exception` names the persistence error first and the rollback's after
  `| Cleanup error:`, and its `getPrevious()` is always the persistence error. In 7.x the message labelled the two the
  other way round, and some calls reported only the rollback's error.
- **Failures after a schema change committed.** When a definition is stored and only the cache invalidation or
  events after it fail, `createCollection()`, `createAttribute()`, `createAttributes()`, `createIndex()` and their
  update, rename and delete siblings rethrow that failure unchanged and keep the table, column or index.
  `createRelationship()` also completes the relationship's indexes before rethrowing it. Such a failure is not
  retried.
- **Engine errors mapped to library exceptions.**

  | Engine condition | Exception |
  |---|---|
  | MariaDB/MySQL deadlock (1213) or lock wait timeout (1205); PostgreSQL deadlock (40P01), serialization failure (40001) or lock not available (55P03) | `Exception\Contention`, a subclass of `Exception\Transaction`, which `withTransaction()` retries twice before rethrowing |
  | MariaDB/MySQL statement on a missing table (1146) | `Exception\NotFound` (`Collection not found`), as 1051, PostgreSQL 42P01 and SQLite `no such table` |
  | MariaDB/MySQL index on a column the table lacks (1072); SQLite `no such column` | `Exception\NotFound` (`Attribute not found`), as 1054 and PostgreSQL 42703 |
  | PostgreSQL invalid UTF-8 (22021) | `Exception\Character` (`Invalid character`), as MariaDB/MySQL 1366 |
  | PostgreSQL 42P01 naming something other than a collection table (an undeclared or mis-quoted alias) | `Exception\Query` (`Query references an undefined table or alias`); a missing collection table stays `NotFound` |
  | PostgreSQL 42P10 of a `distinct()` read ordered by an unselected attribute | `Exception\Query`, in any server language (`lc_messages`) |
  | PostgreSQL undefined function or operator (42883), for example `max()` over a boolean | `Exception\Query` |
  | `renameAttribute()` or `updateAttribute()` with a new key onto a column that exists beside the old one (every SQL engine) | `Exception\Duplicate` (`Attribute already exists`); PostgreSQL threw a generic `Exception`, and the other engines adopted the column |
  | Shared tables: a column another tenant's collection stores with another type | `Exception\Duplicate` (`Attribute exists in the shared table with another type`); PostgreSQL throws its subclass `Exception\Mismatch` |

- `Database::deleteCollection()` on PostgreSQL succeeds again when the collection's table is already gone (7.x
  behaviour): the metadata is removed and the permissions table is dropped. On MariaDB and MySQL it drops the
  permissions table in that case too, so the collection can be created again.
- **MongoDB `count()`** no longer returns `0` when the query fails: it throws the mapped driver error (a timeout
  still throws `Exception\Timeout`). Code that treated a failed count as zero has to catch the exception.
- **Features an adapter lacks.** On an adapter without timeouts (SQLite, Memory, Redis), `Database::setTimeout()` and
  `clearTimeout()` throw `Utopia\Database\Exception` (`Adapter does not support timeouts`). 7.x's SQLite, which
  inherited them from MariaDB, ignored them. `getConnectionId()` throws `Adapter does not support connection ids`
  on adapters without `Feature\ConnectionId`. `schema()` throws `Schema builder is not supported by this adapter`
  where `from()` throws for the query builder. `getSchemaAttributes()` and `getSchemaIndexes()` return `[]`
  without the feature.
- **Unknown columns on MariaDB and MySQL.** A statement that names a column the table lacks (1054) now throws
  `Utopia\Database\Exception\NotFound` (`Attribute not found`) instead of a raw `PDOException`, as PostgreSQL
  already did. This includes a table that has drifted from its metadata: `find()`, `count()` and `sum()` that
  filter, order or group by the missing column, writes that name it, `updateAttribute()` and `renameAttribute()` on
  it, and raw queries.
- **`Database::sum()`** now validates its attribute (the second argument) the way a `Query::sum()` aggregate is
  validated, and throws `Utopia\Database\Exception\Query` when it does not name a numeric, non-array attribute: of
  the main collection or, under a join alias, of the collection that join reads. In 7.x an unknown attribute reached
  the engine and failed there, and a string or array attribute returned 0 (or failed on PostgreSQL). Sum only
  numeric attributes. With joins, a bare name resolves as in an aggregation query (see
  [Aggregations](#aggregations)), and a joined attribute can always be qualified with its alias
  (`sum('orders', 'item.price', [$join])`).

## Caches

- **Cache key names changed: do not share a cache between 7.x and 8.0 processes.** A document is cached in one hash
  per document, as in 7.x, but the key now includes the database name
  (`{cacheName}-cache-{hostname}:{database}:{namespace}:{tenant}:collection:{collection}:{id}`; 7.x used
  `{cacheName}-cache-{hostname}:{namespace}:{tenant}:collection:{collection}:{id}`), with one field per selection
  whose value records the collection epoch it was filled under. 7.x and 8.0 keys are disjoint, so neither version
  reads or invalidates the other's entries. During a rolling upgrade on one cache this holds in both directions: a
  7.x process keeps serving documents, permissions and collection definitions that an 8.0 process has changed, and
  an 8.0 process keeps serving what a 7.x process has changed (a revoked permission included), until the entry
  expires after the cache TTL (`Database::TTL`, 24 hours). Deploy without overlap, or run one side without a cache
  during the overlap (for example with `Utopia\Cache\Adapter\None`), and flush the cache once the last 7.x process
  has stopped.
- **Invalidation.** A single-document write (`createDocument()`, `updateDocument()`, `increaseDocumentAttribute()`,
  `decreaseDocumentAttribute()`, `deleteDocument()`) and `purgeCachedDocument()` purge only that document, inside the
  transaction and again after the outermost commit or rollback; other cached documents of the collection stay cached.
  Batch writes and schema changes retire the collection's cached documents at once. If the purge after a commit fails,
  the write throws with the data committed, and the collection's cached documents are retired instead.
- **Transactions.** Inside `withTransaction()` a read uses the cache only for documents the transaction has not
  written; it never fills the cache. A transaction started on the adapter directly reads uncached (see
  [Known limitations](#known-limitations)).
- **Read replicas.** With `ReadWritePool`, reads served by a replica are not cached; only reads the pool sends to the
  primary (in a transaction or within the sticky window after a write) fill the document and query caches.
- **Collection definitions carry the document-cache epoch.** A cached collection definition holds, per tenant, the
  epoch its collection's documents are cached under, so a cached `getDocument()` costs two cache round trips (the
  definition and the document) and `getCollection()`, `find()`, `count()` and `sum()` one before their query, as in
  7.x. Batch writes and schema changes purge the collection's definition when they retire its documents, so the
  first read afterwards also reads the definition from the database once.
- Use a cache adapter with generations (`Utopia\Cache\Feature\Leasable`) for the document cache too: without them a
  read that overlaps a write can cache the previous row until the next write or the TTL.
- **Query cache layout.** `find()` results are cached in one hash per collection scope, one field per query and role
  context, whose value records the epoch it was filled under. An invalidation clears the hash, so the number of keys
  no longer grows with writes; on Redis a collection scope keeps one key holding its generation. All of a
  collection's results share one key: under Redis Cluster they live on one slot, and an invalidation rejects every
  in-flight fill of that collection. On a cache without fields (Memory, Filesystem) a collection scope holds one
  result at a time.
- **Abandoned writes.** A write that blocks a collection's cache and never finishes its invalidation (a worker killed
  mid-transaction) no longer keeps that cache off until a flush. For the document cache the limit is
  `$database->setCacheWriterTimeout($seconds)`, for the query cache `new QueryCache($cache, $cacheName,
  writerTimeout: $seconds)`, both 3600 seconds by default. Reads resume once the unfinished write is older than the
  timeout, and the next write re-enables the cache once every other unfinished write is older than the timeout. A
  transaction that runs longer than the timeout is treated as abandoned: raise it above your longest transaction,
  and use the same value in every process (the shortest one applies).
- **Keys left by 8.0 pre-releases.** On Redis, keys matching `*#owner:*`, document entries whose key ends in
  `:<hash>#<epoch>` and query-cache keys matching `*:qcache:*#active:*` are no longer read and can be deleted. On
  Redis a purged document keeps one key holding its generation, with no expiry: the key count grows with the number
  of document ids ever written, not with the number of writes.
- `purgeCachedCollection()` also invalidates the collection's cached `find()` results, and
  `purgeCachedCollection('_metadata')` reads the collection list and purges each cached definition.
- With a query cache installed, a cache error in `find()` logs a warning and reads the database, as `getDocument()`
  does.
- **`purgeCachedQueries()` also purges the `find()` query cache.**
  `Database::purgeCachedQueries($collection, $namespace = null)` rotates the `withCache()` region under
  `getQueryCacheKey()` as in 7.x. When a query cache is installed with `setQueryCache()`, it now also invalidates the
  collection's `find()` results in that namespace. It returns `false` if either purge fails. It does not throw for a
  cache failure: the error is logged as a warning.
- **Filters that belong to one handle.** `Database::addFilter()` still registers a filter for every handle in the
  process. A filter that belongs to one handle goes in the `Database` constructor's `$filters` argument, or on a
  `TypeRegistry` given to that handle through `setTypeRegistry()` (see [Custom types](#custom-types)).

## Adapters

This section matters if you check adapter capabilities, subclass an adapter or write your own.

### Capabilities and feature interfaces

The 51 `getSupportFor*()` methods of `Adapter` are removed. A behaviour flag is now a `Utopia\Database\Capability`
case checked with `$adapter->supports(Capability::X)`. A group of methods an adapter may or may not implement is a
`Utopia\Database\Adapter\Feature\*` interface, checked with `$adapter->hasFeature(Feature\X::class)`. Prefer
`hasFeature()` to `instanceof`: `Adapter\Pool` forwards the optional feature methods to the adapter it borrows
without implementing their interfaces, so only `hasFeature()` answers correctly for a pooled adapter.

| 7.x | 8.0 |
|---|---|
| `getSupportForAlterLocks()` | `supports(Capability::AlterLock)` |
| `getSupportForAttributeResizing()` | `supports(Capability::AttributeResizing)` |
| `getSupportForAttributes()` | `supports(Capability::DefinedAttributes)` |
| `getSupportForBatchCreateAttributes()` | `supports(Capability::BatchCreateAttributes)` |
| `getSupportForBatchOperations()` | `supports(Capability::BatchOperations)` |
| `getSupportForBoundaryInclusiveContains()` | `supports(Capability::BoundaryInclusive)` |
| `getSupportForCacheSkipOnFailure()` | `supports(Capability::CacheSkipOnFailure)` |
| `getSupportForCaching()` | `supports(Capability::Caching)` |
| `getSupportForCastIndexArray()` | `supports(Capability::CastIndexArray)` |
| `getSupportForCasting()` | `supports(Capability::Casting)` |
| `getSupportForDistanceBetweenMultiDimensionGeometryInMeters()` | `supports(Capability::MultiDimensionDistance)` |
| `getSupportForFulltextIndex()` | `supports(Capability::Fulltext)` |
| `getSupportForFulltextWildcardIndex()` | `supports(Capability::FulltextWildcard)` |
| `getSupportForGetConnectionId()` | `hasFeature(Feature\ConnectionId::class)` |
| `getSupportForHostname()` | `supports(Capability::Hostname)` |
| `getSupportForIdenticalIndexes()` | `supports(Capability::IdenticalIndexes)` |
| `getSupportForIndex()` | `supports(Capability::Index)` |
| `getSupportForIndexArray()` | `supports(Capability::IndexArray)` |
| `getSupportForIntegerBooleans()` | `supports(Capability::IntegerBooleans)` |
| `getSupportForInternalCasting()` | `hasFeature(Feature\InternalCasting::class)` |
| `getSupportForJSONOverlaps()` (SQL adapters) | `supports(Capability::JSONOverlaps)` |
| `getSupportForMultipleFulltextIndexes()` | `supports(Capability::MultipleFulltextIndexes)` |
| `getSupportForNestedTransactions()` | `supports(Capability::NestedTransactions)` |
| `getSupportForNumericCasting()` (SQL adapters) | `supports(Capability::NumericCasting)` |
| `getSupportForObject()` | `supports(Capability::Objects)` |
| `getSupportForObjectIndexes()` | `supports(Capability::ObjectIndexes)` |
| `getSupportForOperators()` | `supports(Capability::Operators)` |
| `getSupportForOptionalSpatialAttributeWithExistingRows()` | `supports(Capability::OptionalSpatial)` |
| `getSupportForOrderRandom()` | `supports(Capability::OrderRandom)` |
| `getSupportForPCRERegex()` | `supports(Capability::PCRE)` |
| `getSupportForPOSIXRegex()` | `supports(Capability::POSIX)` |
| `getSupportForQueryContains()` | `supports(Capability::QueryContains)` |
| `getSupportForReconnection()` | `supports(Capability::Reconnection)` |
| `getSupportForRegex()` | `supports(Capability::Regex)` |
| `getSupportForRelationships()` | `hasFeature(Feature\Relationships::class)` |
| `getSupportForSchemaAttributes()` | `hasFeature(Feature\SchemaAttributes::class)` |
| `getSupportForSchemaIndexes()` | `hasFeature(Feature\SchemaIndexes::class)` |
| `getSupportForSchemas()` | `supports(Capability::Schemas)` |
| `getSupportForSpatialAttributes()` | `hasFeature(Feature\Spatial::class)` |
| `getSupportForSpatialAxisOrder()` | `supports(Capability::SpatialAxisOrder)` |
| `getSupportForSpatialIndexNull()` | `supports(Capability::SpatialIndexNull)` |
| `getSupportForSpatialIndexOrder()` | `supports(Capability::SpatialIndexOrder)` |
| `getSupportForTTLIndexes()` | `supports(Capability::TTLIndexes)` |
| `getSupportForTimeouts()` | `hasFeature(Feature\Timeouts::class)` |
| `getSupportForTransactionRetries()` | `supports(Capability::TransactionRetries)` |
| `getSupportForTrigramIndex()` | `supports(Capability::TrigramIndex)` |
| `getSupportForUTCCasting()` | `hasFeature(Feature\UTCCasting::class)` |
| `getSupportForUniqueIndex()` | `supports(Capability::UniqueIndex)` |
| `getSupportForUnsignedBigInt()` | `supports(Capability::UnsignedBigInt)` |
| `getSupportForUpdateLock()` | `supports(Capability::UpdateLock)` |
| `getSupportForUpsertOnUniqueIndex()` | `supports(Capability::UpsertOnUniqueIndex)` |
| `getSupportForUpserts()` | `hasFeature(Feature\Upserts::class)` |
| `getSupportForVectors()` | `supports(Capability::Vectors)` |

`Capability::Upserts`, `Capability::Subqueries`, `Capability::CTEs` and `Capability::WindowFunctions`, which the 8.0
pre-releases had, are removed: check upsert support with `hasFeature(Feature\Upserts::class)`. Every remaining case
is declared by at least one adapter. SQLite and MongoDB now report `Capability::QueryContains`. On Memory and Redis
`setSupportForAttributes(false)` returns `true` and changes nothing: they always enforce the collection's
attributes, and only MongoDB has a schemaless mode.

The methods that went with a feature moved to its interface: `getConnectionId()` (`Feature\ConnectionId`),
`getSchemaAttributes()` and `getSchemaIndexes()` (`Feature\SchemaAttributes`, `Feature\SchemaIndexes`),
`setTimeout()` and `clearTimeout()` (`Feature\Timeouts`), `createRelationship()`, `updateRelationship()` and
`deleteRelationship()` (`Feature\Relationships`), `upsertDocuments()` (`Feature\Upserts`), `getColumnType()`
(`Feature\ColumnTypes`), `decodePoint()`, `decodeLinestring()` and `decodePolygon()` (`Feature\Spatial`),
`castingBefore()` and `castingAfter()` (`Feature\InternalCasting`), and `setUTCDatetime()` (`Feature\UTCCasting`).
An adapter that does not support a feature no longer declares its methods: for example, only MongoDB implements
`Feature\InternalCasting` and `Feature\UTCCasting`, so the SQL, Memory and Redis adapters no longer have
`castingBefore()`, `castingAfter()` or `setUTCDatetime()`. The SQL adapters also implement `Feature\RawQuery`
(`rawQuery()`, `rawMutation()`) and `Feature\QueryBuilder` (`getBuilder()`, `getSchema()`). To list what an adapter
reports, call `$adapter->capabilities()`.

### Writing or subclassing an adapter

- `Adapter` now implements `Feature\Attributes`, `Feature\Collections`, `Feature\Databases`, `Feature\Documents`,
  `Feature\Indexes` and `Feature\Transactions`. Report optional behaviour by overriding `capabilities()`, and
  implement the `Feature` interfaces your adapter supports.
- `Adapter\SQLite` now extends `Adapter\SQL` instead of `Adapter\MariaDB`. A check like
  `$adapter instanceof MariaDB` no longer matches SQLite: check capabilities and features instead. The MariaDB
  methods SQLite inherited in 7.x, such as `getConnectionId()`, `setTimeout()` and `getViolatedKey()`, are no longer
  available on it.
- Changed adapter signatures:

  | 7.x | 8.0 |
  |---|---|
  | `createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false)` | `createAttribute(string $collection, Attribute $attribute)` |
  | `updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false)` | `updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null)` |
  | `createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = [], int $ttl = 1)` | `createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = [])` |
  | `createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = '')` | `createRelationship(Relationship $relationship)` |
  | `updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null)` | `updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null)` |
  | `deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side)` | `deleteRelationship(Relationship $relationship)` |
  | `find(..., string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ)` | `find(..., CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read)`; `$orderTypes` holds `OrderDirection` cases |
  | `setTimeout(int $milliseconds, string $event = Database::EVENT_ALL)`, `clearTimeout(string $event)` | `setTimeout(int $milliseconds, Event $event = Event::All)`, `clearTimeout(Event $event = Event::All)` |
  | `getTimeout(): int` | `getTimeout(Event $event = Event::All): int` |
  | `increaseDocumentAttribute(..., int\|float $value, ..., int\|float\|null $min = null, int\|float\|null $max = null)` | Numbers may also be numeric strings (`string\|int\|float`), for unsigned 64-bit values |
  | `SQL::__construct(mixed $pdo)` | `SQL::__construct(object $pdo)`: a `Utopia\Database\PDO`, a PDO-compatible proxy or a native `PDO` |
  | `SQL::deleteAttribute(string $collection, string $id, bool $array = false)` | `deleteAttribute(string $collection, string $id)` |
  | `SQL::execute(mixed $stmt)` | `execute(mixed $stmt, ?Event $event = null)` |
  | `SQL::getSQLType(string $type, ...)`, `getSQLIndexType(string $type)` | Take `ColumnType` and `IndexType` cases |
  | `SQL::getOperatorSQL(string $column, Operator $operator, array &$binds)` | `getOperatorSQL(string $column, Operator $operator, int &$bindIndex)` |

- `Adapter::relaxAttributeRequired(string $collection, string $id): bool` is new. The library calls it when an
  attribute becomes optional without a column change; it does nothing by default, and PostgreSQL drops the column's
  `NOT NULL` there.
- `SQL::getSpatialColumnSrid(): ?int` (protected) is new. It returns the SRID written into spatial column
  definitions, or `null` for a dialect that cannot declare one (MariaDB). `SQL::getSpatialSQLType()` follows it as
  well, so a dialect returning `null` declares spatial columns without an SRID on `createCollection()`,
  `createAttribute()`, `createAttributes()` and `updateAttribute()` alike.
- `SQL::insertOrIgnore(SQLBuilder $builder): Statement`, `SQL::supportsInsertReturning(): bool` and
  `SQL::documentKeyColumns(): array` (protected) are new. Under `skipDuplicates()` an adapter's `createDocuments()`
  must return only the documents it inserted; the SQL adapters learn them from `RETURNING`, or, where
  `supportsInsertReturning()` is false (MySQL), from reading the ids before and, when rows were skipped, after the
  insert.
- `SQL` declares `abstract protected function getColumnNames(string $collection): array` (the table's physical
  column names, empty when the table is missing). `SQL::renameAttribute()` and the engines' `updateAttribute()` use
  it through `isRenamed()` to complete a rename another tenant of a shared table already ran.
- `SQL::getNullOrder(): OrderDirection` (protected) returns the direction in which the engine sorts null before
  every other value (`OrderDirection::Asc` by default; the PostgreSQL adapter returns `OrderDirection::Desc`). A SQL
  adapter for an engine that sorts nulls last in ascending order overrides it, or a cursor over a joined read skips
  or repeats rows holding null.
- Declare `Capability::NestedTransactions` only when a failed nested transaction rolls back to its savepoint and
  leaves the enclosing transaction open. `Database` drops the `document_purge` events of a failed nested call only
  on such adapters; without it they fire with the enclosing commit.
- `Adapter::withTenant($tenant, $callback)` scopes the tenant to the calling coroutine. `Database::withTenant()` uses
  it and no longer calls `setTenant()`, so an adapter that overrides `setTenant()` to react to tenant changes has to
  key such state by `getTenant()` instead.

### Removed adapter methods

Queries now compile through the utopia-php/query builders (`getBuilder()`, `createBuilder()`), transforms through
`Hook\Transform`, and tenant and permission conditions through hooks in `Utopia\Database\Hook`. The 7.x methods
behind the old string-building path are removed. Nothing in 8.0 calls them, so an adapter subclass that overrides or
calls one must drop the override or the call.

| Removed | Visibility in 7.x | Replacement |
|---|---|---|
| `SQL::getSQLConditions(array $queries, array &$binds, string $separator = 'AND', ?string $forCollection = null)` | public | The adapter's query builder (`getBuilder()` / `createBuilder()`) |
| `SQL::getSQLConditionsForCollection()` | protected | The same |
| `getSQLCondition(Query $query, array &$binds, ?string $forCollection = null)` on `SQL` (abstract), `MariaDB`, `Postgres` and `SQLite` | protected | The same |
| `SQL::getSQLOperator()` | protected | The same |
| `handleSpatialQueries()` on `MariaDB` and `Postgres` | protected | The same |
| `handleDistanceSpatialQueries()` on `MariaDB`, `MySQL` and `Postgres` | protected | The same |
| `Postgres::handleObjectQueries()` | protected | The same |
| `SQLite::getLikeCondition()` | protected | The same. SQLite's `LIKE ... ESCAPE` lives in `Utopia\Database\Builder\SQLite` |
| `getSQLPermissionsCondition()` on `SQL` and `Postgres` | protected | The permission hooks in `Utopia\Database\Hook` (`PermissionFilter` and the join filters) |
| `getSQLVectorDistance()` on `SQL` and `Postgres` | protected | The query builder |
| `Adapter::getTenantQuery()` (abstract) and its implementations on `SQL`, `Memory`, `Redis` and `Pool` (`Mongo` keeps its own) | public | `Hook\TenantFilter` (SQL) and `Hook\Mongo\TenantFilter` (MongoDB) |
| `getInsertKeyword()` on `SQL`, `Postgres` and `SQLite`, and `getInsertSuffix()` and `getInsertPermissionsSuffix()` on `SQL` and `Postgres` | protected | `SQL::insertOrIgnore(SQLBuilder $builder): Statement`, which `Postgres` overrides to name the id as the conflict target |
| `getUpsertStatement()` on `SQL` (abstract), `MariaDB`, `Postgres` and `SQLite` | protected, public on `MariaDB` and `SQLite` | The same |
| `SQL::registerOperatorBind()` | protected | `getOperatorSQL()` binds operator values itself |
| `SQL::getFulltextValue()` and `Postgres::getFulltextValue()` | protected | utopia-php/query's builders normalize search terms (`compileSearchExpr()`) |
| `Adapter::before()` and `Adapter::trigger()` | public, protected | `Hook\Transform`, registered with `Database::addHook()` |
| `SQL::getLikeOperator()`, `SQL::getRegexOperator()`, `Postgres::getLikeOperator()` and `Postgres::getRegexOperator()` | public | The query builders emit `LIKE`/`ILIKE` and `REGEXP`/`~` |
| `Adapter::getAttributeProjection()` (abstract) and its implementations on `SQL`, `Memory`, `Redis` and `Pool` (`Mongo` keeps a private one) | protected | The query builders build the projection |

`escapeWildcards()` is kept, and so are the public helpers `SQL::getSpatialTypeFromWKT()` and
`Query::isSpatialAttribute()`, even where nothing in the library calls them any more.

### SQLite adapter subclasses that override `createBuilder()`

The SQLite adapter builds its queries with `Utopia\Database\Builder\SQLite`, not `Utopia\Query\Builder\SQLite`. SQLite
has no default LIKE escape character, and this builder adds `ESCAPE '\'` so that `startsWith`, `endsWith`,
`containsString`, `containsAny`, `containsAll`, `notContains`, `notStartsWith` and `notEndsWith` match `_`, `%` and
`\` literally, as they did in 7.x. If you subclass `Utopia\Database\Adapter\SQLite` and override `createBuilder()`,
return `Utopia\Database\Builder\SQLite` or a subclass of it. A plain `Utopia\Query\Builder\SQLite` treats `_` and `%`
as wildcards and a backslash as a literal character.

### Connections

- **Session settings and reconnects.** `Utopia\Database\PDO` reconnects on its own when a call outside a
  transaction finds the connection gone, and retries the call on the new connection. A new connection starts a new
  database session, so session state set earlier with `exec()` is gone after a reconnect. Set session state that
  must last through `configure()` instead:

  ```php
  $pdo->configure('time_zone', "SET time_zone = '+00:00'");
  ```

  `configure(string $setting, string $statement)` runs the statement now and again on every connection a later
  reconnect opens, before the retried call runs there. A later statement for the same `$setting` replaces the
  earlier one. Attributes set with `setAttribute()` after connecting are replayed the same way, before the session
  statements. If a statement fails on the new connection, `reconnect()` throws and the wrapper keeps the old
  connection, so the next call reconnects again instead of running on an unconfigured session. The MariaDB and
  MySQL adapters keep `setTimeout()` this way, so a timeout also bounds the statement that triggered the reconnect.
  Behind Swoole's `PDOProxy`, which reconnects without replaying session state, the adapter sets the timeout again
  before its next statement; the statement the proxy itself retries after its reconnect runs without it.
- **Lost-connection detection.** `Connection::hasError()` decides by the driver's error first: MySQL and MariaDB
  errors 1053, 2002, 2006, 2013 and 4031, SQLSTATE class `08`, and PostgreSQL `57P01` to `57P05`. Statement timeouts
  (MariaDB 1969, MySQL 3024, PostgreSQL 57014) are not lost connections and are never retried. The message fallback
  carries Swoole 6.2's `DetectsLostConnections` list, so a PHP without ext-swoole, or with
  `swoole.enable_library=Off`, detects the same lost connections as one with it.
- **Lost transactions.** When the connection loses a transaction that `withTransaction()` calls are nested in, the
  nested call and every enclosing call throw `Utopia\Database\Exception\Transaction` (`Failed to execute
  transaction: the transaction was lost before it could commit`) and are not retried. Nothing written in the lost
  transaction is committed. A nested call that fails while its enclosing transaction holds is still rolled back to
  its savepoint and retried, and a top-level call that failed to begin, or whose work failed, is still retried. A
  top-level commit that finds the connection no longer holds the transaction (a reconnect the callback did not
  surface) throws `Exception\Transaction` too, and the callback is not run again: statements after such a reconnect
  may already have run on their own. Code that catches an expected exception (for example `Duplicate`) from a
  nested call and carries on no longer receives that exception when the transaction was lost underneath it: catch
  `Exception\Transaction` around the outermost call and run the whole unit again. A transaction the engine rolled
  back over a lock conflict is not lost: MariaDB and MySQL roll the whole transaction back, savepoints included, when
  a statement loses a deadlock (1213), or a lock wait timeout (1205) with `innodb_rollback_on_timeout`. The nested
  calls rethrow that `Exception\Contention` unchanged, and the outermost call runs again, as in 7.x, because nothing
  of the attempt is stored.
- **Statements after a lost transaction.** When `Utopia\Database\PDO` reconnects because a statement inside a
  transaction found the connection gone, it rethrows and then refuses every statement (`exec()`, `query()`,
  `prepare()`, `beginTransaction()`, `commit()`) with a `PDOException` until the transaction is ended with
  `rollBack()`, a `ROLLBACK` statement or `reconnect()`. `inTransaction()` reports the transaction until then. Code
  that catches the connection error inside `withTransaction()` and carries on no longer writes on the new connection
  in autocommit: the transaction fails and is rolled back instead.

### MongoDB: rebuild key and unique indexes

8.0 changes the partial filter of the key and unique indexes the MongoDB adapter creates, and fixes two defects that
7.x and the 8.0 pre-releases share:

- Unique indexes: `createIndex()` gave every attribute the filter `{attr: {$exists: true, $type: 'string'}}`,
  whatever its type, so a unique index on an integer, big integer, float, boolean or datetime attribute covered no
  document and accepted duplicates. `createCollection()` required `int` for integers, `long` for big integers and
  `double` for floats, which left out integers past 32 bits, big integers inside 32 bits and floats stored as
  integers. 8.0 requires every type a value of the attribute can be stored as.
- Key indexes: both paths added a `$type` clause, and MongoDB uses a partial index only for queries that imply its
  filter, which a filter on a value never does for `$type`. No query used these indexes, whatever the attribute
  type. 8.0 gives a key index `{first attribute: {$exists: true}}` only, which a filter on a non-null value of that
  attribute implies, so a single or compound key index serves any filter on its first attribute.

Existing indexes keep the filter they were created with. The library does not rebuild them; run this step once per
database after upgrading. It covers both changes, so one rebuild is enough.

1. For every collection, read its `indexes` and `attributes` from the database's metadata collection
   (`Database::getCollection()`), and pick:
   - every index of type `key`, whatever its attributes' types;
   - every index of type `unique` with at least one attribute of type `integer`, `biginteger` (stored as `bigint`),
     `float`, `double`, `boolean` or `datetime`.
2. For a `unique` index, look for duplicates first, because the rebuilt index enforces uniqueness and its creation
   fails (error `11000`, `Exception\Duplicate` or `Exception\Unique`) while duplicates exist. Group the documents that
   hold a value for every attribute of the index by those attributes (and by `_tenant` under shared tables), and list
   the groups with more than one document, for example:

       db.getCollection('<namespace>_<collection>').aggregate([
           { $match: { <field>: { $exists: true, $ne: null } } },
           { $group: { _id: { tenant: '$_tenant', value: '$<field>' }, count: { $sum: 1 }, ids: { $push: '$_uid' } } },
           { $match: { count: { $gt: 1 } } },
       ])

   The stored field name is the attribute key with each `.` written as `__dot__`. Resolve every duplicate (change or
   remove documents) before step 3.
3. Drop and recreate the index with the same key, type, attributes, lengths and orders:
   `Database::deleteIndex($collection, $key)` and then `Database::createIndex($collection, $index)`. Between the two
   calls the collection has no such index: a unique constraint is not enforced and queries do not use it, so run the
   step when the collection takes no writes. A key index needs no duplicate check.

Unique indexes whose attributes are all strings (`string`, `varchar`, `text`, `mediumtext`, `longtext`, `id`,
`uuid7`), fulltext and TTL indexes, and the internal `_uid`, `_createdAt`, `_updatedAt` and `_permissions` indexes
need no rebuild. You can tell a rebuilt index by its `partialFilterExpression` (`db.<collection>.getIndexes()`): a
key index names only its first field, with no `$type`, and a unique index on an integer attribute has
`$type: ['int', 'long']`.

### MongoDB: collections

`Adapter\Mongo::createCollection()` is idempotent: creating a collection that already exists returns `true` instead
of throwing `Exception\Duplicate`, unless the server itself reports the collection as created concurrently (code 48)
outside shared tables. `Database::createCollection()` still throws `Duplicate` for a collection whose metadata
exists.

### SQLite

- **Document ids compare case-insensitively.** `getDocument()`, `equal('$id', ...)`, `notEqual('$id', ...)` and joins
  on `$id` compare `_uid` with `COLLATE NOCASE`, matching the unique index and MariaDB: `getDocument('Doc')` finds
  `doc`. Code that relied on SQLite telling `doc` and `Doc` apart must not: the unique index never allowed both.
- **Shared-table files created by 7.x or the 8.0 pre-releases** keep unique indexes that declare
  `_tenant COLLATE NOCASE`.
  Lookups still use them once `ANALYZE` has run (`analyzeCollection()`); without statistics, joins walk the tenant's
  rows once per joined alias. Recreate them once per collection (uniqueness is unchanged, `_tenant` is an integer;
  `{tenant}` is the tenant segment of the existing index names):

  ```sql
  DROP INDEX `{namespace}_{tenant}_{collection}__index1`;
  CREATE UNIQUE INDEX `{namespace}_{tenant}_{collection}__index1` ON `{namespace}_{collection}` (`_tenant`, `_uid` COLLATE NOCASE);
  DROP INDEX `{namespace}_{tenant}_{collection}_perms__index_1`;
  CREATE UNIQUE INDEX `{namespace}_{tenant}_{collection}_perms__index_1` ON `{namespace}_{collection}_perms` (`_tenant`, `_document` COLLATE NOCASE, `_type` COLLATE NOCASE, `_permission` COLLATE NOCASE);
  ```

  Plain-table files need nothing.
- **Regex.** `Query::regex()` works on SQLite through the adapter's `REGEXP` function; patterns are PCRE
  (`preg_match`, case-sensitive, `u` flag), and `supports(Capability::Regex)` is true whenever that function is
  registered (`Utopia\Database\PDO` or `Pdo\Sqlite` connections).
- **`getSchemaIndexes()` returns index ids.** Each entry's `$id` and `indexName` are the index id (`email`,
  `_index1`), not the SQLite object name (`{namespace}_{tenant}_{collection}_email`) or the FTS5 table
  (`…_{hash}_fts`). To read the physical names, query `sqlite_master`. Under shared tables the list includes indexes
  other tenants created on the shared table, since they cover every tenant's rows.

### Shared tables

All tenants' collections with the same id share one physical table. Several tenants declaring the same collection id
with the same attributes is the normal case.

- **Conflicting attributes and indexes are refused.** `createAttribute()` and `createAttributes()` over a column
  another tenant created reuse it when the column type matches, and throw `Utopia\Database\Exception\Duplicate`
  (`Attribute exists in the shared table with another type`) when it does not (on PostgreSQL its subclass
  `Exception\Mismatch`); a refused batch creates none of its columns. Before, the column was dropped with the other
  tenant's values. `createIndex()` over an index another tenant uses throws `Duplicate` (`Index exists in the shared
  table with another definition`) when the definition differs. Outside shared tables an orphaned column or index
  (left by a partial failure) is still dropped and recreated. While migrating shared tables (`setMigrating(true)`)
  neither check runs, as before.
- **Renames.** A rename is physical: the first tenant to rename an attribute of a shared collection id renames the
  column for every tenant, and tenants whose metadata still has the old key read it as null until their own rename
  runs. Run the rename for every tenant (as project-by-project migrations do). A later tenant's rename
  (`renameAttribute()`, or `updateAttribute()` with a new key) completes without DDL when the old column is gone and
  the new one exists; otherwise the engine decides (the old column missing is `NotFound`, a new column beside the old
  one is `Duplicate`).

### Query comments

`Database::setMetadata()` values are written as `/* key: value */` comments at the start of the SQL statements, as
in 7.x, ahead of the registered `Transform` hooks.

- Comments now precede every statement the SQL adapters prepare, including raw queries, `ping()`,
  `getConnectionId()`, schema introspection and SQLite's transaction statements. 7.x annotated only statements that
  went through an event transformation. Statements PDO issues itself (begin, commit, rollback, `lastInsertId()`),
  savepoints and the timeout session statements carry none.
- A `Transform` receives the SQL with the comment block first. Do not anchor patterns on the statement keyword at
  the start of the string.
- `resetMetadata()` takes effect at once. In 7.x the last metadata transformation stayed registered after a reset.
- Scalars and `Stringable` values are rendered as text (`true` as `1`, `false` as an empty string, as in 7.x).
  Arrays, `null` and other objects are rendered as JSON. In 7.x an array printed `Array` with a warning and a
  non-`Stringable` object threw.
- Keys and values are rewritten where needed: `/*` becomes `/ *`, `*/` becomes `* /`, control characters and
  U+2028/U+2029 become spaces, and invalid UTF-8 bytes are replaced with mbstring's substitute character (`?` by
  default).

## Mirror

- **Query cache.** Install the query cache through the mirror: `$mirror->setQueryCache($queryCache)` installs it on
  the source and the destination as well, so writes through the mirror invalidate the cache its reads use. An
  `Invalidator` added with `$mirror->addHook()` is installed on the mirror and the source.
- **Setters reach the wrapped databases.** Besides `setDatabase()`, `setNamespace()`, `setSharedTables()`,
  `setTenant()`, `setMaxQueryValues()`, `setCache()`, `setAuthorization()`, validation and the document-type
  setters, a mirror now forwards `setQueryCache()`, `setCacheName()`, `setGlobalCollections()`,
  `resetGlobalCollections()`, `setTenantPerDocument()`, `setCacheWriterTimeout()`, `setTimeout()`, `clearTimeout()`,
  `setMetadata()`, `resetMetadata()`, `enableFilters()`, `disableFilters()`, `skipFilters()`, `enableLocks()`,
  `enableProfiling()`, `disableProfiling()`, `setMigrating()` and `setTypeRegistry()` to its source and destination.
  In 7.x these changed the mirror alone.
- **Timeouts on the destination.** A destination that cannot apply a timeout (MariaDB applies it on the connection)
  is reported through `onError()` with the action `setTimeout` or `clearTimeout`; the call itself succeeds when the
  source applied it. `enableLocks()` reports a destination failure the same way, with the action `enableLocks`.
- **`create()`.** A destination that cannot create the database makes `create()` throw, after the source has
  created it, so a mirror never reports a database its destination lacks. Every other call forwarded to the
  destination reports a destination failure through `onError()` and returns the source's result.
- **Profiling.** `$mirror->enableProfiling()` enables profiling on the source and the destination, and
  `$mirror->getProfiler()` returns the source's profiler, which records the mirror's queries, because both use the
  source's adapter. The destination's queries are recorded by `$mirror->getDestination()->getProfiler()`.
- **Scoped setters.** `withTenant()`, `withPreserveDates()`, `withPreserveSequence()`, `skipValidation()` and
  `skipFilters()` called on a mirror open on the mirror, its source and its destination for the duration of the
  callback. `skipRelationships()`, `skipRelationshipsExistCheck()` and `withRequestTimestamp()` open on the mirror
  and its source only; the destination applies the source's resulting documents with preserved dates.
  `withRequestTimestamp()` now runs its callback once; 7.x ran it once per database when a destination was set.
- **Replication.** Inside a coroutine (a Swoole server), `createDocuments()`, `updateDocuments()`, the upserts,
  `deleteDocument()` and `deleteDocuments()` return once the source write is done and replicate in a coroutine of
  their own; a destination failure reaches `onError()` later. `createDocument()`, `updateDocument()`,
  `increaseDocumentAttribute()` and `decreaseDocumentAttribute()` replicate before returning, after the pending
  replications of their document. Outside a coroutine all replication finishes before the call returns. In 7.x every
  write replicated before returning.
  - Each replication runs under the state the caller had at the time of the call: authorization status and roles,
    tenant, relationship and silence state and the toggles (see [Coroutines](#coroutines)), without the request
    timestamp. A write made inside `skip()` replicates under it even after the caller has left the scope.
  - Writes to one document reach the destination in the order they were made through the mirror; a failed
    replication is reported to `onError()` and does not hold back later ones.
  - A schema change through a mirror waits for the queued replications of the collections it touches before it
    reaches the destination.
- **Write filters.** A `null` return from `beforeCreateCollection()`, `beforeUpdateCollection()`,
  `beforeCreateAttribute()`, `beforeUpdateAttribute()` or `beforeCreateIndex()` skips that change on the destination,
  and a collection whose creation was skipped is not replicated. An exception from a filter hook is reported to
  `onError()` under the write's action and skips that replication; the source change stands.
- **Decorators.** Decorator hooks added through a mirror stay on the mirror: they decorate what its reads and writes
  return (including the documents bulk writes hand `onNext`), and the destination receives undecorated documents.
  `createDocument()` returns the document written to the source, as `updateDocument()` does, instead of the
  destination's copy.
- **Upserts.** `upsertDocument()` and `upsertDocumentsWithIncrease()` through a mirror now run on the source and are
  replicated to the destination, like `upsertDocuments()`; in 7.x they were never replicated. Hooks registered
  through the mirror receive `document_purge` for each upserted document and `documents_upsert` once per call. A
  failed replication is reported to `onError()` as `upsertDocuments` for a plain upsert, as in 7.x, and as
  `upsertDocumentsWithIncrease` for an increasing one.

## Validators and helpers

- `Utopia\Database\Validator\Queries\Documents` has new trailing constructor parameters:
  `bool $supportForJoins = false`, `bool $supportForAggregations = false` and `bool $sharedTables = false`. By
  default, a validator you construct yourself accepts only filters, ordering, selection and pagination, as in 7.x.
  To accept `join`, `leftJoin`, `rightJoin`, `crossJoin` and `fullOuterJoin`, pass `supportForJoins: true`. To
  accept aggregate functions, `groupBy`, `having` and `distinct`, pass `supportForAggregations: true`. `Database`
  sets these flags from the adapter's `Capability::Joins`, `Capability::Aggregations` and shared-tables setting.
- `Utopia\Database\Validator\Query\Select`, `Aggregate` and `GroupBy` accept `$tenant` only when constructed with
  `sharedTables: true` (third constructor argument); `Queries\Documents` and `Queries\Document` take the same flag as
  their last argument. A validator built directly now rejects `select('$tenant')` unless given the flag;
  `Database::find()` already rejected it without shared tables in 7.x.
- `Utopia\Database\Validator\Queries\Document` takes optional `idAttributeType`, `maxValuesCount`, `minAllowedDate`,
  `maxAllowedDate`, `supportUnsignedBigInt` and `sharedTables` arguments for the join conditions of
  `getDocument()`, with the same meaning as on `Queries\Documents`. It also takes a trailing
  `bool $supportForJoins = true`: joins stay accepted by default, as `Database::getDocument()` expects, and with
  `false` a join query is refused (`Invalid query method: join`), as `Queries\Documents` refuses it without its flag.
- The new `Utopia\Database\Validator\Query\Join` takes the main collection's attributes (`new Join($attributes)`) to
  check the columns of a join condition. `new Join()` accepts any column of the main collection.
- `Queries\Documents` and `Query\Filter` default `supportUnsignedBigInt` to `true`, as in 7.x.
- The protected `Database::getDocumentsValidator()` is now
  `getDocumentsValidator(Document $collection, array $joinedCollections = [])`. Subclasses that override it must add
  the parameter.
- Changed validator signatures: `Validator\Attribute`'s `check*()` methods take an `Attribute`, `Validator\Index`'s
  take an `Index`, `Validator\Attribute::getRequiredFilters()` and `validateDefaultTypes()` take a `ColumnType`,
  `Structure::addFormat()`, `getFormat()` and `hasFormat()` (and those of `PartialStructure`) take a `ColumnType`,
  and `Query\Filter::isValidAttributeAndValues()` takes a `Method` case. `Validator\Operator` takes a trailing
  `bool $supportUnsignedBigInt = true`.
- `Validator\Permissions` and `Helpers\Permission::aggregate()` take their allowed permission types as
  `PermissionType` cases; a list of strings throws a `TypeError`. `Validator\Authorization\Input` accepts a
  `PermissionType` case or a string.
- `Validator\Authorization`'s status is no longer a `protected bool $status` property; subclasses read and change it
  through `getStatus()`, `setStatus()`, `skip()` and `withStatus()`. `skip()` and `withStatus()` are scoped to the
  calling coroutine and the coroutines it starts; `setStatus()`, `enable()`, `disable()` and `reset()` change the
  shared status unless called inside such a scope (see [Coroutines](#coroutines)).
- `Validator\Structure` takes a trailing `array $storedAttributes = []`: the attributes whose values are the stored
  ones, which it does not validate again. `Database::updateDocument()` passes it.
- `Database::convertQueries()` takes an optional `array $joinedCollections` (join alias => collection). With it,
  filters on `alias.attribute`, the filters of join ON lists and `having()` conditions in the list are converted
  too; aggregates and selects in the list are left as they are. Without it the method converts as before.
- `Utopia\Database\Storage::joinAlias()` returns the alias an undeclared join is read under (`j0`, `j1`, ...).

## Rules for features new in 8.0

These features do not exist in 7.x. Their rules are listed here because they differ from what a reader of the 7.x
API might expect. [CHANGELOG.md](CHANGELOG.md) describes the features themselves.

### Joins

- A joined collection is read exactly as a direct `find()` on it would be. If the caller holds the collection-level
  permission, every row of the joined collection is visible. If not, and the collection has document security, only
  the rows the caller holds document-level read on are visible. If neither applies, the query throws
  `Utopia\Database\Exception\Authorization`. Adding a join never hides main-collection rows that are readable
  through the collection grant, in `find()`, `count()`, `sum()` and `getDocument()`.
- A query can declare at most 8 joins (`Validator\Query\Join::MAX_PER_QUERY`). More is rejected with
  `Utopia\Database\Exception\Query` (`Too many joins: at most 8 are allowed`) by `find()`, `count()`, `sum()` and
  `getDocument()`, also inside `skipValidation()`.
- Join aliases must be identifiers, unique within the query regardless of case, and different from
  `Query::DEFAULT_ALIAS` and from the key of a relationship attribute of the main collection; anything else throws
  `Utopia\Database\Exception\Query`. An alias equal to a relationship key is refused with
  `Join alias "<alias>" is the key of the relationship attribute "<alias>": give the join another alias`, so
  `alias.attribute` and `alias.*` never mean both a joined column and a related document's attribute. Relationship
  keys are matched by exact name.
- A join's ON list (`Query::join($collection, $alias, [...])`) holds `Query::on()` conditions and the plain filters
  `equal`, `notEqual`, `greaterThan`, `greaterThanEqual`, `lessThan`, `lessThanEqual`, `between`, `notBetween`,
  `isNull`, `isNotNull`, `contains`, `containsAny`, `notContains`, `startsWith`, `notStartsWith`, `endsWith` and
  `notEndsWith`, alone or inside `and()`/`or()`. Anything else in an ON list (`limit`, `offset`, cursors, orders,
  selects, aggregates, joins, `containsAll`, `search`, `regex`, `exists`, vector and spatial queries) throws
  `Utopia\Database\Exception\Query` (`Unsupported join ON condition: <method>`) in `find()`, `count()`, `sum()` and
  `getDocument()`.
- A filter on a joined column (`alias.attribute`) is checked against the joined collection's attribute exactly as a
  filter on the main collection is checked against its own: type, size, array-ness and the `contains` rules. A vector
  query cannot target a joined attribute (`Vector queries cannot be used on a joined attribute: <alias.attribute>`).
- A filter on a joined column, in the query, in a join's ON list or in `having()`, is also converted as a filter on
  the main collection is: array `contains*` filters match elements, and datetimes (with `alias.$createdAt` and
  `alias.$updatedAt`) are compared in UTC.
- A select of joined attributes returns what it names. The main collection's unselected attributes are left out, as
  for any select; only a select of a related document's attributes (`relationship.attribute`) returns the others as
  well, as in 7.x.
- A select may name `alias.*` next to other selects: it stands for what the join returns without a select, the
  joined collection's `$id` and attributes as `alias.$id` and `alias.attribute`. An aggregation query still rejects
  it as an ungrouped select, and an alias the query does not join is not found.
- An order may name a joined attribute by its bare name when the main collection does not declare it and exactly one
  join's collection does (`orderAsc('price')` over a join whose collection declares `price`); a name the main
  collection declares always orders by the main collection. A bare name more than one join declares throws
  `Utopia\Database\Exception\Query` (`Attribute "<name>" is ambiguous across joins; qualify it with a join alias`).
  A bare order pages with a cursor like its qualified form: the cursor row holds the value under `alias.price`.
- Index the attributes your join conditions compare. A join on an unindexed attribute is accepted, but the engine
  has to scan the joined table to pair its rows, and on a large collection such a read can exceed the statement
  timeout (observed on MariaDB and MySQL shared tables).
- On MySQL, a joined collection's permission check is kept out of the optimizer's semi-join search (`NO_SEMIJOIN`)
  when the collection is left, right or full outer joined, and for every joined collection from five joins. Inner
  joins below five keep semi-joins.
- `Database::updateDocuments()` and `Database::deleteDocuments()` do not accept join queries. They throw
  `Utopia\Database\Exception\Query` with `Join queries are not supported for bulk updates` or
  `Join queries are not supported for bulk deletes`.
- On adapters without joins or aggregations (Memory, MongoDB, Redis), `find()`, `count()` and `sum()` reject those
  queries during validation with `Invalid query method: <method>`.
- MariaDB, MySQL and SQLite run a full outer join as two queries joined by `UNION ALL`. They accept one full outer
  join per query, and a right join after it has to join on a table joined before the full outer join or on the full
  outer joined table (directly or through other joins); other chains throw `Utopia\Database\Exception\Query`.
  PostgreSQL runs full outer joins natively and has no such limit. A full outer join combined with a right join
  under shared tables reads what a dedicated database reads, on every engine.

#### Paging a joined read

- A cursor over a joined read names the row the read returned, not only its main document. A joined row read is
  ordered by its explicit orders, then by the main `$sequence` (unless an order names the main `$id` or `$sequence`),
  then, for each join that can pair a row with several joined rows, by that join's `alias.$id` ascending (unless an
  order names that alias's `$id` or `$sequence`). An inner or left join whose condition compares the joined `$id` with
  `=` pairs at most one joined row and adds no order. Every page is ordered the same way, so paging with
  `cursorAfter()` or `cursorBefore()` returns each joined row exactly once.
- Pass a row the same read returned as the cursor. It has to carry every value the read orders by, under the name the
  read orders by (`note.score`, `$sequence`, `note.$id`); a missing value throws `Utopia\Database\Exception\Order`
  (`Cursor has no value for order attribute 'note.$id'. …`). A value is never taken from the main document's attribute
  of the same name. A read whose `select()` leaves a paged join's `alias.$id` out has to select it to be paged.
- A value may be null (a row an outer join did not match, a nullable attribute). Nulls keep the engine's position:
  first in ascending order on MariaDB, MySQL and SQLite, last on PostgreSQL, and the cursor pages through them.
- A row a right or full outer join returned without a main document (its `$id` is `''`) is a valid cursor for that
  read. A read without joins or `distinct()` still refuses a cursor document without an `$id`
  (`Invalid query: Invalid cursor: …`).
- `getDocument()` with a join that matches several joined rows returns the one with the lowest `$sequence`, join by
  join in join order.

### Aggregations

- An aggregation query is one with an aggregate (`count`, `countDistinct`, `sum`, `avg`, `min`, `max`, the
  statistical and the bitwise aggregates) or a `groupBy()`. A `groupBy()` without an aggregate counts. A select in
  an aggregation query may name only the attributes the query groups by; any other select throws
  `Utopia\Database\Exception\Query`
  (`Invalid query: Cannot select "<attribute>": an aggregation query can only select the attributes it groups by`).
  Group by the attribute or leave it out of the select. `*` and relationship wildcards (`key.*`, `parent.child.*`)
  are accepted and ignored, so a listing that always adds them keeps working. A join alias's `alias.*` is rejected
  like any ungrouped select. A join alias cannot equal a relationship key of the main collection (see
  [Joins](#joins)), so `key.*` is always the relationship's wildcard. The rule applies to `find()`, `count()` and
  `sum()`, which validate their query set the same way.
- An order in an aggregation query may name only an aggregate alias or an attribute the query groups by, and
  `orderRandom()` is accepted; any other order throws `Utopia\Database\Exception\Query`
  (`Invalid query: Cannot order by "<attribute>": an aggregation query can only order by its groups and aggregates`).
  A grouped attribute that only a joined collection declares matches its bare name and its aliased name alike.
- A `Utopia\Database\Validator\Query\Select` or `Validator\Query\Order` built directly applies its rule only when
  it is handed the query set's aggregates and groups (`setAggregations()`, `setGroupBy()`), as
  `Utopia\Database\Validator\Queries` does.
- `having()` conditions follow the filter rules. Each condition compares an aggregate alias of the same query, or an
  attribute passed to `groupBy()`. Compare aliases only at the top level of `having()`. `search()` inside `having()`
  needs a fulltext index, and value lists are capped by `setMaxQueryValues()`.
- `sum`, `avg`, `stddev*` and `variance`/`var*` need a numeric, non-array attribute. `bitAnd`, `bitOr` and `bitXor`
  need an integer attribute. Only `count` accepts `*`.
- `min` and `max` need an attribute whose values every engine can order: not an array, object, boolean, spatial or
  vector attribute (PostgreSQL has neither function for its BOOLEAN, JSONB, GEOMETRY and VECTOR columns), on the main
  collection and under a join alias. Otherwise they throw `Utopia\Database\Exception\Query` (`Aggregate <min|max>
  requires an attribute whose values are ordered, not an array, object, boolean, spatial or vector one:
  <attribute>`). `count` and `countDistinct` accept every attribute.
- Aggregates and `groupBy()` refuse a relationship attribute on a side that holds no column (the parent side of a
  one-to-many, the child side of a many-to-one and of a one-way one-to-one, either side of a many-to-many), as filters
  do: `Cannot aggregate virtual relationship attribute: <key>`, `Cannot group by virtual relationship attribute:
  <key>`.
- `stddev`, `stddevPop`, `stddevSamp`, `variance`, `varPop` and `varSamp` need
  `Capability::StatisticalAggregates`, and `bitAnd`, `bitOr` and `bitXor` need `Capability::BitwiseAggregates`.
  SQLite has neither, and `find()` throws `Utopia\Database\Exception\Query` for them there. Check
  `$database->getAdapter()->supports(...)` before offering them.
- An aggregate alias is an identifier (letters, digits and `_`, not starting with a digit) of at most 63 characters
  (`Validator\Query\Aggregate::MAX_ALIAS_LENGTH`), and it cannot repeat another aggregate's alias or the name a
  grouped attribute is returned under.
- Over an empty result, `count` and `countDistinct` return `0` and every other aggregate returns `null`, on every
  adapter. `Database::sum()` still returns `0` for no rows, as in 7.x.
- With joins, a bare attribute in an aggregate function or `groupBy()` refers to the main collection's attribute
  when the main collection declares it, else to the attribute of the one joined collection that declares it. A name
  no collection declares, or that more than one join declares, throws `Utopia\Database\Exception\Query`; qualify it
  with the join alias (`alias.attribute`).
- `Database::sum($collection, $attribute, $queries)` resolves a bare `$attribute` the same way: the main
  collection's attribute when it declares one, else the attribute of the one join that declares it
  (`sum('orders', 'price', [$join])`). A name several joins declare throws the ambiguity error above.
- Each group is returned under the column name the engine gives it (`groupBy(['note.name'])` returns `name`), unless
  another group of the query is returned under the same name: then the joined group is returned under its qualified
  name. `groupBy(['name', 'note.name'])` returns `name` (the main collection's) and `note.name`; two joined groups of
  one name (`groupBy(['a.code', 'b.code'])`) return `a.code` and `b.code`.
- Over an empty result an unaliased `bitAnd`, `bitOr` or `bitXor` returns `null` as an aliased one does (MariaDB and
  MySQL answer them with every bit set or `0`). On PostgreSQL every unaliased `bitAnd` is named `bit_and`, so give
  them aliases to read more than one.
- An aggregation query is not ordered by vector distance: a vector query only filters it, keeping the rows that have
  a vector, and a fulltext `search()` only filters it, as it filters every read.

### `distinct()`

- A `distinct()` read is ordered only by its explicit orders. Next to a vector query it is not ordered by distance,
  and its rows carry no `$distance`: the vector query only filters it, keeping the rows that have a vector. To order
  by distance, read rows without `distinct()`. A `search()` filters a distinct read, as it filters every read. A
  distinct row has no `$id`, so a cursor on a distinct read pages by its order values alone: pass a row the read
  returned, and give the read a `select()` of named attributes and an order on each of them. Otherwise the read
  throws `Utopia\Database\Exception\Query` (`A cursor on a distinct() read pages along its orders, …`).
- Vector distance orders row reads only, not aggregation queries or distinct reads. A distinct row stands for every
  row with its selected values, so it has no single distance, and PostgreSQL and MySQL order a `SELECT DISTINCT`
  only by selected columns. Without an explicit order, a distinct read's rows come back in engine order.
- On PostgreSQL and MySQL, `distinct()` with an order on an attribute the select leaves out throws
  `Utopia\Database\Exception\Query` (`A distinct() query can only be ordered by a selected attribute on this
  database`). Select the order attribute as well. On MariaDB, MySQL and SQLite the same applies to a `distinct()`
  query over a full outer join.

### Fulltext search

A fulltext `search()` only filters, as in 7.x. A search read is ordered by its explicit orders and then by
`$sequence`, and a cursor pages along them. Results are not ranked by relevance and carry no `_relevance`
attribute.

### Query builder: `from()`, `execute()` and `rawQuery()`

`Database::from($collection)` returns a utopia-php/query builder over the collection's table, and
`Database::execute($statement)` runs a statement as written. Both are available on the SQL adapters only
(`Feature\QueryBuilder`); elsewhere they throw `Utopia\Database\Exception`. They are an escape hatch for statements
the document API cannot express: they check no permissions, bypass the document and query caches (call
`purgeCachedDocument()` for documents you change: it also invalidates the collection's cached `find()` results),
write no permission rows, skip validation and run no hooks or events (a `Mirror` does not replicate them). Both throw
`Utopia\Database\Exception\Authorization` unless authorization is disabled: build and run them inside
`$database->getAuthorization()->skip(fn () => ...)`.

`Database::rawQuery($sql, $bindings)` is the same escape hatch for SQL you write yourself: it runs the statement as
written, checks no permissions, applies no tenant scope (under shared tables it reads every tenant's rows unless the
SQL limits them) and runs no hooks or events. It throws `Utopia\Database\Exception\Authorization` unless
authorization is disabled: run it inside `$database->getAuthorization()->skip(fn () => ...)`.

Skipping authorization does not skip tenancy for `from()`. Under shared tables a read stays within the tenant selected when
`from()` handed the builder out: the main table and every table joined through the builder's join methods. An
`update()` or `delete()` is kept to the tenant on its main table; the builder's join methods do not apply to them.
To read another tenant, select it with `setTenant()` or `withTenant()`; there is no other opt-out. A right or full
outer join needs the main table named as `from()` names it, and a statement without a main table is refused
(`Utopia\Database\Exception\Query`). Not scoped for you: `insert()`, which writes exactly the columns you give it,
`$tenant` included; SQL you write yourself; builders not obtained from `from()` (subqueries, unions, lateral joins);
and the second table of a dialect's multi-table write (`updateJoin()`, `deleteJoin()`, `updateFrom()`,
`deleteUsing()`), whose main table keeps its tenant condition.

### `exists()` and `notExists()`

`Query::exists([...])` and `Query::notExists(...)` run on the SQL adapters and take attribute names as their values.
On adapters with defined attributes each name has to be an attribute of the collection that holds a column, or an
`alias.attribute` of a join; an internal column name (`_permissions`), an unknown attribute, a relationship side
without a column or a related document's attribute throws `Utopia\Database\Exception\Query`. Schemaless adapters
accept any name.

### Query cache

`setQueryCache(new QueryCache($cache))` caches `find()` results per hostname, database, namespace, tenant and
collection. Writes through the `Database` invalidate only the scope they write in. `purgeCachedDocument()` and
`purgeCachedQueries($collection)` invalidate it as well; call one of them after changing data behind the library's
back.

- Use a cache adapter with generations (`Utopia\Cache\Feature\Leasable`: Redis and Redis\Multiplexing, and Pool,
  Sharding and CircuitBreaker when the adapters they wrap have them). With any other adapter a collection's cache
  stays off for one region TTL after each write, because concurrent writers cannot be told apart.
- Collection listings (`find('_metadata')`, `listCollections()`) are never cached.
- Under tenant-per-document, a write invalidates the scope of each written document's tenant.

### Custom types

Implement `Utopia\Database\Type\Custom` (`name()`, `encode()`, `decode()`) and register the type on a
`Utopia\Database\Type\TypeRegistry`. Give the registry to a `Database` with `setTypeRegistry()`, and list the type's
name in an attribute's `filters`. The type applies only to handles that share that registry. On those handles it
takes precedence over a global filter of the same name (`Database::addFilter()`), and filters passed to the
`Database` constructor take precedence over both. `register()` rejects the built-in filter names
(`Database::DEFAULT_FILTERS`) with `Utopia\Database\Exception\Duplicate`.

### Pools and profiling

- **Query profiling.** `Database::enableProfiling()` attaches a `Utopia\Database\Profiler\QueryProfiler` that
  records the SQL statements the adapter runs. The profiler keeps the newest `QueryProfiler::DEFAULT_CAPACITY`
  (1000) entries; older entries are dropped as new ones arrive. Change the bound with
  `$database->getProfiler()?->setCapacity($entries)` (at least 1). `getQueryCount()` and `getTotalTime()` cover every
  statement logged since the last `reset()`, including dropped ones, so they can exceed what `getLogs()` returns.
  `disableProfiling()` stops recording and detaches the profiler from the adapter; what was captured stays readable
  through `getProfiler()->getLogs()` until `reset()`. Behind `Adapter\Pool` a connection carries the handle's
  profiler only while it is checked out. A `Pool` subclass that checks connections out itself (with
  `$this->pool->use(...)`) should call `$this->releaseBorrowedAdapter($adapter)` before giving the connection back.
  Each `Utopia\Database\Profiler\QueryLog` carries the statement's bound values (`bindings`), the collection it
  reads (`collection`, for the statements `getDocument()`, `find()`, `count()` and `sum()` run) and the operation
  that ran it (`operation`, the `Utopia\Database\Event` value, such as `document_find`). Statements that bind by
  hand (`rawQuery()`, schema changes) log no bindings, and writes log no collection. `QueryLog` has no
  `explainPlan`.
- **Capability questions behind a `Pool`.** `supports()`, `capabilities()` and `hasFeature()` ask a connection the
  first time and are then answered without one, for every handle built over the same `Utopia\Pools\Pool`.
  `supports(Capability::DefinedAttributes)` is the exception: it reports the schema mode of the connection that
  answers, so it always asks one. `Database::enableLocks()` reaches every borrowed connection.
- **Read/write splitting (`Adapter\ReadWritePool`).** Reads go to the read pool and writes to the write pool. After
  a write returns, or a `withTransaction()` block finishes, reads stay on the write pool for the sticky window
  (`setStickyDuration()`, default 5000 ms; `setSticky(false)` turns it off), so a caller reads its own writes.
  `getDocument(..., forUpdate: true)` and `rawQuery()` always use the write pool, since a locking read must run on
  the primary and raw SQL may write; both also open the sticky window. So do the reads that decide a write: the
  batch `updateDocuments()` and `deleteDocuments()` select, and the document `upsertDocuments()` compares against. Calls that touch no data (capability checks
  such as `supports()`, `capabilities()` and `hasFeature()`, limits, value casting, and configuration such as
  `setSupportForAttributes()`) are answered wherever a read would be and never open the sticky window.
  `getHostname()` always names the write pool's host, because it namespaces document and query cache keys, and is
  looked up once per handle. `getDriver()` counts as a write: code that runs statements through the raw driver gets
  a write-pool connection.

## Known limitations

- A transaction begun on the adapter directly (`$database->getAdapter()->startTransaction()`) is not seen by the
  cache invalidation or the `document_purge` queue that `withTransaction()` keeps. Each write inside it invalidates
  the caches and fires `document_purge` when that write returns, inside the adapter transaction and before it
  commits, and a rollback does not withdraw them. Reads inside it are not served from the document cache. Group
  writes with `withTransaction()` instead.

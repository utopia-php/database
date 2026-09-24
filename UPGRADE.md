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
  `utopia-php/async` 0.1 is used by `Mirror` replication and the relationship hook.
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

- `Hook\Permissions` writes the permission rows that the SQL adapters (MariaDB, MySQL, PostgreSQL and SQLite)
  check document-level permissions against. Without it, a SQL adapter stores no permission rows, so `find()`,
  `count()` and `sum()` never return a document that only its document-level permissions make readable. MongoDB,
  Memory and Redis keep permissions with the document and do not need it.
- `Hook\Relationships` populates related documents on reads and runs nested writes, cascades and the relationship
  permission checks. Without it, a read returns a relationship attribute's stored value (the related document's id)
  instead of the related document, and a nested related document cannot be written. `Database::getRelationshipHook()`
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
  `ColumnType::BigInteger` and the enum value for every other type).
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
- Silences apply to the calling coroutine only.

### Hook failures

Whether a lifecycle hook's exception reaches the caller depends on the event, as in 7.x:

| Events | A hook throws an `\Exception` |
|---|---|
| `index_create`, `document_read`, `document_create`, `documents_create`, `document_update`, `documents_update`, `documents_upsert`, `document_increase`, `document_decrease`, `document_delete`, `documents_delete`, `document_find`, `document_count`, `document_sum`, and `document_purge` fired by a document write or `purgeCachedDocument()` | The exception reaches the caller; the remaining hooks do not run. |
| All other events: `database_*`, `collection_*`, `attribute_create`, `attributes_create`, `attribute_update`, `attribute_delete`, `index_rename`, `index_delete`, and `document_purge` fired by an attribute schema change | The exception is swallowed; the remaining hooks still run. |

An `\Error` (for example a `TypeError`) always reaches the caller. Differences from 7.x:

- 7.x also swallowed an `\Error` at the isolated events.
- At an isolated event, 7.x skipped the remaining listeners after the first failure. 8.0 runs them.
- `document_purge` from `updateDocument()` and `deleteDocument()` now fires after the write's transaction has
  committed. In 7.x it fired inside the transaction, so a failing purge listener made the transaction retry and then
  roll back. Now the write stays committed and the call throws. `updateDocuments()`, `upsertDocuments()`,
  `increaseDocumentAttribute()`, `decreaseDocumentAttribute()` and `deleteDocuments()` already fired it after commit
  in 7.x.

A `Decorator`'s exception always reaches the caller.

### `document_purge`

It fires once per written document from `updateDocument()` (for both the old and the new `$id` when the id changes),
`updateDocuments()`, `upsertDocuments()`, `increaseDocumentAttribute()`, `decreaseDocumentAttribute()`,
`deleteDocument()` and `deleteDocuments()`, and from `purgeCachedDocument()`. The payload is
`Document(['$id' => $id, '$collection' => $collectionId])`. As in 7.x, `createDocument()` and `createDocuments()` do
not fire it. Attribute schema changes fire it for the collection's metadata document (`$collection` = `_metadata`).

### `attribute_create` from `createAttributes()`

`createAttributes()` fires `attribute_create` once per attribute, with that attribute's `Document` as payload (the
same shape `createAttribute()` sends). It then fires `attributes_create` once, with the list. In 7.x,
`createAttributes()` fired `attribute_create` once, with the array of attribute documents as payload, and nothing
fired `attributes_create`.

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
- Linking a related document, at any nesting depth of an update, needs update permission on that document; without
  it the update throws `Utopia\Database\Exception\Authorization` and nothing is written. Relinking a document that
  is already linked needs only read permission.

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
  every nesting level. Otherwise `fromStorage()` builds a document like the constructor: nested arrays carrying
  `$id` or `$collection` become `Document`s, and a non-string `$id` or a `$permissions` value that is not an array
  still throws. Build caller input with the constructor, which rejects non-string permissions.
- `Document::setAttribute()` takes a `SetType` case, and `Document::getPermissionsByType()` takes a
  `PermissionType` case.

## Errors

- **Unique index violations.** Every adapter now reports a unique index violation as
  `Utopia\Database\Exception\Unique` with the message `Document with the requested unique attributes already exists`
  (7.x: `Unique index violation`). The class and its hierarchy are unchanged: `Unique` extends `Duplicate`, and a
  conflicting document `$id` still throws a plain `Duplicate` with `Document already exists`. Match on the class,
  not the message: catch `Unique` before `Duplicate` to tell the two apart. `Exception\Unique` has no constructor of
  its own and never rewrites the message it is given.
- **Unknown columns on MariaDB and MySQL.** A statement that names a column the table lacks (1054) now throws
  `Utopia\Database\Exception\NotFound` (`Attribute not found`) instead of a raw `PDOException`, as PostgreSQL
  already did. This includes a table that has drifted from its metadata: `find()`, `count()` and `sum()` that
  filter, order or group by the missing column, writes that name it, `updateAttribute()` and `renameAttribute()` on
  it, and raw queries.
- **`Database::sum()`** now validates its attribute (the second argument) the way a `Query::sum()` aggregate is
  validated, and throws `Utopia\Database\Exception\Query` when it does not name a numeric, non-array attribute: of
  the main collection or, under a join alias, of the collection that join reads. In 7.x an unknown attribute reached
  the engine and failed there, and a string or array attribute returned 0 (or failed on PostgreSQL). Sum only
  numeric attributes; with joins, qualify a joined attribute with its alias
  (`sum('orders', 'item.price', [$join])`).

## Caches

- **Cache key names changed: do not share a cache between 7.x and 8.0 processes.** Document cache keys now include
  the database name (`{cacheName}-cache-{hostname}:{database}:{namespace}:{tenant}:collection:{collection}`), and
  purging a document advances a per-collection epoch instead of deleting the old key. During a rolling upgrade, a
  write handled by a 7.x process purges only the old keys, so an 8.0 process can serve a stale cached document until
  the cache TTL. After the last 7.x process has stopped, flush the cache, or deploy without overlap.
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
  definitions, or `null` for a dialect that cannot declare one (MariaDB).

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
| `getInsertKeyword()` on `SQL` and `Postgres`, and `getInsertSuffix()` and `getInsertPermissionsSuffix()` on `SQL` (`SQLite` and `Postgres` keep their own) | protected | The query builder's insert, insert-or-ignore and upsert statements |
| `getUpsertStatement()` on `SQL` (abstract), `MariaDB`, `Postgres` and `SQLite` | protected, public on `MariaDB` and `SQLite` | The same |
| `SQL::registerOperatorBind()` | protected | `getOperatorSQL()` binds operator values itself |
| `SQL::getFulltextValue()` and `Postgres::getFulltextValue()` | protected | utopia-php/query's builders normalize search terms (`compileSearchExpr()`) |
| `Adapter::before()` and `Adapter::trigger()` | public, protected | `Hook\Transform`, registered with `Database::addHook()` |

`escapeWildcards()` is kept, and so are the public helpers `getLikeOperator()`, `getRegexOperator()`,
`SQL::getSpatialTypeFromWKT()` and `Query::isSpatialAttribute()`, even where nothing in the library calls them
any more.

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
  earlier one. If a statement fails on the new connection, `reconnect()` throws and the wrapper keeps the old
  connection, so the next call reconnects again instead of running on an unconfigured session. The MariaDB and
  MySQL adapters keep `setTimeout()` this way, so a timeout also bounds the statement that triggered the reconnect.
  Behind Swoole's `PDOProxy`, which reconnects without replaying session state, the adapter sets the timeout again
  before its next statement; the statement the proxy itself retries after its reconnect runs without it.
- **Lost-connection detection.** `Connection::hasError()` decides by the driver's error first: MySQL and MariaDB
  errors 1053, 2002, 2006, 2013 and 4031, SQLSTATE class `08`, and PostgreSQL `57P01` to `57P05`. Statement timeouts
  (MariaDB 1969, MySQL 3024, PostgreSQL 57014) are not lost connections and are never retried. The message fallback
  carries Swoole 6.2's `DetectsLostConnections` list, so a PHP without ext-swoole, or with
  `swoole.enable_library=Off`, detects the same lost connections as one with it.

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
  `resetGlobalCollections()`, `setTenantPerDocument()`, `setTimeout()`, `clearTimeout()`, `setMetadata()`,
  `resetMetadata()`, `enableFilters()`, `disableFilters()`, `skipFilters()`, `enableLocks()`, `enableProfiling()`,
  `disableProfiling()`, `setMigrating()` and `setTypeRegistry()` to its source and destination. In 7.x these changed
  the mirror alone.
- **Timeouts on the destination.** A destination that cannot apply a timeout (MariaDB applies it on the connection)
  is reported through `onError()` with the action `setTimeout` or `clearTimeout`; the call itself succeeds when the
  source applied it.
- **Profiling.** `$mirror->enableProfiling()` enables profiling on the source and the destination, and
  `$mirror->getProfiler()` returns the source's profiler, which records the mirror's queries, because both use the
  source's adapter. The destination's queries are recorded by `$mirror->getDestination()->getProfiler()`.
- **Scoped setters.** `withTenant()`, `withPreserveDates()`, `withPreserveSequence()`, `skipRelationships()`,
  `skipRelationshipsExistCheck()` and `withRequestTimestamp()` called on a mirror apply to the mirror and its source
  for the duration of the callback, not to the destination: replication can still be writing to the destination
  after the callback returns, and it applies the source's resulting documents with preserved dates.
  `skipValidation()` and `skipFilters()` also cover the destination. `withRequestTimestamp()` now runs its callback
  once; 7.x ran it once per database when a destination was set.
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
  `getDocument()`, with the same meaning as on `Queries\Documents`.
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
  `Utopia\Database\Exception\Query` (`Too many joins: at most 8 are allowed`).
- Join aliases must be identifiers, unique within the query regardless of case, and different from
  `Query::DEFAULT_ALIAS`; anything else throws `Utopia\Database\Exception\Query`.
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

### Aggregations

- An aggregation query is one with an aggregate (`count`, `countDistinct`, `sum`, `avg`, `min`, `max`, the
  statistical and the bitwise aggregates) or a `groupBy()`. A `groupBy()` without an aggregate counts. A select in
  an aggregation query may name only the attributes the query groups by; any other select throws
  `Utopia\Database\Exception\Query`
  (`Invalid query: Cannot select "<attribute>": an aggregation query can only select the attributes it groups by`).
  Group by the attribute or leave it out of the select. `*` and relationship wildcards (`key.*`, `parent.child.*`)
  are accepted and ignored, so a listing that always adds them keeps working. A join alias's `alias.*` is rejected
  like any ungrouped select, and a wildcard whose first segment is a join alias of the query is that alias's
  wildcard even when the main collection has a relationship with the same key: `account.*` next to a join aliased
  `account` is rejected. The rule applies to `find()`, `count()` and `sum()`, which validate their query set the
  same way.
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
- An aggregation query is not ordered by vector distance: a vector query only filters it, keeping the rows that have
  a vector, and a fulltext `search()` only filters it, as it filters every read.

### `distinct()`

- A `distinct()` read is ordered only by its explicit orders. Next to a vector query it is not ordered by distance,
  and its rows carry no `$distance`: the vector query only filters it, keeping the rows that have a vector. To order
  by distance, read rows without `distinct()`. A `search()` filters a distinct read, as it filters every read. A
  cursor on a distinct read pages along its explicit orders, so give the read an order on a selected attribute.
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

### Query builder: `from()` and `execute()`

`Database::from($collection)` returns a utopia-php/query builder over the collection's table, and
`Database::execute($statement)` runs a statement as written. Both are available on the SQL adapters only
(`Feature\QueryBuilder`); elsewhere they throw `Utopia\Database\Exception`. They are an escape hatch for statements
the document API cannot express: they check no permissions, bypass the document and query caches (call
`purgeCachedDocument()` for documents you change: it also invalidates the collection's cached `find()` results),
write no permission rows, skip validation and run no hooks or events (a `Mirror` does not replicate them). Both throw
`Utopia\Database\Exception\Authorization` unless authorization is disabled: build and run them inside
`$database->getAuthorization()->skip(fn () => ...)`.

Skipping authorization does not skip tenancy. Under shared tables a read stays within the tenant selected when
`from()` handed the builder out: the main table and every table joined through the builder's join methods. An
`update()` or `delete()` is kept to the tenant on its main table; the builder's join methods do not apply to them.
To read another tenant, select it with `setTenant()` or `withTenant()`; there is no other opt-out. A right or full
outer join needs the main table named as `from()` names it, and a statement without a main table is refused
(`Utopia\Database\Exception\Query`). Not scoped for you: `insert()`, which writes exactly the columns you give it,
`$tenant` included; SQL you write yourself; builders not obtained from `from()` (subqueries, unions, lateral joins);
and the second table of a dialect's multi-table write (`updateJoin()`, `deleteJoin()`, `updateFrom()`,
`deleteUsing()`), whose main table keeps its tenant condition.

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

- `Database::sum()` reads a bare attribute name from the main collection. A name that only a joined collection
  declares throws `Utopia\Database\Exception\Query` (`Attribute not found in schema: <name>`), where `find()` would
  resolve it through the join. Qualify a joined attribute with its join alias
  (`sum('orders', 'item.price', [$join])`).
- `groupBy()` over a main and a joined attribute of the same name (`groupBy(['name', 'note.name'])`) returns both
  groups under that one name, holding the joined value.
- A `distinct()` read with a cursor and no explicit order ignores the cursor, because it has no order to page along.
  Page a distinct read with an explicit order on a selected attribute.

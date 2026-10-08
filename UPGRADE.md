# Upgrading from 7.x to 8.0

This guide lists the changes you may need to make when you move from utopia-php/database 7.x (last release 7.4.0)
to 8.0. [CHANGELOG.md](CHANGELOG.md) lists everything that is new in 8.0. If you built against the unreleased
`feat-query-lib` branch, also read [Changes since the 8.0 pre-releases](#changes-since-the-80-pre-releases).

- [Before you start](#before-you-start)
- [Register the permission and relationship hooks](#register-the-permission-and-relationship-hooks)
- [Constants are now enums](#constants-are-now-enums)
- [Namespaces](#namespaces)
- [Queries](#queries)
- [Schema: value objects](#schema-value-objects)
- [Lifecycle events are hooks](#lifecycle-events-are-hooks)
- [Relationships](#relationships)
- [Documents](#documents)
- [Bulk writes and reads](#bulk-writes-and-reads)
- [Configuration toggles](#configuration-toggles)
- [Coroutines](#coroutines)
- [Errors](#errors)
- [Caches](#caches)
- [Filters](#filters)
- [Adapters](#adapters)
- [Mirror](#mirror)
- [Validators and helpers](#validators-and-helpers)
- [Removed unused public methods](#removed-unused-public-methods)
- [Changes since the 8.0 pre-releases](#changes-since-the-80-pre-releases)
- [Rules for features new in 8.0](#rules-for-features-new-in-80)
- [Known limitations](#known-limitations)

## Before you start

- PHP 8.5 or later is required, as for 7.x.
- Two new dependencies are installed with the library. `utopia-php/query` 0.7 provides the query, schema and
  builder types that 8.0 uses in its signatures (`Utopia\Query\Method`, `Utopia\Query\Schema\ColumnType`, ...).
  `utopia-php/async` 0.2 is used by `Mirror` replication and the relationship hook. Its
  `Utopia\Async\Serializer::unserialize()` no longer decodes closure payloads: code of your own that calls it on
  closure payloads switches to `Serializer::unserializeTrusted()` (see utopia-php/async's UPGRADE.md).
- Many constants became enums, and many signatures now take or return enum cases. PHP never treats an enum case as
  equal to a string, so a comparison like `$query->getMethod() === 'equal'` is now always `false` and raises no
  error. Run PHPStan at level 4 or higher on your code after upgrading: it reports these comparisons.
- Most API changes have a one-to-one replacement. Each table below lists the 7.x form and the 8.0 form; every
  removed name is listed with its replacement. 8.0 ships no aliases for removed names.

## Register the permission and relationship hooks

Document permissions and relationships are now hooks, and a `Database` registers neither on its own. Register both
right after you create the `Database`:

```php
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;

$database->addHook(new Permissions());
$database->addHook(new Relationships());
```

`addHook()` throws `Utopia\Database\Exception` for a hook it does not recognise. A hook that implements
`Hook\Attachable` is given the database it is added to through `attach(Database $database)`, so hooks are configured
in their constructor and never take the database there: `new Relationships(bool $prepare = true)`,
`new Tenancy(string $column = Storage::TENANT)`. A `Relationships` hook belongs to one database: adding it to a second
one throws, so give each database its own instance (or a `clone`). `removeHook($hook)` unregisters a hook instance, or every hook of a
class when given its class name.

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
  cascade runs, no key is set to null and `Restrict` does not block the delete.

## Constants are now enums

Every removed string constant maps to an enum case with the same backing value, except `Database::VAR_BIGINT` (see
[Attribute types](#attribute-types)). Stored metadata and query strings do not change.

### `Database`

| 7.x | 8.0 |
|---|---|
| `VAR_STRING`, `VAR_VARCHAR`, `VAR_TEXT`, `VAR_MEDIUMTEXT`, `VAR_LONGTEXT`, `VAR_INTEGER`, `VAR_BOOLEAN`, `VAR_DATETIME`, `VAR_ID`, `VAR_OBJECT`, `VAR_VECTOR`, `VAR_RELATIONSHIP`, `VAR_POINT`, `VAR_LINESTRING`, `VAR_POLYGON` | `Utopia\Query\Schema\ColumnType::String`, `Varchar`, `Text`, `MediumText`, `LongText`, `Integer`, `Boolean`, `Datetime`, `Id`, `Object`, `Vector`, `Relationship`, `Point`, `Linestring`, `Polygon` |
| `VAR_UUID7` | `ColumnType::Uuid7`. It names MongoDB's sequence id type (`Database::getIdAttributeType()`) and is not an attribute type: an attribute stored as `uuid7` is refused (see [Stored metadata](#stored-metadata)) |
| `VAR_FLOAT` (`'double'`) | `ColumnType::Double`. `ColumnType::Float` (`'float'`) is a separate, new type |
| `VAR_BIGINT` (`'bigint'`) | `ColumnType::BigInteger`, whose value is `'biginteger'` (see [Attribute types](#attribute-types)) |
| `STRING_TYPES` | No replacement: list the string cases (`String`, `Varchar`, `Text`, `MediumText`, `LongText`) |
| `SPATIAL_TYPES` | `$attribute->isSpatial()` on an `Attribute` |
| `ATTRIBUTE_FILTER_TYPES` | No replacement: the `datetime()`, `point()`, `lineString()`, `polygon()`, `vector()` and `object()` factories add their filter themselves. The built-in filter names are the `Utopia\Database\Filter` cases |
| `INTERNAL_ATTRIBUTES` | `$database->internalAttributes()`, a list of `Attribute` (tenant-aware) |
| `INSERT_BATCH_SIZE`, `DELETE_BATCH_SIZE` | `Database::BATCH_SIZE` (see [Bulk writes and reads](#bulk-writes-and-reads)) |
| `INDEX_KEY`, `INDEX_UNIQUE`, `INDEX_FULLTEXT`, `INDEX_SPATIAL`, `INDEX_OBJECT`, `INDEX_TRIGRAM`, `INDEX_TTL`, `INDEX_HNSW_EUCLIDEAN`, `INDEX_HNSW_COSINE`, `INDEX_HNSW_DOT` | `Utopia\Query\Schema\IndexType::Key`, `Unique`, `Fulltext`, `Spatial`, `Object`, `Trigram`, `Ttl`, `HnswEuclidean`, `HnswCosine`, `HnswDot` |
| `ORDER_ASC`, `ORDER_DESC` | `Utopia\Query\OrderDirection::Asc`, `Desc`, for index orders and adapter order types alike |
| `ORDER_RANDOM` | `OrderDirection::Random` for queries (`Query::orderRandom()`). An index refuses it with `Exception\Index` |
| `PERMISSION_CREATE`, `PERMISSION_READ`, `PERMISSION_UPDATE`, `PERMISSION_DELETE`, `PERMISSION_WRITE` | `Utopia\Database\PermissionType::Create`, `Read`, `Update`, `Delete`, `Write` |
| `PERMISSIONS` | `[PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete]` |
| `RELATION_ONE_TO_ONE`, `RELATION_ONE_TO_MANY`, `RELATION_MANY_TO_ONE`, `RELATION_MANY_TO_MANY` | `Utopia\Database\RelationshipType::OneToOne`, `OneToMany`, `ManyToOne`, `ManyToMany` |
| `RELATION_MUTATE_CASCADE`, `RELATION_MUTATE_RESTRICT`, `RELATION_MUTATE_SET_NULL` | `Utopia\Database\RelationshipDeleteAction::Cascade`, `Restrict`, `SetNull` |
| `RELATION_SIDE_PARENT`, `RELATION_SIDE_CHILD` | `Utopia\Database\RelationshipSide::Parent`, `Child` |
| `CURSOR_AFTER`, `CURSOR_BEFORE` | `Utopia\Query\CursorDirection::After`, `Before` |
| `EVENT_*` (all 33) | `Utopia\Database\Event` cases: `EVENT_DOCUMENT_CREATE` is `Event::DocumentCreate`, `EVENT_ALL` is `Event::All`, and so on. The values are unchanged (`Event::DocumentCreate->value === 'document_create'`). New cases: `DatabaseUpdate`, `DocumentUpsert`, `DocumentAggregate`, `AttributeRename`, `IndexesCreate` |
| `COLLECTION` (protected) | `Database::collectionDefinition(): Collection` |

### `Query`

- The 48 `TYPE_*` constants are replaced by `Utopia\Query\Method` cases with the same values. Most names map
  directly (`TYPE_EQUAL` is `Method::Equal`, `TYPE_CURSOR_AFTER` is `Method::CursorAfter`); these four do not:
  `TYPE_GREATER` is `Method::GreaterThan`, `TYPE_GREATER_EQUAL` is `Method::GreaterThanEqual`, `TYPE_LESSER` is
  `Method::LessThan` and `TYPE_LESSER_EQUAL` is `Method::LessThanEqual`. `Query::TYPE_ELEM_MATCH` is removed.
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

- `Validator\Query\Select::INTERNAL_ATTRIBUTES` (protected) is removed. `$database->internalAttributes()` returns the
  internal attributes as `Attribute` value objects, including `$tenant` under shared tables.
- `Adapter\SQL::VECTOR_DISTANCE_COLUMN` (protected) is replaced by `Utopia\Database\Storage::DISTANCE`.

### Arguments that take enum cases

These methods take or return enum cases where 7.x used the constants' strings:

| Method | Argument or return value |
|---|---|
| `Database::find()`, `cursor()` | `PermissionType $forPermission = PermissionType::Read` |
| `Database::setTimeout()`, `clearTimeout()` | `Event $event = Event::All` |
| `Database::getIdAttributeType()` | returns a `ColumnType` case (was a string) |
| `Document::setAttribute()` | `SetType $type = SetType::Assign` |
| `Document::getPermissionsByType()` | `PermissionType $type` |
| `Query::getMethod()` | returns a `Method` case (see [Queries](#queries)) |
| `Operator::__construct()`, `setMethod()` | `OperatorType $method` |
| `Operator::getMethod()` | returns an `OperatorType` case |

## Namespaces

Moved classes keep no alias under their 7.x name.

| 7.x | 8.0 |
|---|---|
| `Utopia\Database\Helpers\ID` | `Utopia\Database\Id` |
| `Utopia\Database\Helpers\Permission`, `Helpers\Role` | `Utopia\Database\Permission`, `Utopia\Database\Role` |
| `Utopia\Database\Mirroring\Filter` | `Utopia\Database\Mirror\Filter`; its `init()` is `initialize()` |
| `Validator\Attribute` | `Validator\AttributeDefinition` (see [Validators and helpers](#validators-and-helpers)) |
| `Validator\Index` | `Validator\IndexDefinition` |
| `Validator\Queries` | `Validator\Queries\Base` |
| `Validator\IndexedQueries` | `Validator\Queries\Indexed` |
| `Validator\ObjectValidator` | `Validator\ObjectValue` |

```php
// 7.x
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

// 8.0
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\Role;

$permissions = [Permission::read(Role::user(Id::unique()))];
```

## Queries

- `Utopia\Database\Query` now extends `Utopia\Query\Query`. Every 7.x method still exists, and the query wire format
  (`Query::parse()`, JSON) is unchanged.
- `Query::getMethod()` returns a `Utopia\Query\Method` case instead of a string, and `Operator::getMethod()` returns
  an `OperatorType` case. Compare with cases: `$query->getMethod() === Method::Equal`. A comparison with a string is
  always `false` and raises no error (see [Before you start](#before-you-start)). `setMethod()`, `isMethod()` and the
  constructor accept a `Method` case or its string value.
- `Query::groupByType()` returns a `Utopia\Database\ParsedQuery` object instead of an array. It extends the query
  library's `ParsedQuery` with `orderAttributes` and `orderTypes` (`OrderDirection` cases); read the groups from its
  properties. A cursor that is not a document throws `Exception\Query`. `Query::groupForDatabase()` is removed.
- `Query::cursorAfter()` and `Query::cursorBefore()` take `array|object` instead of `Document`. Pass the cursor
  document, as before. `Validator\Query\Cursor` accepts a `Document` or a document id and refuses an array.
- Aggregates and joins carry their alias in a property: read it with `getAlias()`. `toArray()` writes it as
  `"alias"`, and `parse()` reads it there as well as from the 7.x positions.
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
  (strings, integers, doubles, booleans).
- On MongoDB, `startsWith()` and `endsWith()` are anchored: `startsWith('foo')` no longer returns `barfoo`, and
  `endsWith('foo')` no longer returns `foobar`. Both remain case-sensitive on MongoDB (on MariaDB and MySQL they
  follow the column collation). A caller that relied on the substring behaviour should use `containsString()`.
- `orderRandom()` is validated against `Capability::OrderRandom`. On MongoDB, which does not support it, `find()`
  throws `Utopia\Database\Exception\Query` (`Random order is not supported by this adapter`) instead of a generic
  `Exception`.
- `distinct()` needs `Capability::Aggregations`. On Memory, Redis and MongoDB, `find()` refuses it with
  `Utopia\Database\Exception\Query` (`Distinct queries are not supported by this adapter`) also when validation is
  skipped; MongoDB returned duplicate rows then.
- A filter on a path into an object attribute (`meta.address.city`) takes keys of `a-z`, `A-Z`, `0-9`, `_` and `-`
  only. The query validator refuses any other key with `Utopia\Database\Exception\Query` on every adapter (7.x
  refused such keys on PostgreSQL only), and on PostgreSQL the query is refused also when validation is skipped.
- On PostgreSQL, an exact search (`search('title', '"foo bar"')`) matches the words as an adjacent phrase, as on
  MariaDB, MySQL and SQLite, and `notSearch()` with an exact term excludes only that phrase. In 7.4.0 PostgreSQL
  matched both words in any order. To match both words in any order, pass a `search()` for each word.

## Schema: value objects

The schema methods take value objects instead of long lists of scalar arguments, and the create and update methods
return what they stored. `Attribute`, `Index` and `Relationship` are `final readonly` classes with a private
constructor: build them with their factories. `Collection` stays a `Document`, because it is the metadata row, and is
built with `Collection::create()`. Updates take `AttributeUpdate`, `CollectionUpdate` and `RelationshipUpdate`.

```php
// 7.x
$database->createCollection('movies', $attributes, $indexes, [Permission::read(Role::any())], true);
$database->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true);
$database->createIndex('movies', 'idx_year', Database::INDEX_KEY, ['year'], [], [Database::ORDER_DESC]);
$database->createRelationship('movies', 'reviews', Database::RELATION_ONE_TO_MANY, true, 'reviews', 'movie', Database::RELATION_MUTATE_CASCADE);
$database->updateAttributeRequired('movies', 'year', false);
$database->updateRelationship('movies', 'reviews', onDelete: Database::RELATION_MUTATE_SET_NULL);
$database->updateCollection('movies', [Permission::read(Role::any())], false);

// 8.0
$database->createCollection(Collection::create(
    id: 'movies',
    attributes: $attributes,
    indexes: $indexes,
    permissions: [Permission::read(Role::any())],
    documentSecurity: true,
));
$database->createAttribute('movies', Attribute::integer('year', required: true));
$database->createIndex('movies', Index::key('idx_year', ['year'], orders: [OrderDirection::Desc]));
$database->createRelationship('movies', Relationship::oneToMany(
    'reviews',
    key: 'reviews',
    twoWay: true,
    twoWayKey: 'movie',
    onDelete: RelationshipDeleteAction::Cascade,
));
$database->updateAttribute('movies', 'year', new AttributeUpdate(required: false));
$database->updateRelationship('movies', 'reviews', new RelationshipUpdate(onDelete: RelationshipDeleteAction::SetNull));
$database->updateCollection('movies', new CollectionUpdate(permissions: [Permission::read(Role::any())], documentSecurity: false));
```

Every schema method names its collection argument `$collection`, and its attribute, index or relationship argument
`$key`.

| 7.x | 8.0 |
|---|---|
| `createCollection(string $id, array $attributes = [], array $indexes = [], ?array $permissions = null, bool $documentSecurity = true): Document` | `createCollection(Collection $collection): Collection` |
| `updateCollection(string $id, array $permissions, bool $documentSecurity): Document` | `updateCollection(string $collection, CollectionUpdate $update): Collection`. A `null` field keeps the stored value |
| `getCollection(string $id): Document`, an empty `Document` when the collection is missing | `getCollection(string $collection): Collection`, which throws `Exception\NotFound` when it is missing |
| — | `findCollection(string $collection): ?Collection`, `null` when the collection is missing. A miss fires no event |
| `deleteCollection(string $id): bool` | `deleteCollection(string $collection): void` |
| `exists(?string $database = null, ?string $collection = null): bool` | `exists(?string $database = null): bool` for a database, `collectionExists(string $collection, ?string $database = null): bool` for a collection |
| `createAttribute(string $collection, string $id, string $type, int $size, bool $required, mixed $default = null, bool $signed = true, bool $array = false, ?string $format = null, array $formatOptions = [], array $filters = []): bool` | `createAttribute(string $collection, Attribute $attribute): Attribute` |
| `createAttributes(string $collection, array $attributes): bool`, with attribute arrays | `createAttributes(string $collection, array $attributes): array`, a list of `Attribute` in and out |
| `updateAttribute(string $collection, string $id, ?string $type = null, ?int $size = null, ?bool $required = null, mixed $default = null, ?bool $signed = null, ?bool $array = null, ?string $format = null, ?array $formatOptions = null, ?array $filters = null, ?string $newKey = null): Document` | `updateAttribute(string $collection, string $key, AttributeUpdate $update): Attribute` |
| `updateAttributeRequired()`, `updateAttributeFormat()`, `updateAttributeFormatOptions()`, `updateAttributeFilters()`, `updateAttributeDefault()` | `updateAttribute()` with the `AttributeUpdate` field of the same name (`required`, `format`, `filters`, `default`) |
| `renameAttribute(string $collection, string $old, string $new): bool` | `renameAttribute(string $collection, string $old, string $new): void`. `AttributeUpdate(key: ...)` renames as well |
| `deleteAttribute(string $collection, string $id): bool` | `deleteAttribute(string $collection, string $key): void` |
| `checkAttribute(Document $collection, Document $attribute): bool` | `checkAttribute(string $collection, Attribute $attribute): bool` |
| `createIndex(string $collection, string $id, string $type, array $attributes, array $lengths = [], array $orders = [], int $ttl = 1): bool` | `createIndex(string $collection, Index $index): Index` |
| — | `createIndexes(string $collection, array $indexes): array`. It validates every index first, creates each one, then writes the definition once |
| `renameIndex(string $collection, string $old, string $new): bool` | `renameIndex(string $collection, string $old, string $new): void` |
| `deleteIndex(string $collection, string $id): bool` | `deleteIndex(string $collection, string $key): void` |
| `createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, ?string $id = null, ?string $twoWayKey = null, string $onDelete = Database::RELATION_MUTATE_RESTRICT): bool` | `createRelationship(string $collection, Relationship $relationship): Relationship`, with both keys resolved |
| `updateRelationship(string $collection, string $id, ?string $newKey = null, ?string $newTwoWayKey = null, ?bool $twoWay = null, ?string $onDelete = null): bool` | `updateRelationship(string $collection, string $key, RelationshipUpdate $update): Relationship`. It works from either side |
| `deleteRelationship(string $collection, string $id): bool` | `deleteRelationship(string $collection, string $key): void` |
| `getInternalAttributes(): array` | `internalAttributes(): array`, a list of `Attribute` |
| `getSchemaAttributes(string $collection): array`, `getSchemaIndexes(string $collection): array`, lists of `Document` | Lists of `Schema\Column` and `Schema\Index` (see [Schema introspection](#schema-introspection)) |
| `getIdAttributeType(): string` | `getIdAttributeType(): ColumnType` |
| — | `update(string $database, string $new): bool` renames a database (see [Renaming a database](#renaming-a-database)) |

`create()`, `exists()`, `update()` and `delete()` on a database keep returning `bool`.

### `Attribute`

One factory per type. Each takes only the arguments its type uses:

| Factory | Notes |
|---|---|
| `string($key, int $size = Database::LENGTH_KEY, ...)`, `varchar(...)` | Also `required`, `default`, `array`, `format`, `filters` |
| `text($key, ?int $size = null, ...)`, `mediumText(...)`, `longText(...)` | `size: null` means the engine's maximum for the type |
| `integer($key, ..., bool $signed = true, bool $array = false, IntegerWidth $width = IntegerWidth::Bits32, ...)` | A 7.x integer of size 8 or more is `width: IntegerWidth::Bits64` |
| `bigInteger(...)`, `float(...)`, `double(...)` | `signed`, `array`, `format`, `filters` |
| `boolean($key, bool $required = false, bool\|array\|null $default = null, bool $array = false, array $filters = [])` | |
| `datetime($key, bool $required = false, string\|array\|null $default = null, bool $array = false)` | Adds the `datetime` filter |
| `point($key, ...)`, `lineString($key, ...)`, `polygon($key, ...)` | Never arrays; add their filter |
| `vector($key, int $dimensions, ...)` | The size is the dimension count |
| `object($key, ...)`, `id($key, ...)` | |
| `relationship($key, Relationship $relationship, RelationshipSide $side)` | Built for you by `createRelationship()` |

```php
// 7.x
$database->createAttribute('movies', 'rating', Database::VAR_INTEGER, 8, false, 0, false, false, 'range', ['min' => 0, 'max' => 10], ['encrypt']);

// 8.0
$database->createAttribute('movies', Attribute::integer(
    'rating',
    default: 0,
    signed: false,
    width: IntegerWidth::Bits64,
    format: new Format('range', ['min' => 0, 'max' => 10]),
    filters: ['encrypt'],
));
```

- **Properties, not getters.** Read `$attribute->key`, `type` (`ColumnType`), `size` (`?int`, `null` where the type
  has no size), `required`, `default`, `signed`, `array`, `format` (`?Format`, with `name` and `options`), `filters`
  (`list<string>`), `relationship` (`?Relationship`) and `side` (`?RelationshipSide`). A 7.x attribute's
  `format` and `formatOptions` are one `Format`. `filters` takes `Utopia\Database\Filter` cases or names.
- **Helpers.** `width(): ?IntegerWidth` (integers only), `resolvedSize(): int`, `isSpatial()`, `isNumeric()`,
  `isInteger()`, `bounds(): ?NumericBounds`, the static `Attribute::isRelationship(Document $attribute)`, and
  `apply(AttributeUpdate $update)` and `withFilters(array $filters)`, which return a changed copy.
  `Attribute::availableTypes(Adapter\Profile $profile)` lists the types an adapter supports, out of `Attribute::TYPES`.
- **Storage.** `Attribute::fromDocument()` reads a stored attribute document and `toDocument()` writes one with the
  same keys. `Attribute::fromArray()` reads an array. `Attribute::typeFromStored()` and `Attribute::storedType()`
  convert between `ColumnType` cases and stored type strings (see [Attribute types](#attribute-types)).
- **Library fields only.** `status` and `options` are not attribute fields any more: `fromDocument()` ignores both,
  except a relationship's options, which become `relationship` and `side`. Keep application fields such as a
  `status` in your own documents.
- **Normalised values.** The factories, `apply()` and `withFilters()` keep each type's invariants: a type's own
  filter is always kept, `size` is `null` where the factory takes no size, and spatial, object and vector attributes
  are never arrays. A spatial attribute stored with a size or an array flag is read without them. Stored values
  are normalised per type:

  | Factory | Stored values |
  |---|---|
  | `datetime()` | `signed: false`, `filters: ['datetime']` |
  | `point()`, `lineString()`, `polygon()` | `array: false`, `filters: [<type>]` |
  | `vector()` | `size: <dimensions>`, `array: false`, `filters: ['vector']` |
  | `object()` | `array: false`, `filters: ['object']` |
  | `integer()` | `size` 0 (`Bits32`) or 8 (`Bits64`) |
  | `boolean()`, `double()`, `id()` | `size: 0` |
  | `string()`, `varchar()`, `text()`, `mediumText()`, `longText()` | `signed: true` |
  | `relationship()` | the 7.x `options` shape plus `side` |

  Code that compares stored attribute documents field by field (for example to detect drift) has to compare these
  normalised values.
- **64-bit integers.** `bounds()` gives a `Bits64` integer the bounds `[PHP_INT_MIN, PHP_INT_MAX]` signed and
  `[0, PHP_INT_MAX]` unsigned; 7.x applied the 32-bit range. `increaseDocumentAttribute()`,
  `decreaseDocumentAttribute()` and the numeric operators on a size-8 integer accept values past 2147483647.

### `Index`

| Factory | Notes |
|---|---|
| `key($key, array $attributes, array $lengths = [], array $orders = [])`, `unique(...)` | Orders are `?OrderDirection` |
| `fulltext($key, array $attributes)`, `trigram($key, array $attributes)` | No lengths or orders |
| `spatial($key, string $attribute, ?OrderDirection $order = null)`, `object($key, string $attribute)` | One attribute |
| `hnswEuclidean($key, string $attribute)`, `hnswCosine(...)`, `hnswDot(...)` | One vector attribute |
| `ttl($key, string $attribute, int $ttl)` | `ttl` of at least 1 |

```php
// 7.x
$database->createIndex('movies', 'expiry', Database::INDEX_TTL, ['expiresAt'], [], [], 3600);

// 8.0
$database->createIndex('movies', Index::ttl('expiry', 'expiresAt', 3600));
```

- Read `$index->key`, `type` (`IndexType`), `attributes`, `lengths` (`list<?int>`), `orders`
  (`list<?OrderDirection>`) and `ttl` (`?int`, TTL indexes only). `withKey()`, `withLengths()` and `withOrders()`
  return a changed copy.
- `OrderDirection::Random` and a `ttl` below 1 throw `Exception\Index`.
- Fulltext and TTL indexes store no `orders`, fulltext indexes store no `lengths`, and an index other than TTL no
  longer stores `ttl: 1`. `Index::fromDocument()` reads both shapes, the 7.x `'asc'`/`'desc'` orders, and the legacy
  type `index` as a key index.

### `Relationship`

`Relationship::oneToOne()`, `oneToMany()`, `manyToOne()` and `manyToMany()` take
`(string $relatedCollection, ?string $key = null, bool $twoWay = false, ?string $twoWayKey = null,
RelationshipDeleteAction $onDelete = RelationshipDeleteAction::Restrict)`. The collection it belongs to is the first
argument of `createRelationship()`. A `null` key is derived from the related collection's id and a `null` two-way key
from the source collection's id; `createRelationship()` returns the relationship with both resolved.

- Read `$relationship->relatedCollection`, `type` (`RelationshipType`), `twoWay`, `key`, `twoWayKey` and
  `onDelete` (`RelationshipDeleteAction`). `inverse(string $collection)` is the other side's view, and
  `apply(RelationshipUpdate $update)` a changed copy.
- `onDelete` has three cases: `Cascade`, `Restrict` and `SetNull`. `RelationshipDeleteAction::toForeignKeyAction()`
  converts for the adapter.
- A relationship attribute of a `Collection` carries its `Relationship` and `RelationshipSide` in
  `$attribute->relationship` and `$attribute->side`.

### `Collection`

```php
// 7.x
$collection = $database->getCollection('movies');
if ($collection->isEmpty()) {
    // missing
}
foreach ($collection->getAttribute('attributes', []) as $attribute) {
    $type = $attribute->getAttribute('type');
}

// 8.0
$collection = $database->findCollection('movies');
if ($collection === null) {
    // missing
}
foreach ($collection->attributes() as $attribute) {
    $type = $attribute->type;
}
```

- `Collection::create(string $id, string $name = '', array $attributes = [], array $indexes = [], ?array $permissions
  = null, bool $documentSecurity = true, array $metadata = [])` builds one; `Collection::fromArray()` and
  `Collection::fromDocument()` read a stored definition.
- `attributes()` and `indexes()` return the definition as `Attribute` and `Index` lists, built on the first call.
  `name()`, `documentSecurity()` and `declaredPermissions(): ?array` read the other fields.
  `Collection::NAME`, `ATTRIBUTES`, `INDEXES` and `DOCUMENT_SECURITY` name the stored keys.
- `documentSecurity` is always stored: `Collection::create(documentSecurity: false)` reads back `false`.
- `$metadata` takes only keys the metadata collection stores. `create()` refuses `attributes`, `indexes` and
  `documentSecurity` in it (pass them as arguments), and `createCollection()` throws `Exception\Structure` for a
  collection carrying any other key, before anything is created.

### Updates

`new AttributeUpdate(?ColumnType $type = null, ?int $size = null, ?bool $required = null, mixed $default =
Unchanged::Value, ?bool $signed = null, ?bool $array = null, Format|Unchanged|null $format = Unchanged::Value, ?array
$filters = null, ?string $key = null)` is sparse: `null` keeps a field. `default` and `format` use
`Unchanged::Value` to keep the stored value, because `null` is a value for them:

- `default: null` removes the default, and `format: null` removes the format.
- A default on an attribute stored as required throws, and so does a default sent together with `required: true`.
  `required: true` alone removes the stored default.
- `required: false` relaxes the column's `NOT NULL`. 7.x's `updateAttributeRequired(false)` changed only the
  metadata and left the column `NOT NULL`.
- `key` renames the attribute, like `renameAttribute()`.
- A `Mirror` replicates attribute updates to its destination.

`new CollectionUpdate(?array $permissions = null, ?bool $documentSecurity = null)` and
`new RelationshipUpdate(?string $key = null, ?string $twoWayKey = null, ?bool $twoWay = null,
?RelationshipDeleteAction $onDelete = null)` follow the same rule.

### Stored metadata

Collection metadata written by 7.x reads without migration: every 7.4 `VAR_*` type hydrates. Two kinds of stored
value make a collection unreadable, because hydrating its definition throws, so every read and write of the
collection fails until the row is repaired:

- an attribute type outside `Attribute::TYPES`, `uuid7` included: `Exception\Structure`;
- a relationship `onDelete` of `setDefault` or `noAction`: `Exception\Relationship`.

Repair such a row before you upgrade, while 7.x still reads it: set a supported `onDelete` with
`updateRelationship()`, and delete or recreate the attribute with a supported type. After the upgrade, edit the
collection's row in the metadata table (`_metadata`): in its `attributes`, set the relationship's `options.onDelete`
on both sides to `cascade`, `restrict` or `setNull`, or remove the attribute of the unknown type. Then drop the cached
definition with `$database->purgeCachedDocument(Database::METADATA, $collection)`.

An index of an unknown stored type is read as a key index, so it does not block reads. Creating such an index is
refused.

### Attribute types

- `Database::VAR_BIGINT` is replaced by `ColumnType::BigInteger`, whose value is `'biginteger'`. The type stored in
  collection metadata is still `'bigint'`, exactly as in 7.x: existing rows need no migration and new bigint
  attributes are written as `'bigint'` too. Do not compare or write stored type strings against
  `ColumnType::BigInteger->value`. Use the value object (`$attribute->type === ColumnType::BigInteger`), read a raw
  string with `Attribute::typeFromStored($type)` (it accepts `'bigint'` and `'biginteger'`), and write one with
  `Attribute::storedType($type)` (it returns `'bigint'` for `ColumnType::BigInteger` and the enum value for every
  other type). Error messages that name a type use the stored spelling too: a bigint default mismatch reads
  `Default value … does not match given type bigint`, as in 7.x.
- `toDocument()` and the attribute event payloads report bigint attributes as `'bigint'`.
- An empty `format` means no format: an attribute read from metadata written by 7.x, which stored `''`, has a
  `null` format.

### Collections and attributes

- `createCollection()` checks every attribute like `createAttribute()`: object, spatial and vector attributes need
  the adapter to support them. An unknown type cannot reach it: `Attribute::fromArray()` and `fromDocument()` refuse
  it with `Exception\Structure`. In 7.x the SQL adapters failed with `Unknown type: <type>` and MongoDB created the
  collection.
- `createCollection()`, `createAttribute()` and `createAttributes()` return what they stored: the filters added for
  `datetime`, `object`, spatial and vector attributes, and the index `lengths` and `orders` adjusted for the adapter.
  The value objects you pass are never changed, so shared definitions, such as the ones in a config array, are safe
  to pass directly. In 7.x `createCollection()` wrote these changes into the documents it was given.
- `updateAttribute()` now updates `id` attributes (7.x threw `Unknown attribute type: id`), and it refuses
  relationship attributes with `Cannot update relationship as an attribute`: use `updateRelationship()`.
- The default of a point, linestring or polygon attribute is validated like a value of that attribute: a point
  needs two numeric coordinates in range, a linestring at least two points, a polygon closed rings of at least four
  points. 7.x accepted any array.
- `createAttribute()` and `createAttributes()` refuse a varchar of size 0 or above the maximum varchar length, as
  `createCollection()` does and as 7.x did.
- **Index length accounting.** Big integer columns (big integer, id, and 64-bit integer) count 8 bytes toward the
  maximum index length, and a `text()`, `mediumText()` or `longText()` attribute without a size counts as the
  engine's maximum for its type. An index that relied on the old undercount has to give the text column a prefix
  length.
- **Existing indexes.** On adapters with `Capability::SchemaIntrospection`, `createIndex()` compares an index that
  exists in the schema but not in the metadata with the request (columns, prefix lengths, key, unique, fulltext or
  spatial): a match is adopted and a mismatch is dropped and recreated, as in 7.3.12.
- `analyzeCollection()` refreshes planner statistics on PostgreSQL and SQLite (the collection's table and its
  permissions table) and returns `true`; it returned `false` there. Call it after bulk loads (migrations, imports):
  a freshly loaded collection otherwise plans joins with empty-table statistics, and SQLite never gathers
  statistics on its own.
- On PostgreSQL `getSizeOfCollection()` reports the table's relation size, which a delete does not reduce until the
  table is vacuumed; `analyzeCollection()` refreshes statistics only.

### Schema introspection

`getSchemaAttributes()` returns `list<Schema\Column>` (`name`, `type` as the engine's canonical native type,
`length`, `nullable`) and `getSchemaIndexes()` returns `list<Schema\Index>` (`name`, `type` as `IndexType`,
`columns`, `lengths`), read back from the engine. Both are meaningful only where the adapter declares
`Capability::SchemaIntrospection`: MariaDB, MySQL, SQLite and PostgreSQL. MongoDB, Memory and Redis return `[]`.

PostgreSQL now introspects its schema, so it reconciles orphan columns and indexes like the other SQL adapters:
`createAttribute()` and `createIndex()` adopt or replace a column or index the schema has but the metadata lacks.
PostgreSQL reports a fulltext index as a key index and no prefix lengths. An index whose name PostgreSQL shortened to
its md5 form, or another tenant's index of a shared table, is not matched and is created as before.

### Renaming a database

`Database::update(string $database, string $new): bool` renames a database and fires `Event\Database\Updated`. It
retires the cached definitions and documents of the moved collections under both names. Every adapter refuses it
under shared tables, also when called on the adapter directly.

| Adapter | How |
|---|---|
| MariaDB, MySQL | Moves every table into the new schema in one atomic `RENAME TABLE`, then drops the old schema when it is empty (a table created there during the rename keeps it). Grants on the old schema do not move |
| PostgreSQL | `ALTER SCHEMA ... RENAME TO` |
| SQLite | Nothing to move: SQLite stores no database name, so `update()` succeeds and `exists()` is always `false` |
| Memory, Redis | Re-keys the stored data, and rolls back on failure |
| MongoDB | Moves each collection, and rolls back on failure. A sharded cluster throws `Exception`. The client stays bound to the old database: build a new client for the new name |

## Lifecycle events are hooks

`Database::on()`, `Database::before()`, `Mirror::on()` and `Adapter::before()` are removed, and so are the
`Database::EVENT_*` constants. Everything is registered with `Database::addHook()`, which dispatches on the hook's
type and throws for a hook it does not recognise:

| Hook interface | Receives | Replaces |
|---|---|---|
| `Utopia\Database\Hook\Lifecycle` | a typed event object per event, for side effects | `on()` |
| `Utopia\Database\Hook\Decorator` | each document a read or write returns, to modify it | nothing in 7.x |
| `Utopia\Database\Hook\Transform` | each SQL statement before it runs | `before()` |
| `Utopia\Database\Hook\Write` | document writes, to write rows of their own, such as `Hook\Permissions` | built into 7.x |
| `Utopia\Database\Hook\Relationships` | relationship resolution and mutation | built into 7.x |
| `Utopia\Database\Cache\Invalidator` | the writes that invalidate the `find()` query cache | nothing in 7.x |

`removeHook($hook)` unregisters a hook instance, or every hook of a class given its class name.

### Lifecycle hooks (replaces `on()` and the `EVENT_*` constants)

A listener is now an object implementing `Utopia\Database\Hook\Lifecycle` and registered with
`Database::addHook()`. Its `handle(Event\Domain $event): void` receives one typed event object per event: every
`Event` case except `All` has a `final readonly` class under `Utopia\Database\Event\{Database,Collection,Attribute,
Index,Document,Permission}`, and `$event->event` is the `Event` case. Match on the class to read its typed payload. To
keep 7.x's by-name behaviour, also implement `Utopia\Database\Hook\Named` (`getName(): string`):

- Registering a named hook replaces the lifecycle hook already registered under that name, in its position.
  Re-registering on every request or job no longer stacks listeners. Hooks without a name are appended every time
  they are added.
- A name is unique per `Database`, across all events. 7.x kept one listener per event and name, so
  `on(EVENT_DOCUMENT_CREATE, 'usage', ...)` and `on(EVENT_DOCUMENT_DELETE, 'usage', ...)` were two listeners. In 8.0
  several `Named` hooks with the same name keep only the one registered last: handle all of a name's events in one
  hook, or give each hook its own name.
- `silent()` can silence a named hook on its own (see below).
- A hook that also implements `Hook\Selective` (`handles(Event $event): bool`) receives only the events it accepts.
  An event object is built only when a registered hook handles its event.

```php
// 7.x
$database->on(Database::EVENT_DOCUMENT_CREATE, 'calculate-usage', function (mixed $document) {
    // ...
});

// 8.0
final class Usage implements Lifecycle, Named, Selective
{
    public function getName(): string
    {
        return 'calculate-usage';
    }

    public function handles(Event $event): bool
    {
        return $event === Event::DocumentCreate;
    }

    public function handle(Domain $event): void
    {
        if ($event instanceof Event\Document\Created) {
            // $event->collection, $event->document
        }
    }
}

$database->addHook(new Usage());
```

On a `Mirror`, lifecycle hooks are registered on the source database. `Mirror::silent()` silences both the source
and the mirror.

### Event classes

Each event's payload is a typed property; 7.x passed arrays, strings, `false` and `Document(['modified' => N])`.

| Event | Class | Properties |
|---|---|---|
| `database_create`, `database_update`, `database_delete`, `database_list` | `Event\Database\Created`, `Updated`, `Deleted`, `Listed` | `database`; `new` (update); `deleted` (delete); `databases` (list) |
| `collection_create`, `collection_read`, `collection_update`, `collection_delete` | `Event\Collection\Created`, `Read`, `Updated`, `Deleted` | `collection`, `definition` (`Collection`) |
| `collection_list` | `Event\Collection\Listed` | `collections` |
| `attribute_create`, `attribute_update`, `attribute_delete` | `Event\Attribute\Created`, `Updated`, `Deleted` | `collection`, `attribute` (`Attribute`) |
| `attribute_rename` | `Event\Attribute\Renamed` | `collection`, `old`, `attribute` |
| `attributes_create` | `Event\Attribute\BatchCreated` | `collection`, `attributes` |
| `index_create`, `index_delete` | `Event\Index\Created`, `Deleted` | `collection`, `index` (`Index`) |
| `index_rename` | `Event\Index\Renamed` | `collection`, `old`, `index` |
| `indexes_create` | `Event\Index\BatchCreated` | `collection`, `indexes` |
| `document_create`, `document_read`, `document_update`, `document_delete` | `Event\Document\Created`, `Read`, `Updated`, `Deleted` | `collection`, `document` |
| `document_upsert` | `Event\Document\Upserted` | `collection`, `document`, `created` |
| `document_increase`, `document_decrease` | `Event\Document\Increased`, `Decreased` | `collection`, `document`, `attribute` |
| `documents_create`, `documents_update`, `documents_delete` | `Event\Document\BatchCreated`, `BatchUpdated`, `BatchDeleted` | `collection`, `count` |
| `documents_upsert` | `Event\Document\BatchUpserted` | `collection`, `count`, `created`, `updated` |
| `document_find` | `Event\Document\Found` | `collection`, `documents` |
| `document_aggregate` | `Event\Document\Aggregated` | `collection`, `rows` |
| `document_count`, `document_sum` | `Event\Document\Counted`, `Summed` | `collection`, `count`; `attribute`, `sum` |
| `document_purge` | `Event\Document\Purged` | `collection`, `id` |
| `permissions_create`, `permissions_read`, `permissions_delete` | `Event\Permission\Created`, `Read`, `Deleted` | `collection`, `document`, `permissions` |

`Event::domain()` names the class of a case. New events: `upsertDocument()` fires `document_upsert` (not
`documents_upsert`), `renameAttribute()` fires `attribute_rename` (it fired `attribute_update`), `createIndexes()`
fires `indexes_create` after each `index_create`, `aggregate()` fires `document_aggregate` (not `document_find`),
and `update()` fires `database_update`. `findOne()` and `findCollection()` fire nothing on a miss; 7.x's `findOne()`
fired `document_find` with `false`.

### `silent()`

`silent(callable $callback, ?array $hooks = null)`:

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
`deleteDocument()` and `deleteDocuments()`, and from `purgeCachedDocument()`. The event is
`Event\Document\Purged` with the document's `collection` and `id`. As in 7.x, `createDocument()` and `createDocuments()` do
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
- Telling a cascade's survivors from what it removed takes a read per related collection. A delete makes those reads
  only while an active lifecycle hook handles `document_update`: any hook that does not implement `Hook\Selective`,
  or one whose `handles(Event::DocumentUpdate)` is `true` (for `Event\DispatcherHook`, a listener for
  `Event\Document\Updated` or a PSR-14 dispatcher). Implement `Hook\Selective` on a hook that ignores
  `document_update` to spare its deletes those reads.
- They are read and written with permissions skipped and are not checked against the caller's read permission:
  treat them as privileged, like the documents `deleteDocuments()` and `upsertDocuments()` pass to `$onNext`.
- They fire when the delete's own transaction returns, like `document_delete`, not when an outer transaction
  commits. A delete whose commit fails and is retried reports only what the committed attempt changed.
- Difference from 7.4.0: when a hook throws, `document_delete` and every related `document_update` still fire, and
  the first exception reaches the caller afterwards. 7.4.0 stopped at the first failure.

### `attribute_create` from `createAttributes()`

`createAttributes()` fires `attribute_create` (`Event\Attribute\Created`) once per attribute, then
`attributes_create` (`Event\Attribute\BatchCreated`) once, with the list. In 7.x, `createAttributes()` fired
`attribute_create` once, with the array of attribute documents as payload, and nothing fired `attributes_create`.

### `Event\DispatcherHook`

`Event\DispatcherHook` forwards the typed events to listeners registered per class with
`on(string $eventClass, callable $listener)` and to an optional PSR-14 dispatcher. It handles an event only while a
listener or the dispatcher can receive it.

```php
$dispatcher = new DispatcherHook();
$dispatcher->on(Event\Document\Created::class, function (Event\Document\Created $event): void {
    // $event->collection, $event->document
});
$database->addHook($dispatcher);
```

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

### Write hooks

A `Hook\Write` stands alone; it does not extend `Utopia\Query\Hook\Write`. `Hook\Interceptor` implements every
method as a no-op, so a hook overrides only what it needs:

- `decorateRow(array $row, Hook\RowMetadata $metadata): array` adjusts each row before it is written. `$metadata->tenant`
  is the document's tenant, else the adapter's.
- `afterDocumentCreate()`, `afterDocumentUpdate()`, `afterDocumentBatchUpdate()`, `afterDocumentUpsert()` and
  `afterDocumentDelete()` take a `Hook\WriteContext` as their last argument. `afterDocumentUpdate()` receives the id
  the document is stored under.
- `Hook\WriteContext` is an interface the SQL adapters implement: `builder(string $table)`, `rawBuilder()`,
  `rawTable(string $table)`, `run(Statement $statement, Event $event): bool`, `fetch(Statement $statement, Event
  $event): array`, `decorateRow(array $row, Document $document)`, `skipPermissions(Document $document): bool` (the
  update keeps that document's permissions) and `ignoreDuplicates()`.

### Subclasses of `Database`

- The protected `trigger(string $event, mixed $args = null)` is removed. Event dispatch (`listens()`, `dispatch()`)
  is internal: register a `Hook\Lifecycle` to act on events.
- The protected `createDocumentInstance()` is `newDocument()`.
- `casting()` and `applySelectFiltersToDocuments()` are internal, and so are `getRelationshipHook()` and the public
  helpers of `Hook\Relationships` that `Database` calls.
- `increaseDocumentAttribute()` and `decreaseDocumentAttribute()` accept numeric strings as well
  (`string|int|float`), for unsigned 64-bit values, so an override has to widen its parameter types.
- `Database` is now composed of the traits in `Utopia\Database\Trait`. Its public methods are unchanged by that.
- The protected `$listeners` and `$silentListeners` properties are gone. Registered lifecycle hooks are in the
  protected `$lifecycleHooks`; to silence or test for silence, use `silent()` and the protected
  `areEventsSilenced()`.
- The protected `decodeAttribute()` applies the filter it is given. `decode()` reads `setFiltering()` and
  `skipFilters()` once per document and calls it only for the filters they leave enabled, so an override that relied
  on the method returning the value unchanged while filters are disabled no longer has to check.

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
- **`createDocuments()` under `ignoreDuplicates()`** (7.x `skipDuplicates()`) returns and counts only the documents it inserted, and hands only
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
    compared with exact integer arithmetic, so 64-bit and unsigned values never pass through a float. The bound
    accepts the same whole numbers as an operator limit: an integer, an integer string, a string with only zero
    decimals such as `'102.0'`, or a float without a fractional part such as `102.0`, each converted exactly. Pass
    a whole bound, for example `floor($max)` or `ceil($min)`, which admits the same integer values.

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
- **`Document` methods.**

  | 7.x | 8.0 |
  |---|---|
  | `getRead()`, `getCreate()`, `getUpdate()`, `getDelete()`, `getWrite()` | `getPermissionsByType(PermissionType::Read)` and so on. `Write` covers create, update and delete |
  | `getArrayCopy(array $allow = [], array $disallow = [])` | `getArrayCopy()` without arguments, `only(array $keys)` for `$allow` and `except(array $keys)` for `$disallow` |
  | `find()`, `findAndReplace()`, `findAndRemove()` | No replacement: read and write the attribute with `getAttribute()` and `setAttribute()` |

  `only()` and `except()` filter top-level keys only, as `$allow` and `$disallow` did, and convert nested documents
  to arrays like `getArrayCopy()`; `only([])` returns `[]`. `Document::fromRow()` and `fromStorage()` are internal.

```php
// 7.x
$readers = $document->getRead();
$response = $document->getArrayCopy(disallow: ['$permissions']);

// 8.0
$readers = $document->getPermissionsByType(PermissionType::Read);
$response = $document->except(['$permissions']);
```

## Bulk writes and reads

| 7.x | 8.0 |
|---|---|
| `createDocuments($collection, $documents, $batchSize = INSERT_BATCH_SIZE, $onNext, $onError)` | `createDocuments(string $collection, array $documents, int $batchSize = Database::BATCH_SIZE, ?callable $onNext = null): int` |
| `updateDocuments($collection, $updates, $queries, $batchSize, $onNext, $onError)` | `updateDocuments(string $collection, Document $updates, array $queries = [], int $batchSize = Database::BATCH_SIZE, ?callable $onNext = null): int` |
| `upsertDocuments($collection, $documents, $batchSize, $onNext, $onError)` | `upsertDocuments(string $collection, array $documents, int $batchSize = Database::BATCH_SIZE, ?callable $onNext = null, ?string $increase = null): int` |
| `upsertDocumentsWithIncrease($collection, $attribute, $documents, $onNext, $onError, $batchSize)` | `upsertDocuments(..., increase: $attribute)` |
| `deleteDocuments($collection, $queries, $batchSize = DELETE_BATCH_SIZE, $onNext, $onError)` | `deleteDocuments(string $collection, array $queries = [], int $batchSize = Database::BATCH_SIZE, ?callable $onNext = null): int` |
| `iterate($collection, $queries, $forPermission)`, `foreach($collection, $callback, $queries, $forPermission)` | `cursor(string $collection, array $queries = [], int $batchSize = Database::CURSOR_BATCH_SIZE, PermissionType $forPermission = PermissionType::Read): Generator` |
| `find()` with aggregates or `groupBy()` | `aggregate(string $collection, array $queries): array`, a list of rows |
| `purgeCachedDocument(string $collectionId, ?string $id): bool`, `purgeCachedCollection(string $collectionId): bool` | `purgeCachedDocument(string $collection, string $id): void`, `purgeCachedCollection(string $collection): void` |

```php
// 7.x
$database->deleteDocuments('sessions', [$expired], onNext: fn (Document $deleted, Document $old) => $log($old), onError: fn (Throwable $error) => $report($error));
foreach ($database->iterate('sessions', [Query::limit(25)]) as $session) {
    // ...
}

// 8.0
$database->deleteDocuments('sessions', [$expired], onNext: fn (Document $deleted, ?Document $previous) => $log($previous));
foreach ($database->cursor('sessions', batchSize: 25) as $session) {
    // ...
}
```

- **`$onNext`** has one shape everywhere: `callable(Document $document, ?Document $previous): void`. `$previous` is
  `null` on create, the stored document an update or upsert replaced, and on delete a separate copy of the deleted
  document, so changing the first argument does not change it. `$onError` is removed: an exception from `$onNext` aborts the call, like any
  other callback.
- **Batch size.** `Database::BATCH_SIZE` (1000) replaces `INSERT_BATCH_SIZE` and `DELETE_BATCH_SIZE`. A batch size
  above it throws `Exception\Limit` instead of being reduced to it; one below 1 still writes one document at a time.
- **`cursor()`** is the only generator. It reads the matches in batches of `$batchSize` (default
  `Database::CURSOR_BATCH_SIZE`, 100). A `limit()` in the queries caps how many documents it yields; an `offset()`
  or `cursorAfter()` positions the first batch only; `cursorBefore()` throws `Utopia\Database\Exception`
  (`Cursor before not supported in this method.`) when called. In `iterate()` and `foreach()` a `limit()` set the
  page size, and without one they read pages of 25: move the limit to `batchSize`, or pass `batchSize: 25`, to keep
  the page size.
- **`aggregate()`** returns `list<array<string, mixed>>`. An unaliased aggregate comes back under
  `<method>_<attribute>`, or `<method>` for `count('*')`; give it an alias to choose the name. Two aggregates that
  would come back under one name (two identical unaliased ones, or a default that equals another's alias) throw
  `Exception\Query`. It fires `document_aggregate`. `find()` refuses aggregate and `groupBy()` queries with `Exception\Query`.
- **`sum()` and `count()`** throw `Exception\Query` for a `$max` of 0 or less; they returned 0.
- **`increaseDocumentAttribute()` and `decreaseDocumentAttribute()`** throw `Exception\Type` for a change value of 0
  or less; they threw `InvalidArgumentException`.
- **`findOne()`** fires no `document_find` on a miss.
- **`deleteDocument()`** still returns `bool`.

## Configuration toggles

Every toggle has one form: `set<Noun>(bool): static` sets it, `is<Participle>()` or `has<Noun>()` reads it, and a
scoped form takes the value and a callback. Setters return `static` and `reset`/`clear` methods return `void`.

| 7.x | 8.0 |
|---|---|
| `enableValidation()`, `disableValidation()` | `setValidation(bool $validation)`; read with `isValidating()`; scope with `withValidation(bool $validation, callable $callback)` or `skipValidation(callable $callback)` |
| `enableFilters()`, `disableFilters()` | `setFiltering(bool $filtering)`; read with `isFiltering()`; scope with `withFiltering(bool $filtering, callable $callback, ?array $filters = null)` or `skipFilters(callable $callback, ?array $filters = null)` |
| `enableLocks(bool $enabled)` | `setLocks(bool $locks)` |
| `getPreserveDates()`, `withPreserveDates(callable $callback)` | `isPreservingDates()`, `withPreserveDates(bool $preserve, callable $callback)` |
| `getPreserveSequence()`, `withPreserveSequence(callable $callback)` | `isPreservingSequence()`, `withPreserveSequence(bool $preserve, callable $callback)` |
| `skipDuplicates(callable $callback)` (database and adapter) | `ignoreDuplicates(callable $callback)` |
| `getDropUnknownAttributes()` | `isDroppingUnknownAttributes()` |
| `getSharedTables()` (database and adapter) | `hasSharedTables()` |
| `getTenantPerDocument()` (database and adapter) | `isTenantPerDocument()` |
| `clearDocumentType(string $collection): static` | `clearDocumentType(string $collection): void` |
| `clearAllDocumentTypes(): static` | `clearDocumentTypes(): void`, which keeps the metadata collection's `Collection` type |
| `setMigrating(): self`, `setMaxQueryValues(): self`, `setAuthorization(): self` | Return `static` |
| `silent(callable $callback, ?array $listeners = null)` | `silent(callable $callback, ?array $hooks = null)` |
| `getKeywords()` | `$database->profile()->limits->keywords` |
| `getConnectionId(): string` | `getConnectionId(): ?string`, `null` on an adapter without `Feature\Connection` (Memory) |
| — | `getHostname(): ?string`. `ping()` returns `true` and `reconnect()` does nothing without `Feature\Connection` |
| Adapter `setDatabase()`, `setSharedTables()`, `setTenant()`, `setTenantPerDocument()` returned `bool` | Return `static` |
| Adapter `setDebug()`, `getDebug()`, `resetDebug()` | `setMetadata()`, `getMetadata()`, `resetMetadata()`, which also reach the query comments |
| Adapter `enableAlterLocks(bool $enable)` | `Database::setLocks(bool $locks)` |
| `Authorization::setDefaultStatus(bool $status)` | `new Authorization(defaultStatus: $status)` |
| `Authorization::addRole()`, `removeRole()`, `cleanRoles()`, `setStatus()`, `enable()`, `disable()` returned `void` | Return `static` |
| `new Authorization\Input(string $action, array $permissions)` | `new Authorization\Input(PermissionType $action, array $permissions)` |

`setProfiling(bool)` and `isProfiling()` turn the query profiler on and off (see
[Pools and profiling](#pools-and-profiling)).
`setTenant()` and `withTenant()` are unchanged.

```php
// 7.x
$database->disableValidation();
$database->withPreserveDates(fn () => $database->createDocument('logs', $log));
$database->skipDuplicates(fn () => $database->createDocuments('logs', $logs));

// 8.0
$database->setValidation(false);
$database->withPreserveDates(true, fn () => $database->createDocument('logs', $log));
$database->ignoreDuplicates(fn () => $database->createDocuments('logs', $logs));
```

## Coroutines

Under Swoole, several coroutines can share one `Database` and one `Authorization`. The scopes that 7.x applied to the
whole handle now apply to the calling coroutine and the coroutines it starts; sibling coroutines sharing the handle
or the `Authorization` do not see them:

- `Authorization::skip()` and `Authorization::withRoles()`;
- `silent()`, `skipRelationships()`, `skipRelationshipsExistCheck()`, `withFiltering()` and `skipFilters()`,
  `withValidation()` and `skipValidation()`, `withPreserveDates()`, `withPreserveSequence()`, `withTenant()`,
  `withRequestTimestamp()`, and `ignoreDuplicates()` on the database and on the adapter.

A plain setter is scoped only by a scope over the same state:

- `Authorization::setStatus()`, `enable()`, `disable()` and `reset()` by `Authorization::skip()`;
- `Authorization::addRole()`, `removeRole()` and `cleanRoles()` by `Authorization::withRoles()`;
- `setTenant()` by `withTenant()`;
- `setValidation()` by `withValidation()` and `skipValidation()`;
- `setFiltering()` by `withFiltering()` and `skipFilters()`, with or without filter names;
- `Hook\Relationships::setEnabled()` by `Hook\Relationships::withEnabled()` and `skipRelationships()`;
- `setPreserveDates()` by `withPreserveDates()`, and `setPreserveSequence()` by `withPreserveSequence()`.

Outside every scope over its state, a setter in the coroutine that opened a scope, or in a coroutine it started that
is still connected to it, changes the shared value, as in 7.x: `disable()` inside `withRoles()` turns authorization
off for every coroutine sharing the `Authorization`. A coroutine cut off from the scope is covered in the next
paragraph. Inside such a scope, whether the calling coroutine opened it or inherited it from the coroutine that
started it, a setter changes only what the calling coroutine and the coroutines it starts see, and only until the
scope ends. When the scope ends, the value is what it was before the scope, as in 7.x.

A coroutine sees a scope only while every coroutine between it and the scope's owner is still running, because
Swoole cannot report the parent of a coroutine that has finished. In a coroutine cut off this way:

- reads see the shared values, or a scope opened outside every coroutine, but never a scope opened in a coroutine
  (not the owner's tenant, status or roles), and on `Adapter\Pool` it borrows a connection of its own outside the
  scope's transaction;
- writes stay its own: while a scope over that state is open on the handle, even one another coroutine opened, a
  setter changes only what that coroutine and the coroutines it starts see, until it ends, and never the shared
  value. For `Authorization`, `skip()` and `withRoles()` each count as a scope over both the status
  and the roles;
- with no such scope open, a setter changes the shared value, as in 7.x.

A connected coroutine and a cut-off one treat a setter differently on purpose. A connected coroutine knows the scopes
it runs under, so a write to a state none of them covers follows the 7.x rule: a grandchild's `disable()` inside
`withRoles()` turns authorization off for every coroutine. A cut-off coroutine cannot tell which scope it ran under,
so it keeps the write local: the same `disable()` there applies only to it and the coroutines it starts.

A cut-off coroutine must not change and then restore state with a pair of setters, such as `disable()` then
`enable()`, or `setTenant($tenant)` then `setTenant($original)`. Each write is shared or local depending on whether a
scope over that state is open on the handle at that moment, so the restore can stay local while the change stays
shared. Use `skip()`, `withRoles()`, `withTenant()` or another `with*()` scope, which always restores the value
when it ends.

While a cut-off coroutine holding such a local write is alive, reads of that state take the slower scoped path in
every coroutine sharing the handle, as they do while any scope over it is open, so a long-lived coroutine should open
its own scopes around the work that needs them rather than call setters.

Work that can outlive the coroutine that started it loses that coroutine's scopes once it returns, so it opens the
scopes it needs itself, inside the child. `Hook\Relationships::withEnabled()` and `withCheckExist()` scope the hook's
own flags the same way as the database scopes.

`Database::snapshot()` returns the calling coroutine's state as a `State\Snapshot`: the authorization status and
roles, the relationship, silence and filter state, the tenant, the validation, preserve-dates, preserve-sequence and
ignore-duplicates toggles, and the request timestamp; a snapshot does not carry a transaction.
`Database::withSnapshot()` and `Hook\Relationships::withSnapshot()`, which run a callback under a snapshot, are
`@internal`: `Mirror` and the relationship hook use them to carry the caller's state into the coroutines they start.
They are not part of the public API.

Relationship population reads its chunks of related ids concurrently only on `Adapter\Pool`, inside a coroutine and
outside `withTransaction()`, and only as many at once as `Hook\Relationships::READ_CONCURRENCY` (4) and
`Pool::getReadConcurrency()` (the connections the pool can hand out without waiting, less one) allow; elsewhere it
reads them one after another. Related documents are merged in chunk order.

Each coroutine sharing a handle tracks its own relationship writes and cascading deletes, so a nested write or a
cascade in one coroutine never cuts another coroutine's short. On `Adapter\Pool`, a transaction belongs to the
coroutine that opened it and the coroutines it starts; see [Pools and profiling](#pools-and-profiling).

## Errors

- **Exception hierarchy.** `Exception\Schema` is the new parent of `Structure`, `Type`, `Character`, `Truncate`,
  `Index`, `Dependency`, `Limit` and `Relationship`, so one `catch (Exception\Schema)` handles every schema violation.
  `Order` and `Operator` now extend `Exception\Query`: a `catch (Exception\Query)` also catches them, so put a
  `catch` of `Order` or `Operator` before it. `Exception\Transaction` keeps exactly its subtree (`Contention`);
  `Timeout` and `Unconfirmed` keep their parents, so `withTransaction()` never runs them again.
- **Exception constructor.** `Exception::__construct(string $message = '', int|string $code = 0, ?\Throwable
  $previous = null)` takes every argument as optional and keeps a string code, such as the SQLSTATE `HY000`, in
  `public readonly ?string $state`; a numeric string is also the integer code. `Exception\Order::__construct(string
  $message, ?string $attribute = null, int|string $code = 0, ?\Throwable $previous = null)` takes the attribute
  second.
- **Unique index violations.** Every adapter now reports a unique index violation as
  `Utopia\Database\Exception\Unique` with the message `Document with the requested unique attributes already exists`
  (7.x: `Unique index violation`). The class and its hierarchy are unchanged: `Unique` extends `Duplicate`, and a
  conflicting document `$id` still throws a plain `Duplicate` with `Document already exists`. Match on the class,
  not the message: catch `Unique` before `Duplicate` to tell the two apart. `Exception\Unique` has no constructor of
  its own and never rewrites the message it is given. The message is `Exception\Unique::MESSAGE`.
- **`ignoreDuplicates()` on PostgreSQL** skips only a document whose id is stored, as in 7.x: a new id that collides
  on another unique index throws `Utopia\Database\Exception\Unique`. MariaDB, MySQL and SQLite cannot name the
  index to ignore and, as in 7.x, skip such a row without error.
- **Retries of metadata writes.** Schema calls that persist a collection definition (`createAttribute()`,
  `createIndex()`, their update, rename and delete siblings, `createRelationship()`) no longer retry a failure that
  fails the same way every time: `Authorization`, `Character`, `Conflict`, `Dependency`, `Duplicate` (and `Unique`
  and `Mismatch`), `Index`, `Limit`, `NotFound`, `Operator`, `Order`, `Query`, `Relationship`, `Restricted`,
  `Structure`, `Timeout`, `Truncate`, `Type` and `Unconfirmed` are thrown on the first attempt. A failure the
  metadata write's transaction retries itself (see [Transaction retries](#errors)) is not run again by the schema
  call, so its retries do not multiply. Other failures, such as an unavailable cache, are still attempted up to three
  times.
- **Transaction retries.** `withTransaction()` runs the callback again only after a failure that can succeed on
  another attempt: an `Exception\Transaction` (a lock conflict, which is `Exception\Contention`, or a failed begin,
  commit or rollback), a lost connection (also as the cause of another failure), an engine lock conflict the adapter
  did not map, or, on MongoDB, an error labelled `TransientTransactionError`, a network error before the commit or a
  command that was never sent. Every other failure runs the callback once and is rethrown at once,
  in a nested call too: every typed library failure (`Structure`, `NotFound`, `Query`, `Type`, `Index`,
  `Dependency`, `Truncate`, `Duplicate`, `Timeout`, ...) and any other exception. 7.x retried everything but
  `Duplicate`, `Restricted`, `Authorization`, `Relationship`, `Conflict`, `Limit` and `Timeout` twice, sleeping 50 ms
  and 100 ms, before rethrowing the same failure. A callback that throws its own exception to have the transaction
  run again must throw `Exception\Transaction` (or `Exception\Contention`) instead. Only the outermost
  `withTransaction()` retries: a call nested in another rolls back to its savepoint and rethrows, and the outermost
  call runs the whole unit again, releasing its locks between attempts. A lock conflict that keeps failing therefore
  runs a nested callback 3 times, not 9 (7.x retried in the savepoint too). A call nested in a transaction begun
  with `startTransaction()` still retries in its savepoint.
- **Commits with an unknown result.** On MongoDB, when the result of a commit is unknown, `withTransaction()` sends
  only the commit again, up to 3 more times, with a `majority` write concern and a 10 s `wtimeout`, and does not run
  the callback again. Each retry can wait up to that 10 s while it holds the connection. The result is unknown after
  a network error, a primary change or shutdown, `MaxTimeMSExpired` (50) or `ExceededTimeLimit` (262) during the
  commit, or an error labelled `UnknownTransactionCommitResult`, so codes 50 and 262 at commit no longer surface as
  `Exception\Timeout`. After a socket timeout or send failure the client drops the connection with its sessions, so
  the commit cannot be sent again and `Unconfirmed` is thrown after a 50 ms pause, when the first retry finds the
  session gone: only a commit that failed with a primary change, shutdown, time limit or label is sent again, and a
  retry the client could not send is tried once more. If the commit still cannot be confirmed, `withTransaction()`
  throws `Utopia\Database\Exception\Unconfirmed`, whose `getPrevious()` is the first commit error, and does not run
  the callback again: treat the work as possibly committed and re-read before acting on it. The queued
  `document_purge` events of its last attempt still fire, because its writes may be stored. 7.x ran the whole
  callback again, which could store its writes twice. A schema call whose definition write ends in `Unconfirmed`
  rethrows it unchanged and keeps the table, column or index, as after a failure once the definition is stored (see
  below). On a sharded cluster (`mongos`) the adapter runs without transactions, as on a standalone server.
- **Commits the server reports aborted.** On MongoDB, a commit that the server reports aborted (`NoSuchTransaction`
  (251) or `WriteConflict` (112)) stored nothing, so `withTransaction()` runs the callback again, within its usual 2
  retries, whether it was the first commit or a retry; when the retries run out it throws `Utopia\Database\Exception`
  with an `Exception\Transaction` cause. 7.x reported a first commit the server had aborted as a success, so the
  callback's writes were lost while the call returned normally. MongoDB aborts the whole transaction on a failed
  write in it, so a callback that catches a failed write (a `Duplicate`, for example) and carries on now runs again
  and then fails, where 7.x returned without storing any of its writes. On the SQL adapters a connection lost
  during `COMMIT` still runs the callback again: the work is at-least-once on a SQL connection loss during `COMMIT`.
- **Failed rollbacks of metadata writes.** When a definition cannot be persisted and the schema change's rollback
  fails too, the thrown `Utopia\Database\Exception` names the persistence error first and the rollback's after
  `| Cleanup error:`, and its `getPrevious()` is always the persistence error. In 7.x the message labelled the two the
  other way round, and some calls reported only the rollback's error.
- **Failures after a schema change committed.** When a definition is stored and only the cache invalidation or
  events after it fail, or its commit ends in `Exception\Unconfirmed`, the schema call rethrows that failure
  unchanged, does not retry it and does not undo the physical change: `createCollection()`, `createAttribute()`,
  `createAttributes()` and `createIndex()` keep the table, column or index, `updateAttribute()`,
  `renameAttribute()` and `renameIndex()` keep the change, and `deleteAttribute()` and `deleteIndex()` leave the
  column or index dropped. `createRelationship()` also completes the relationship's indexes before rethrowing it.
- **Deletes whose definition may be stored.** `deleteCollection()` and `deleteRelationship()` drop the table or
  columns before they remove the definition. When that removal fails, also after its commit or with an
  `Exception\Unconfirmed` commit, they recreate the table or columns empty and throw a `Utopia\Database\Exception`
  whose `getPrevious()` is the failure. If the removal was stored, this leaves an empty table or column without a
  definition: `createRelationship()` with the same key reuses the columns, and on MongoDB `createCollection()` with
  the same id reuses the collection as it is, while on the SQL adapters it throws `Exception\Duplicate` unless
  tables are shared. The library does not run `deleteCollection()` again, and the wrapper no longer has the
  `Unconfirmed` class, so a caller that retries on it runs the delete again: harmless when the removal was not
  stored, and `Exception\NotFound` when it was.
- **Engine errors mapped to library exceptions.**

  | Engine condition | Exception |
  |---|---|
  | MariaDB/MySQL deadlock (1213) or lock wait timeout (1205); PostgreSQL deadlock (40P01), serialization failure (40001) or lock not available (55P03); SQLite `database is locked` (5) | `Exception\Contention`, a subclass of `Exception\Transaction`, which `withTransaction()` retries twice before rethrowing |
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
  inherited them from MariaDB, ignored them. `getConnectionId()` and `getHostname()` return `null` on an adapter
  without `Feature\Connection` (Memory). `schema()` throws `Schema builder is not supported by this adapter`
  where `from()` throws for the query builder. `getSchemaAttributes()` and `getSchemaIndexes()` return `[]`
  without `Capability::SchemaIntrospection`.
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
  expires after the cache TTL (`Database::TTL`, 24 hours). Deploy without overlap, or run both sides without a cache
  during the overlap (for example with `Utopia\Cache\Adapter\None`; a write from an uncached side cannot invalidate
  the other side's entries), and flush the cache once the last 7.x process has stopped.
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
- **Query cache layout.** `find()` results are cached in one hash per collection scope. Each query and role context
  maps to one of `slots` fields (`new Cache\Query($cache, slots: $count)`, 1024 by default), and the value records the
  query and the epoch it was filled under, so a hit needs both to match. An invalidation publishes a new epoch and
  deletes nothing: an older result stays until a fill of its slot replaces it. The number of keys and fields no
  longer grows with writes or with distinct queries, and a write's cost does not depend on how many results are
  cached. A collection scope keeps at most `slots` results, so its memory is bounded by `slots` times its largest
  result, and queries sharing a slot evict each other. All of a collection's results share one key: under Redis
  Cluster they live on one slot, and an invalidation rejects every in-flight fill of that collection. On a cache
  without fields (Memory, Filesystem) a collection scope holds one result at a time.
- **Abandoned writes.** A write that blocks a collection's cache and never finishes its invalidation (a worker killed
  mid-transaction) no longer keeps that cache off until a flush. For the document cache the limit is
  `$database->setCacheWriterTimeout($seconds)`, 3600 seconds by default. The query cache uses the writer timeout
  and the cache name of the `Database` that calls it, so one query cache can be shared by several handles. Reads resume once the unfinished write is older than the
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
- **`delete()` flushes the whole cache.** `Database::delete()` flushes the cache it was given, as in 7.x. On a cache
  shared with other databases or applications (one Redis), that removes their entries too: give each database
  handle a cache of its own, or delete databases through `update()` and a cleanup of your own where that matters.
- **Cache keys.** `getCacheKeys()`, `getCacheBaseKeys()` and `getQueryCacheKey()` stay public.
  `getQueryCacheField()` is internal.
- **Filters that belong to one handle** go in the `Database` constructor's `$filters` or on a `Filter\Registry` (see
  [Filters](#filters)).

## Filters

`Database::addFilter(string $name, callable $encode, callable $decode)` still registers a filter for every handle in
the process; its callbacks receive the value, the document and the database. A filter that belongs to one handle is
a `Utopia\Database\Filter\Codec` (`name()`, `encode(mixed $value)`, `decode(mixed $value)`), which receives only the
value; 7.x passed constructor filters the document and the database as well. `Filter\Callback` builds one from two
closures.

| 7.x | 8.0 |
|---|---|
| `new Database($adapter, $cache, ['name' => ['encode' => $encode, 'decode' => $decode]])` | `new Database($adapter, $cache, [new Filter\Callback('name', $encode, $decode)])`, a list of `Filter\Codec` |
| `getInstanceFilters()` | The codecs given to the constructor, or `getFilters(): Filter\Registry` |

```php
// 7.x
$database = new Database($adapter, $cache, [
    'trim' => [
        'encode' => fn (mixed $value) => \trim($value),
        'decode' => fn (mixed $value) => $value,
    ],
]);

// 8.0
$database = new Database($adapter, $cache, [
    new Filter\Callback('trim', fn (mixed $value) => \trim($value), fn (mixed $value) => $value),
]);
```

- `setFilters(Filter\Registry $filters): static` gives a handle a registry of codecs, which several handles can
  share; `Filter\Registry::register(Filter\Codec $codec): static`, `get(string $name)` and `has(string $name)` manage
  it. A codec takes precedence over a global filter of the same name, and the constructor's codecs over both.
- A codec named after a built-in filter (`Utopia\Database\Filter`: `json`, `datetime`, `point`, `linestring`,
  `polygon`, `vector`, `object`) throws `Exception\Duplicate`. A codec that needs the document or the database is a
  global filter: register it with `addFilter()`.
- Cache keys tell codecs apart by class. A codec class whose instances encode differently implements
  `Filter\Signed` (`signature(): string`) so each instance gets its own key; `Filter\Callback` does.
- List a filter in an attribute's `filters`, by name or as a `Filter` case.

## Adapters

This section matters if you check adapter capabilities, read adapter limits, subclass an adapter or write your own.
[docs/add-new-adapter.md](docs/add-new-adapter.md) describes the contract a new adapter implements.

### Capabilities and features

The 51 `getSupportFor*()` methods of `Adapter` are removed. A behaviour flag is a `Utopia\Database\Capability` case
checked with `$adapter->supports(Capability::X)`. A group of methods an adapter may or may not implement is an
optional `Utopia\Database\Adapter\Feature\*` interface, checked with `$adapter->hasFeature(Feature\X::class)`. Use
`hasFeature()`, never `instanceof`: `Adapter\Pool` forwards the optional feature methods to the adapter it borrows
without implementing their interfaces, so only `hasFeature()` answers correctly for a pooled adapter.
`$database->profile()` answers both without asking the adapter again (see [Limits and profile](#limits-and-profile)).

| 7.x | 8.0 |
|---|---|
| `getSupportForAlterLocks()` | `supports(Capability::AlterLock)` |
| `getSupportForAttributeResizing()` | `supports(Capability::AttributeResizing)` |
| `getSupportForAttributes()` | `supports(Capability::DefinedAttributes)` |
| `getSupportForCaching()` | `supports(Capability::Caching)` |
| `getSupportForCastIndexArray()` | `supports(Capability::IndexArrayCast)` |
| `getSupportForCasting()` | `! hasFeature(Feature\Casting::class)`: the library casts the values of every adapter that does not implement `Feature\Casting` |
| `getSupportForFulltextIndex()` | `supports(Capability::IndexFulltext)` |
| `getSupportForFulltextWildcardIndex()` | `supports(Capability::IndexFulltextWildcard)` |
| `getSupportForGetConnectionId()`, `getSupportForHostname()`, `getSupportForReconnection()` | `hasFeature(Feature\Connection::class)` |
| `getSupportForIdenticalIndexes()` | `supports(Capability::IndexIdentical)` |
| `getSupportForIndex()` | `supports(Capability::IndexKey)` |
| `getSupportForIndexArray()` | `supports(Capability::IndexArray)` |
| `getSupportForIntegerBooleans()` | `supports(Capability::IntegerBooleans)` |
| `getSupportForInternalCasting()`, `getSupportForUTCCasting()` | `hasFeature(Feature\Casting::class)` |
| `getSupportForMultipleFulltextIndexes()` | `supports(Capability::IndexFulltextMultiple)` |
| `getSupportForNestedTransactions()` | `supports(Capability::TransactionNested)` |
| `getSupportForObject()` | `supports(Capability::Objects)` |
| `getSupportForObjectIndexes()` | `supports(Capability::IndexObject)` |
| `getSupportForOperators()` | `supports(Capability::Operators)` |
| `getSupportForOrderRandom()` | `supports(Capability::OrderRandom)` |
| `getSupportForRelationships()` | `hasFeature(Feature\Relationships::class)` |
| `getSupportForSchemaAttributes()`, `getSupportForSchemaIndexes()` | `supports(Capability::SchemaIntrospection)` |
| `getSupportForSchemas()` | `supports(Capability::Schemas)` |
| `getSupportForSpatialAttributes()` | `hasFeature(Feature\Spatial::class)` |
| `getSupportForSpatialAxisOrder()` | `supports(Capability::SpatialAxisOrder)` |
| `getSupportForSpatialIndexNull()` | `supports(Capability::IndexSpatialNull)` |
| `getSupportForSpatialIndexOrder()` | `supports(Capability::IndexSpatialOrder)` |
| `getSupportForTTLIndexes()` | `supports(Capability::IndexTtl)` |
| `getSupportForTimeouts()` | `hasFeature(Feature\Timeouts::class)` |
| `getSupportForTransactionRetries()` | `supports(Capability::TransactionRetries)` |
| `getSupportForTrigramIndex()` | `supports(Capability::IndexTrigram)` |
| `getSupportForUniqueIndex()` | `supports(Capability::IndexUnique)` |
| `getSupportForUnsignedBigInt()` | `supports(Capability::UnsignedBigInt)` |
| `getSupportForUpdateLock()` | `supports(Capability::UpdateLock)` |
| `getSupportForUpsertOnUniqueIndex()` | `supports(Capability::UpsertOnUniqueIndex)` |
| `getSupportForUpserts()` | `hasFeature(Feature\Upserts::class)` |
| `getSupportForVectors()` | `supports(Capability::Vectors)` |
| `getSupportNonUtfCharacters()` | `supports(Capability::NonUtfCharacters)` |
| `getSupportForBatchCreateAttributes()`, `getSupportForBatchOperations()`, `getSupportForBoundaryInclusiveContains()`, `getSupportForCacheSkipOnFailure()`, `getSupportForDistanceBetweenMultiDimensionGeometryInMeters()`, `getSupportForJSONOverlaps()`, `getSupportForNumericCasting()`, `getSupportForOptionalSpatialAttributeWithExistingRows()`, `getSupportForPCRERegex()`, `getSupportForPOSIXRegex()`, `getSupportForQueryContains()`, `getSupportForRegex()` | No replacement: every adapter behaves the same way, or nothing branches on the answer |

`Capability::Joins` and `Capability::Aggregations` are new. `Capability::SchemaIntrospection` is declared by MariaDB,
MySQL, SQLite and PostgreSQL. Every case is declared by at least one adapter. To list what an adapter reports, call
`$adapter->capabilities()`.

The optional features:

| Interface | Methods | Implemented by |
|---|---|---|
| `Feature\Casting` | `castBefore(Document $collection, Document $document): Document`, `castAfter(Document $collection, array $documents): array`, `castDatetime(string $value): mixed` | MongoDB |
| `Feature\Connection` | `ping(): bool`, `reconnect(): void`, `id(): string`, `hostname(): string` | the SQL adapters, MongoDB, Redis |
| `Feature\QueryBuilder` | `builder(string $collection): Builder`, `schema(): Schema` | the SQL adapters |
| `Feature\RawQuery` | `rawQuery(string $query, array $bindings = []): array`, `rawMutation(string $query, array $bindings = []): int` | the SQL adapters |
| `Feature\Relationships` | `createRelationship(string $collection, Relationship $relationship): bool`, `updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update): bool`, `deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side): bool` | the SQL adapters, MongoDB, Memory, Redis |
| `Feature\Schemaless` | `setSchemaless(bool $schemaless): static`, `isSchemaless(): bool` | MongoDB |
| `Feature\Spatial` | `encode(mixed $value, ColumnType $type): string`, `decode(string $value, ColumnType $type): array` | MariaDB, MySQL, PostgreSQL |
| `Feature\Timeouts` | `setTimeout(int $milliseconds, Event $event = Event::All): void`, `clearTimeout(Event $event = Event::All): void`, `getTimeout(Event $event = Event::All): int` | MariaDB, MySQL, PostgreSQL, MongoDB, `Pool` |
| `Feature\Upserts` | `upsertDocument(Document $collection, Change $change): Document`, `upsertDocuments(Document $collection, array $changes, ?string $increase = null): array` | the SQL adapters, MongoDB, Redis |

`Change` is `final readonly` with public `old` and `new`; 7.x's `getOld()`, `setOld()`, `getNew()` and `setNew()` are
removed.

```php
// 7.x
if ($adapter->getSupportForFulltextIndex() && $adapter->getSupportForTimeouts()) {
    $adapter->setTimeout(5000);
}
$limit = $adapter->getLimitForAttributes();

// 8.0
if ($adapter->supports(Capability::IndexFulltext) && $adapter->hasFeature(Feature\Timeouts::class)) {
    $database->setTimeout(5000);
}
$limit = $adapter->limits()->attributes;
```

### Limits and profile

The 16 per-limit getters of `Adapter` are one `Adapter\Limits` value, `$adapter->limits()`:

| 7.x | `Limits` property |
|---|---|
| `getLimitForString()`, `getMaxVarcharLength()` | `string`, `varchar` |
| `getLimitForInt()`, `getLimitForBigInt()` | `integer`, `bigInteger` |
| `getLimitForAttributes()`, `getLimitForIndexes()` | `attributes`, `indexes` |
| `getCountOfDefaultAttributes()`, `getCountOfDefaultIndexes()` | `defaultAttributes`, `defaultIndexes` |
| `getMaxIndexLength()`, `getMaxUIDLength()`, `getDocumentSizeLimit()` | `indexLength`, `uidLength`, `documentSize` |
| `getMinDateTime()`, `getMaxDateTime()` | `minDateTime`, `maxDateTime` |
| `getIdAttributeType()` | `idType` (a `ColumnType`) |
| `getKeywords()`, `getInternalIndexesKeys()` | `keywords`, `internalIndexKeys` |

`Database` keeps `getLimitForAttributes()`, `getLimitForIndexes()`, `getMaxIndexLength()`, `getMaxVarcharLength()`,
`getMinDateTime()`, `getMaxDateTime()`, `getIdAttributeType()` and `getMaxUidLength()` (7.x adapters spelled it
`getMaxUIDLength()`), all read from `limits()`.

`$database->profile(): Adapter\Profile` snapshots the adapter's `limits`, `capabilities`, `features`, `sharedTables`
and `migrating` once per `Database`, and is rebuilt by `setSharedTables()`, `setMigrating()` and `setSchemaless()`.
Its `supports()` and `hasFeature()` answer without a call into the adapter, except
`supports(Capability::DefinedAttributes)`, which it asks the adapter every time, because a pooled connection's
schema mode can change. The validators take it (see [Validators and helpers](#validators-and-helpers)).

### Schemaless mode

`setSupportForAttributes(bool $support)` is removed from every adapter. Switch MongoDB's schemaless mode with
`$database->setSchemaless(! $support)` (`Feature\Schemaless`), which also rebuilds the profile;
`setSchemaless(true)` throws on an adapter that always enforces its attributes. Calling the adapter's
`setSchemaless()` directly leaves the `Database` profile stale. `Adapter\Pool` puts every borrowed connection in the
mode.

### Connection feature

- `ping()`, `reconnect()`, `getConnectionId()` and `getHostname()` are `Feature\Connection`'s `ping()`, `reconnect()`,
  `id()` and `hostname()`. Memory has no connection. `Database::ping()`, `reconnect()`, `getConnectionId()` and
  `getHostname()` forward them.
- SQLite and MongoDB report their handle's `spl_object_id()` as the connection id, which is unique only within the
  process; Redis reports `'0'`.
- The protected `SQL::getPDO()` is removed: `getDriver(): object` returns the PDO (or proxy). `getDriver()` returns
  the adapter itself on Memory and the client on Redis.
- `SQL::getPDOAttributes()` is removed. Pass PDO attributes yourself when you connect (`ATTR_ERRMODE =>
  ERRMODE_EXCEPTION`, `ATTR_DEFAULT_FETCH_MODE => FETCH_ASSOC`, `ATTR_EMULATE_PREPARES => true`,
  `ATTR_STRINGIFY_FETCHES => true`, and a timeout as you need).
- `Adapter::clearTimeouts()` is removed: `clearTimeout(Event::All)` clears every event.

### Writing or subclassing an adapter

- The abstract methods of `Adapter` are the whole mandatory contract. 8.0 pre-releases also declared them in six
  `Feature` interfaces (`Attributes`, `Collections`, `Databases`, `Documents`, `Indexes`, `Transactions`), which are
  removed. Report optional behaviour by overriding `capabilities()`, and implement the optional `Feature`
  interfaces your adapter supports.
- `Adapter\SQLite` now extends `Adapter\SQL` instead of `Adapter\MariaDB`. A check like
  `$adapter instanceof MariaDB` no longer matches SQLite: check capabilities and features instead. The MariaDB
  methods SQLite inherited in 7.x, such as `setTimeout()` and `getViolatedKey()`, are no longer available on it.
- Changed adapter signatures:

  | 7.x | 8.0 |
  |---|---|
  | `exists(string $database, ?string $collection = null): bool` | `exists(string $database): bool` and `collectionExists(string $database, string $collection): bool` |
  | — | `update(string $name, string $new): bool` (abstract; see [Renaming a database](#renaming-a-database)) |
  | `createCollection(string $name, array $attributes = [], array $indexes = [])` | `createCollection(string $collection, array $attributes = [], array $indexes = [])` with lists of `Attribute` and `Index` |
  | `deleteCollection(string $id)` | `deleteCollection(string $collection)` |
  | `createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false)` | `createAttribute(string $collection, Attribute $attribute)` |
  | `updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false)` | `updateAttribute(string $collection, string $key, Attribute $attribute)`; the target key is `$attribute->key` |
  | `deleteAttribute(string $collection, string $id)` | `deleteAttribute(string $collection, string $key)` |
  | `createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = [], array $collation = [], int $ttl = 1)` | `createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = [])` |
  | `deleteIndex(string $collection, string $id)` | `deleteIndex(string $collection, string $key)` |
  | `createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = '')` | `Feature\Relationships::createRelationship(string $collection, Relationship $relationship)` |
  | `updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null)` | `Feature\Relationships::updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, RelationshipUpdate $update)` |
  | `deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side)` | `Feature\Relationships::deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side)` |
  | `deleteDocument(string $collection, string $id)`, `deleteDocuments(string $collection, ...)`, `increaseDocumentAttribute(string $collection, ...)`, `getSequences(string $collection, ...)` | Take the collection `Document`, like every other document method |
  | `updateDocuments(Document $collection, Document $updates, array $documents)` | `updateDocuments(Document $collection, Document $updates, array $documents, array $skipPermissions = [])`: the ids whose permissions the update keeps |
  | `upsertDocuments(Document $collection, string $attribute, array $changes)` | `Feature\Upserts::upsertDocuments(Document $collection, array $changes, ?string $increase = null)`, plus `upsertDocument(Document $collection, Change $change)` |
  | `getSchemaAttributes(string $collection): array`, `getSchemaIndexes(string $collection): array` | Abstract, returning `list<Schema\Column>` and `list<Schema\Index>` |
  | `getColumnType(string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): string` | `getColumnType(Attribute $attribute): ?string` (abstract; `null` on non-SQL adapters) |
  | `castingBefore()`, `castingAfter(Document $collection, Document $document)`, `setUTCDatetime()` | `Feature\Casting::castBefore()`, `castAfter(Document $collection, array $documents)` over a page, `castDatetime()` |
  | `decodePoint()`, `decodeLinestring()`, `decodePolygon()` | `Feature\Spatial::encode()` and `decode(string $value, ColumnType $type)`; the per-shape decoders are protected |
  | `find(..., string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ)` | `find(..., CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read)`; `$orderTypes` holds `OrderDirection` cases |
  | `setTimeout(int $milliseconds, string $event = Database::EVENT_ALL)`, `clearTimeout(string $event)`, `getTimeout(): int` | `Feature\Timeouts`, with `Event $event = Event::All` on all three |
  | `increaseDocumentAttribute(..., int\|float $value, ..., int\|float\|null $min = null, int\|float\|null $max = null)` | Numbers may also be numeric strings (`string\|int\|float`), for unsigned 64-bit values |
  | `supports()` and the getters taking a `$name`/`$id` | Parameters named `$capability`, `$collection` and `$key` |
  | `SQL::__construct(mixed $pdo)` | `SQL::__construct(object $pdo)`: a `Utopia\Database\PDO`, a PDO-compatible proxy or a native `PDO` |
  | `SQL::deleteAttribute(string $collection, string $id, bool $array = false)` | `deleteAttribute(string $collection, string $key)` |
  | `SQL::execute(mixed $statement)` | `execute(mixed $statement, ?Event $event = null)`, on `Adapter\SQL` only |
  | `SQL::getSQLType(string $type, ...)` | `getSqlType()`, taking `ColumnType` cases |
  | `SQL::getOperatorSQL(string $column, Operator $operator, array &$binds)` | `getOperatorSql(string $column, Operator $operator, int &$bindIndex)` |
  | `setDatabase()`, `setSharedTables()`, `setTenant()`, `setTenantPerDocument()`: `bool` | Return `static` |

- Protected and public methods use camel-cased acronyms and full words. Rename your overrides:

  | 7.x | 8.0 |
  |---|---|
  | `getSQLTable()`, `getSQLTableRaw()` | `getTable()`, `getTableRaw()` |
  | `getSQLType()`, `getSQLIndex()`, `getSpatialSQLType()`, `getOperatorSQL()`, `getPDOType()`, `getSQLReadableDistance()`, `getFTS5Value()` | `getSqlType()`, `getSqlIndex()`, `getSpatialSqlType()` (protected on MySQL too), `getOperatorSql()`, `getPdoType()`, `getSqlReadableDistance()`, `getFts5Value()` |
  | `convertArrayToWKT()`, `isExtendedISODatetime()`, `convertUTCDateToString()` | `convertArrayToWkt()`, `isExtendedIsoDatetime(string $value)`, `convertUtcDateToString()` |
  | `bindOperatorParams()`, `getIdentifierQuoteChar()` | `bindOperatorParameters()`, `getIdentifierQuote()` |
  | `getSpatialGeomFromText()`, `getSpatialAxisOrderSpec()` | `getSpatialGeometryFromText()`, `getSpatialAxisOrder()` |
  | `replaceChars()` | `replaceCharacters()` |
  | `Redis::tx()` | `Redis::transaction()` |
  | `getLockType()` (SQL), `listCollections()` and `getTenantFilters()` (MongoDB) | Protected |
  | Parameters `$stmt`, `$fn`, `$op`, `$attrs`, `$val`, `$quoteChar` | `$statement`, `$callback`, `$operation`, `$attributes`, `$value`, `$quoteCharacter` |

- `quote()` and `execute()` are declared on `Adapter\SQL` only. Keywords and internal index keys are `Limits` fields.
- `Adapter::relaxAttributeRequired(string $collection, string $id): bool` is new. The library calls it when an
  attribute becomes optional without a column change; it does nothing by default, and PostgreSQL drops the column's
  `NOT NULL` there.
- `SQL::getSpatialColumnSrid(): ?int` (protected) is new. It returns the SRID written into spatial column
  definitions, or `null` for a dialect that cannot declare one (MariaDB). `getSpatialSqlType()` follows it as
  well, so a dialect returning `null` declares spatial columns without an SRID on `createCollection()`,
  `createAttribute()`, `createAttributes()` and `updateAttribute()` alike.
- `SQL::insertOrIgnore(SQLBuilder $builder): Statement`, `SQL::supportsInsertReturning(): bool` and
  `SQL::documentKeyColumns(): array` (protected) are new. Under `ignoreDuplicates()` an adapter's `createDocuments()`
  must return only the documents it inserted; the SQL adapters learn them from `RETURNING`, or, where
  `supportsInsertReturning()` is false (MySQL), from reading the ids before and, when rows were skipped, after the
  insert.
- `SQL` declares `abstract protected function getColumnNames(string $collection): array` (the table's physical
  column names, empty when the table is missing). `SQL::renameAttribute()` and the engines' `updateAttribute()` use
  it to complete a rename another tenant of a shared table already ran.
- `SQL::getNullOrder(): OrderDirection` (protected) returns the direction in which the engine sorts null before
  every other value (`OrderDirection::Asc` by default; the PostgreSQL adapter returns `OrderDirection::Desc`). A SQL
  adapter for an engine that sorts nulls last in ascending order overrides it, or a cursor over a joined read skips
  or repeats rows holding null.
- Declare `Capability::TransactionNested` only when a failed nested transaction rolls back to its savepoint and
  leaves the enclosing transaction open. `Database` drops the `document_purge` events of a failed nested call only
  on such adapters; without it they fire with the enclosing commit.
- `Adapter::abandonTransaction(): void` (protected, a no-op by default) is new. The outermost `withTransaction()`
  calls it after every failed attempt, once the transaction counter no longer counts the attempt's transaction; an
  adapter whose connection can still hold part of it (a transaction lost with the connection, or one a failed
  rollback left open) ends it there. The SQL adapters roll back whatever the connection still reports.
- `Adapter::isIgnoringDuplicates(): bool` and `Database::isIgnoringDuplicates(): bool` (protected) report whether the
  calling coroutine runs under `ignoreDuplicates()`.
- `Utopia\Database\PDOStatement::getQueryString(): string` returns the wrapped statement's `queryString` without
  going through the magic `__get()`.
- `Adapter::withTenant($tenant, $callback)` scopes the tenant to the calling coroutine. `Database::withTenant()` uses
  it and no longer calls `setTenant()`, so an adapter that overrides `setTenant()` to react to tenant changes has to
  key such state by `getTenant()` instead.
- Write hooks and the tenant hook are registered by the library. `hasTenantHook()` and `hasPermissionHook()` are
  removed; `getTenantHook()` and `getWriteHooks()` are internal. `removeWriteHook()` also takes an instance.

### Methods an adapter no longer has

Methods that went with a feature exist only on the adapters that implement it, and the limit getters are gone from
every adapter. Besides the `getSupportFor*()` methods and the per-limit getters, these public 7.x methods are gone:

| Adapter | Removed public methods | Use instead |
|---|---|---|
| Every adapter | `getConnectionId()`, `getHostname()`, `ping()`, `reconnect()` on the base class | `Feature\Connection` (`id()`, `hostname()`, `ping()`, `reconnect()`) where implemented; the `Database` forwards |
| Every adapter | `setSupportForAttributes()`, `getSupportNonUtfCharacters()`, `clearTimeouts()`, `enableAlterLocks()`, `getKeywords()`, `getInternalIndexesKeys()`, `getTenantQuery()`, `before()`, `setDebug()`, `getDebug()`, `resetDebug()` | See the sections above |
| Every adapter | `getColumnType(string $type, ...)` | `getColumnType(Attribute $attribute)` |
| SQL adapters (MariaDB, MySQL, PostgreSQL, SQLite) | `castingBefore()`, `castingAfter()`, `setUTCDatetime()`, `decodePoint()`, `decodeLinestring()`, `decodePolygon()`, `getPDOAttributes()`, `getSpatialTypeFromWKT()`, `getLikeOperator()`, `getRegexOperator()`, `getSQLConditions()` | Library casting; `encode()`/`decode()`; your own PDO attributes; the query builders |
| MariaDB | `getSpatialSQLType()` (public in 7.4) | Protected `getSpatialSqlType()` |
| MySQL | `getSpatialSQLType()` (public) | Protected `getSpatialSqlType()` |
| SQLite | `setEmulateMySQL()`, `getEmulateMySQL()`, and the MariaDB methods it inherited (`setTimeout()`, `getViolatedKey()`, ...) | A subclass with `protected bool $emulateMySQL = true` |
| MongoDB | `getConnectionId()` is now `id()`; `getSchemaAttributes()` and `getSchemaIndexes()` return `[]`; `castingBefore()`, `castingAfter()`, `setUTCDatetime()`; `listCollections()` and `getTenantFilters()` (now protected); `setSupportForAttributes()` | `id()`; no introspection; `Feature\Casting`; `Database::listCollections()`; `setSchemaless()` |
| Memory | `getConnectionId()`, `ping()`, `reconnect()`, `getHostname()`, `upsertDocuments()` | None: Memory has no connection and no upserts (`Feature\Upserts`) |
| Memory, Redis | `castingBefore()`, `castingAfter()`, `setUTCDatetime()`, `decodePoint()`, `decodeLinestring()`, `decodePolygon()` | Library casting; no spatial attributes |
| Redis | `tx()` | `transaction()` |

### Pool extension points

`Adapter\Pool` subclasses override its protected extension points. These signatures are stable in 8.0; mark every
override `#[\Override]` so that a later rename fails loudly instead of being skipped. Renamed against 7.x and the
8.0 pre-releases:

| Before | 8.0 |
|---|---|
| `delegate(string $method, array $args): mixed` | `delegate(string $method, array $arguments): mixed` (public) |
| `delegateFeature(string $feature, string $method, array $args)` | `delegateFeature(string $feature, string $method, array $arguments)`; `$feature` names an optional feature |
| `borrowAndInvoke(string $method, array $args, ?string $feature = null)` | `borrowAndInvoke(string $method, array $arguments, ?string $feature = null)` |
| `invokeDelegated(Adapter $adapter, string $method, array $args, ?string $feature = null)` | `invokeDelegated(Adapter $adapter, string $method, array $arguments, ?string $feature = null)` |
| `syncBorrowedAdapter(Adapter $adapter)` | `syncBorrowed(Adapter $adapter)` |
| `releaseBorrowedAdapter(Adapter $adapter)` | `releaseBorrowed(Adapter $adapter)` |
| `getHostname(): string`, `ping(): bool` | `hostname(): string`, `ping(): bool` (`Feature\Connection` methods, which `Pool` forwards) |

`pin()`, `withTransaction()`, `inTransaction()` and `getReadConcurrency()` are unchanged. The protected
`Pool::$pinnedAdapter` property is removed (see [Pools and profiling](#pools-and-profiling)). `Pool` implements
`Feature\Timeouts` itself and forwards every other optional feature through `delegateFeature()`.

### Removed adapter methods

Queries now compile through the utopia-php/query builders (`builder()`, `createBuilder()`), transforms through
`Hook\Transform`, and tenant and permission conditions through the hooks in `Utopia\Database\Adapter\SQL\Hook` and
`Utopia\Database\Hook\Mongo`. The 7.x methods
behind the old string-building path are removed. Nothing in 8.0 calls them, so an adapter subclass that overrides or
calls one must drop the override or the call.

| Removed | Visibility in 7.x | Replacement |
|---|---|---|
| `SQL::getSQLConditions(array $queries, array &$binds, string $separator = 'AND', ?string $forCollection = null)` | public | The adapter's query builder (`builder()` / `createBuilder()`) |
| `SQL::getSQLConditionsForCollection()` | protected | The same |
| `getSQLCondition(Query $query, array &$binds, ?string $forCollection = null)` on `SQL` (abstract), `MariaDB`, `Postgres` and `SQLite` | protected | The same |
| `SQL::getSQLOperator()` | protected | The same |
| `handleSpatialQueries()` on `MariaDB` and `Postgres` | protected | The same |
| `handleDistanceSpatialQueries()` on `MariaDB`, `MySQL` and `Postgres` | protected | The same |
| `Postgres::handleObjectQueries()` | protected | The same |
| `SQLite::getLikeCondition()` | protected | The same. SQLite's `LIKE ... ESCAPE` lives in `Utopia\Database\Builder\SQLite` |
| `getSQLPermissionsCondition()` on `SQL` and `Postgres` | protected | The permission hooks in `Utopia\Database\Adapter\SQL\Hook\Permission` (`Filter`, `Join`, `OuterJoin`) |
| `getSQLVectorDistance()` on `SQL` and `Postgres` | protected | The query builder |
| `Adapter::getTenantQuery()` (abstract) and its implementations on `SQL`, `Memory`, `Redis`, `Pool` and `Mongo` | public | `Adapter\SQL\Hook\Tenant\Filter` (SQL) and `Hook\Mongo\Tenant` (MongoDB) |
| `getInsertKeyword()` on `SQL`, `Postgres` and `SQLite`, and `getInsertSuffix()` and `getInsertPermissionsSuffix()` on `SQL` and `Postgres` | protected | `SQL::insertOrIgnore(SQLBuilder $builder): Statement`, which `Postgres` overrides to name the id as the conflict target |
| `getUpsertStatement()` on `SQL` (abstract), `MariaDB`, `Postgres` and `SQLite` | protected, public on `MariaDB` and `SQLite` | The same |
| `SQL::registerOperatorBind()` | protected | `getOperatorSql()` binds operator values itself |
| `SQL::getFulltextValue()` and `Postgres::getFulltextValue()` | protected | utopia-php/query's builders normalize search terms (`compileSearchExpression()`) |
| `Adapter::before()` and `Adapter::trigger()` | public, protected | `Hook\Transform`, registered with `Database::addHook()` |
| `SQL::getLikeOperator()`, `SQL::getRegexOperator()`, `Postgres::getLikeOperator()` and `Postgres::getRegexOperator()` | public | The query builders emit `LIKE`/`ILIKE` and `REGEXP`/`~` |
| `Adapter::getAttributeProjection()` (abstract) and its implementations on `SQL`, `Memory`, `Redis` and `Pool` (`Mongo` keeps a private one) | protected | The query builders build the projection |
| `getRandomOrder()` on `SQL` (abstract), `MariaDB`, `Postgres` and `SQLite` | protected | The query builders' `compileRandom()` emits `RAND()`/`RANDOM()` for `Query::orderRandom()` |
| `SQL::getSpatialTypeFromWKT()` | public | None. The type is the text before the first `(` of the WKT, lower-cased |
| `getSQLIndexType()` on `SQL` and `SQLite` | protected | Each adapter's `createIndex()`, which builds the whole index statement |
| `Postgres::getSQLSchema()` | protected | `getTable()`, which qualifies the table with the schema |
| `Postgres::encodeArray()` and `Postgres::decodeArray()` | protected | None. Array attributes are `JSONB` columns |
| `Memory::unregisterRelationshipField()` | protected | None |

`escapeWildcards()` is kept, and so is the public helper `Query::isSpatialAttribute()`, even where nothing in the
library calls it any more.

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
  transaction is committed. A nested call that fails transiently while its enclosing transaction holds is rolled
  back to its savepoint and rethrown, and the outermost call runs again; a top-level call that failed to begin, or
  whose work failed transiently, is still retried (see [Transaction retries](#errors)). A
  top-level commit that finds the connection no longer holds the transaction (a reconnect the callback did not
  surface) throws `Exception\Transaction` too, and the callback is not run again: statements after such a reconnect
  may already have run on their own. Code that catches an expected exception (for example `Duplicate`) from a
  nested call and carries on no longer receives that exception when the transaction was lost underneath it: catch
  `Exception\Transaction` around the outermost call and run the whole unit again. A callback that catches the nested
  call's `Exception\Transaction` and returns fails the same way instead of reporting the lost work as committed. The
  outermost call ends what is left of the lost transaction on the connection before it throws, so the connection runs
  the next statement. A transaction the engine rolled
  back over a lock conflict is not lost: MariaDB and MySQL roll the whole transaction back, savepoints included, when
  a statement loses a deadlock (1213), or a lock wait timeout (1205) with `innodb_rollback_on_timeout`. The nested
  calls rethrow that `Exception\Contention` unchanged, and the outermost call runs again, as in 7.x, because nothing
  of the attempt is stored.
- **Statements after a lost transaction.** When `Utopia\Database\PDO` reconnects because a statement inside a
  transaction found the connection gone, it rethrows and then refuses every statement (`exec()`, `query()`,
  `prepare()`, `beginTransaction()`, `commit()`) with a `PDOException` until the transaction is ended with
  `rollBack()`, a `ROLLBACK` statement or `reconnect()`. A `rollBack()` that itself finds the connection gone ends the
  transaction too: it rethrows and refuses nothing after it. `inTransaction()` reports the transaction until then, and
  each refusal's `getPrevious()` is the lost connection error. Code that catches the connection error inside
  `withTransaction()` and carries on no longer writes on the new connection in autocommit: the transaction is rolled
  back and, at the top level, runs again.

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

`Adapter\Mongo::exists()` reports a database only when the server lists it; it returned `true` for every name.
`collectionExists()` looks in the database it is given.

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
- **Regex.** `Query::regex()` works on SQLite through the adapter's `REGEXP` function, which `Utopia\Database\PDO`
  and `Pdo\Sqlite` connections register; patterns are PCRE (`preg_match`, case-sensitive, `u` flag).
- **`getSchemaIndexes()` returns index ids.** Each `Schema\Index`'s `name` is the index id (`email`,
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
  `id()`, schema introspection and SQLite's transaction statements. 7.x annotated only statements that
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

- **`onError()`.** `onError(callable $callback): static` callbacks receive one `Mirror\Failure`, with the `method`
  that failed on the destination, its `event` (`?Event`: for example `upsertDocument` reports `DocumentUpsert`,
  `createIndexes` `IndexesCreate`, `renameAttribute` `AttributeRename` and `update` `DatabaseUpdate`) and the
  `error`. 7.x passed `(string $action, \Throwable $error)`.

  ```php
  // 7.x
  $mirror->onError(fn (string $action, \Throwable $error) => $logger->error($action, ['error' => $error]));

  // 8.0
  $mirror->onError(fn (Failure $failure) => $logger->error($failure->method, ['error' => $failure->error]));
  ```

- **Write filters** are `Utopia\Database\Mirror\Filter` (was `Mirroring\Filter`); its `init()` is `initialize()`.
- **Hooks.** `$mirror->addHook(new Relationships(...))` keeps the given hook and attaches a copy to the source and
  one to the destination, so its configuration (`prepare`, a subclass) reaches both. `removeHook()` also removes a
  hook from the source and the destination. `addLifecycleHook()` is protected.
- **Query cache.** Install the query cache through the mirror: `$mirror->setQueryCache($queryCache)` installs it on
  the source and the destination as well, so writes through the mirror invalidate the cache its reads use. An
  `Invalidator` added with `$mirror->addHook()` is installed on the mirror and the source.
- **Setters reach the wrapped databases.** Besides `setDatabase()`, `setNamespace()`, `setSharedTables()`,
  `setTenant()`, `setMaxQueryValues()`, `setCache()`, `setAuthorization()`, validation and the document-type
  setters, a mirror now forwards `setQueryCache()`, `setCacheName()`, `setGlobalCollections()`,
  `resetGlobalCollections()`, `setTenantPerDocument()`, `setCacheWriterTimeout()`, `setTimeout()`, `clearTimeout()`,
  `setMetadata()`, `resetMetadata()`, `setFiltering()`, `withFiltering()`, `skipFilters()`, `setLocks()`,
  `setProfiling()`, `setMigrating()` and `setFilters()` to its source and destination. In 7.x these changed the
  mirror alone.
- **Timeouts on the destination.** A destination that cannot apply a timeout (MariaDB applies it on the connection)
  is reported through `onError()` with the method `setTimeout` or `clearTimeout`; the call itself succeeds when the
  source applied it. `setLocks()` reports a destination failure the same way, with the method `setLocks`.
- **`create()`.** A destination that cannot create the database makes `create()` throw, after the source has
  created it, so a mirror never reports a database its destination lacks. Every other call forwarded to the
  destination reports a destination failure through `onError()` and returns the source's result.
- **Profiling.** `$mirror->setProfiling(true)` enables profiling on the source and the destination, and
  `$mirror->getProfiler()` returns the source's profiler, which records the mirror's queries, because both use the
  source's adapter. The destination's queries are recorded by `$mirror->getDestination()->getProfiler()`.
- **Scoped setters.** `withTenant()`, `withPreserveDates()`, `withPreserveSequence()`, `withValidation()`,
  `skipValidation()`, `withFiltering()` and `skipFilters()` called on a mirror open on the mirror, its source and its destination for the duration of the
  callback. `skipRelationships()`, `skipRelationshipsExistCheck()` and `withRequestTimestamp()` open on the mirror
  and its source only; the destination applies the source's resulting documents with preserved dates.
  `withRequestTimestamp()` now runs its callback once; 7.x ran it once per database when a destination was set.
- **Replication.** Inside a coroutine (a Swoole server), `createDocuments()`, `updateDocuments()`, the upserts,
  `deleteDocument()` and `deleteDocuments()` return once the source write is done and replicate in a coroutine of
  their own; a destination failure reaches `onError()` later. Every other call that reaches the destination
  (`createDocument()`, `updateDocument()`, `increaseDocumentAttribute()`, `decreaseDocumentAttribute()`, the schema
  changes, `create()`, `delete()`, `exists()`, `setTimeout()` and `clearTimeout()`) applies there before it returns,
  after the replications queued before it. Outside a coroutine all replication finishes before the call returns. In
  7.x every write replicated before returning.
  - Each replication runs under the state the caller had at the time of the call: authorization status and roles,
    tenant, relationship and silence state and the toggles (see [Coroutines](#coroutines)), without the request
    timestamp. A write made inside `skip()` replicates under it even after the caller has left the scope.
  - A mirror applies its changes to the destination one at a time, in the order they were made through it, whichever
    documents, related documents or schema they reach: a write never overtakes an earlier one, a schema change waits
    for every queued replication, and two replications never share the destination's connection, so the destination
    needs no `Adapter\Pool`. A failed replication is reported to `onError()` and does not hold back later ones. The
    order holds per mirror; writes through two mirrors over the same destination are not ordered with each other.
  - A write through the mirror made while a replication applies, from `onError()` or a write filter, is part of that
    replication and applies at once.
  - The write filters' document hooks (`beforeCreateDocument()`, ...) run when the replication applies, in its
    coroutine.
  - `$mirror->awaitReplications(?int $timeout = null)` returns once every replication queued so far has reached
    the destination or has been reported to `onError()`, or once `$timeout` milliseconds have passed, without an
    error. `awaitReplications(0)` returns at once, and a negative timeout throws. Call it before a worker stops, or queued replications are lost. `delete()` waits for them before it
    deletes the destination database.
- **Authorization.** `new Mirror($source, $destination)` leaves the source's and the destination's `Authorization` in
  place, and the mirror uses the source's, so roles and `skip()` scopes set on it apply to reads and writes through
  the mirror. In 7.x the mirror started with an `Authorization` of its own, which it also gave the source's adapter.
  `$mirror->setAuthorization()` sets one on the mirror, its source and its destination, as before.
- **Write filters.** A `null` return from `beforeCreateCollection()`, `beforeUpdateCollection()`,
  `beforeCreateAttribute()`, `beforeUpdateAttribute()` or `beforeCreateIndex()` skips that change on the destination,
  and a collection whose creation was skipped is not replicated. An exception from a filter hook is reported to
  `onError()` under the write's method and skips that replication; the source change stands.
- **Decorators.** Decorator hooks added through a mirror stay on the mirror: they decorate what its reads and writes
  return (including the documents bulk writes hand `onNext`), and the destination receives undecorated documents.
  `createDocument()` returns the document written to the source, as `updateDocument()` does, instead of the
  destination's copy.
- **Upserts.** `upsertDocument()` and an increasing `upsertDocuments()` through a mirror now run on the source and
  are replicated to the destination, like a plain `upsertDocuments()`; in 7.x they were never replicated. Hooks
  registered through the mirror receive `document_purge` for each upserted document, `document_upsert` for
  `upsertDocument()` and `documents_upsert` once per `upsertDocuments()` call. A failed replication is reported to
  `onError()` with the method `upsertDocument` or `upsertDocuments`.

## Validators and helpers

The validators that depended on adapter limits and capabilities take the `Adapter\Profile` of the `Database`
(`$database->profile()`) instead of positional limits and booleans.

| 7.x | 8.0 |
|---|---|
| `new Validator\Attribute(array $attributes, array $schemaAttributes = [], int $maxAttributes = 0, int $maxWidth = 0, ... 14 more limits and booleans)` | `new Validator\AttributeDefinition(array $attributes, Adapter\Profile $profile, array $schemaAttributes = [], ?\Closure $attributeCount = null, ?\Closure $attributeWidth = null, ?\Closure $filter = null)` |
| `new Validator\Index(array $attributes, array $indexes, int $maxLength, array $reservedKeys = [], ... 15 booleans)` | `new Validator\IndexDefinition(array $attributes, array $indexes, Adapter\Profile $profile)` |
| `new Queries\Documents(array $attributes, array $indexes, string $idAttributeType, int $maxValuesCount = 5000, int $maxUIDLength = 36, \DateTime $minAllowedDate, \DateTime $maxAllowedDate, bool $supportForAttributes = true, bool $supportUnsignedBigInt = true)` | `new Queries\Documents(array $attributes, array $indexes, Adapter\Profile $profile, int $maxValuesCount = 5000)` |
| `new Queries\Document(array $attributes, bool $supportForAttributes = true)` | `new Queries\Document(array $attributes, Adapter\Profile $profile, int $maxValuesCount = 5000)` |
| `new Structure(Document $collection, string $idAttributeType, \DateTime $minAllowedDate, \DateTime $maxAllowedDate, bool $supportForAttributes = true, bool $supportUnsignedBigInt = true, ?Document $currentDocument = null)` | `new Structure(Document $collection, Adapter\Profile $profile, ?Document $currentDocument = null, array $storedAttributes = [])` |
| `new Query\Order(array $attributes = [], bool $supportForAttributes = true)` | `new Query\Order(array $attributes = [], bool $supportForAttributes = true, bool $supportForOrderRandom = true)` |
| `Validator\Queries`, `Validator\IndexedQueries` | `Validator\Queries\Base`, `Validator\Queries\Indexed` |
| `Validator\ObjectValidator` | `Validator\ObjectValue` |

```php
// 7.x
$validator = new Index($attributes, $indexes, $adapter->getMaxIndexLength(), [], $adapter->getSupportForIndexArray(), ...);

// 8.0
$validator = new IndexDefinition($attributes, $indexes, $database->profile());
```

- `Queries\Documents` and `Queries\Document` accept joins, aggregations and `$tenant` exactly as the profile allows:
  joins with `Capability::Joins`, aggregate functions, `groupBy`, `having` and `distinct` with
  `Capability::Aggregations`, and `select('$tenant')` under shared tables.
- `Utopia\Database\Validator\Query\Select`, `Aggregate` and `GroupBy` accept `$tenant` only when constructed with
  `sharedTables: true` (third constructor argument). A validator built directly rejects `select('$tenant')` without
  it; `Database::find()` already rejected it without shared tables in 7.x.
- `Query\Order` refuses `orderRandom()` when constructed with `supportForOrderRandom: false`.
- The new `Utopia\Database\Validator\Query\Join` takes the main collection's attributes (`new Join($attributes)`) to
  check the columns of a join condition. `new Join()` accepts any column of the main collection.
  `Validator\Query\Joined\Collection` and `Validator\Query\Joined\Attributes` check the joined collections.
- `Query\Filter` defaults `supportUnsignedBigInt` to `true`, as in 7.x.
- `Validator\Query\Cursor` accepts a `Document` or a document id, and refuses an array.
- The protected `Database::getDocumentsValidator(Document $collection, array $joinedCollections = [])` is new in 8.0.
- Changed validator signatures: `AttributeDefinition`'s `check*()` methods take an `Attribute`,
  `IndexDefinition`'s take an `Index` (`checkTTLIndexes()` is `checkTtlIndexes()`), `getRequiredFilters()` and
  `validateDefaultTypes()` take a `ColumnType`, `Structure::addFormat()`, `getFormat()` and `hasFormat()` (and those
  of `PartialStructure`) take a `ColumnType`, `Query\Filter::isValidAttributeAndValues()` takes a `Method` case, and
  `Validator\Spatial::isWKTString()` is `isWktString()`. `Validator\Operator` takes a trailing
  `bool $supportUnsignedBigInt = true`. Every `isValid()` takes `mixed $value`: a call that names the argument
  after the 7.x parameter (`$permissions` on `Permissions`, `$roles` on `Roles`, `$document` on `Structure` and
  `PartialStructure`, `$input` on `Authorization`) passes `value:` instead.
- `Validator\Permissions` and `Permission::aggregate()` take their allowed permission types as `PermissionType`
  cases; a list of strings throws a `TypeError`. `Validator\Authorization\Input` takes a `PermissionType` case.
- `Validator\Authorization`'s status is no longer a `protected bool $status` property; subclasses read and change it
  through `getStatus()`, `setStatus()` and `skip()`. `skip()` is scoped to the calling coroutine and the coroutines
  it starts; `setStatus()`, `enable()`, `disable()` and `reset()` change the shared status unless called inside
  such a scope (see [Coroutines](#coroutines)). `setDefaultStatus()` is a constructor argument,
  `new Authorization(bool $defaultStatus = true)`. `restore()` is internal.
- `Validator\Structure` takes `array $storedAttributes = []`: the attributes whose values are the stored ones, which
  it does not validate again. `Database::updateDocument()` passes it.
- `Database::convertQueries()` takes an optional `array $joinedCollections` (join alias => collection). With it,
  filters on `alias.attribute`, the filters of join ON lists and `having()` conditions in the list are converted
  too; aggregates and selects in the list are left as they are. Without it the method converts as before.

## Removed unused public methods

Nothing in the library, Appwrite, Appwrite Cloud or utopia-php/migration calls these 7.x methods.

| Removed | Replacement |
|---|---|
| `Adapter\SQL::setFloatPrecision(int $precision)` | Floats are bound with 17 digits. A subclass can set the protected `$floatPrecision` property |
| `Adapter\SQLite::setEmulateMySQL()`, `getEmulateMySQL()` | A subclass sets the protected `$emulateMySQL` property to `true` |
| `Database::getInstanceFilters()` | The codecs given to the constructor, or `getFilters()` |
| `Mirror::getWriteFilters()` | The `$filters` given to the constructor. A subclass reads the protected `$writeFilters` property |
| `Validator\Structure::getFormats()` | `Structure::hasFormat($name, $type)` and `Structure::getFormat($name, $type)` |

## Changes since the 8.0 pre-releases

Appwrite, Appwrite Cloud and utopia-php/migration built against the unreleased `feat-query-lib` branch. These names
from those builds changed before 8.0.0; none of them exists in 7.x.

| Pre-release | 8.0.0 |
|---|---|
| `new Collection(...)`, `new Attribute(...)`, `new Index(...)`, `new Relationship(...)`, the 18 `Attribute\*` subclasses | `Collection::create()` and the factories; the constructors are private |
| `Attribute::getKey()`, `getType()`, `isArray()`, `getFormat()` and the other getters, `$attribute->type` through magic `__get()` | Public readonly properties (`$attribute->type`) |
| `Attribute::persistedType()`, `normalizeType()`, `tryNormalizeType()` | `Attribute::storedType()`, `Attribute::typeFromStored()` |
| `Attribute::isSpatialType($type)` and the other static `is*Type()`, `getNumericBounds()`, `setFilters()` | `isSpatial()`, `isNumeric()`, `isInteger()`, `bounds()`, `withFilters()` on the value object |
| `Attribute::linestring()`, `Index::fullText()`, `Index::index()` | `Attribute::lineString()`, `Index::fulltext()`, `Index::key()` |
| `Attribute::availableTypes(objects:, spatial:, vectors:)` | `Attribute::availableTypes(Adapter\Profile $profile)` |
| `Index::getIndexedAttributes()`, `setLengths()`, `setOrders()`, orders as `Utopia\Query\Schema\Order` | `$index->attributes`, `withLengths()`, `withOrders()`, orders as `OrderDirection` |
| `Relationship(collection: ...)`, `getSourceCollection()`, `RelationType`, `RelationSide`, `ForeignKeyAction $onDelete` | `createRelationship($collection, ...)`, `RelationshipType`, `RelationshipSide`, `RelationshipDeleteAction` |
| `Collection::getDeclaredAttributes()`, `getIndexes()`, `getName()`, `getDeclaredPermissions()`, `hasDocumentSecurity()`, `$metadata`, `isEmpty()` on a missing collection | `attributes()`, `indexes()`, `name()`, `declaredPermissions()`, `documentSecurity()`, `Collection::create(metadata:)`, `findCollection()` |
| `createRelationship(Relationship $relationship)` | `createRelationship(string $collection, Relationship $relationship)` |
| `updateAttribute(..., ColumnType\|string\|null $type, ...)`, `updateRelationship(..., ?ForeignKeyAction $onDelete)`, `updateCollection(string $id, ...)` | `AttributeUpdate`, `RelationshipUpdate`, `CollectionUpdate` |
| `Database::ATTRIBUTE_FILTER_COLUMN_TYPES`, `Database::INTERNAL_ATTRIBUTES`, static `internalAttributes()`, `getInternalAttributes()`, `internalAttributeDocuments()` | The factories add their filters; `$database->internalAttributes()` |
| `Validator\Attribute`, `Validator\Index` with positional booleans, `Queries\Bounds` | `AttributeDefinition`, `IndexDefinition` with an `Adapter\Profile`; `$profile->limits` |
| The six mandatory `Feature\{Attributes,Collections,Databases,Documents,Indexes,Transactions}` interfaces | The abstract methods of `Adapter` |
| `Feature\SchemaAttributes`, `Feature\SchemaIndexes`, `Feature\ColumnTypes` | `Capability::SchemaIntrospection` and the abstract `getSchemaAttributes()`, `getSchemaIndexes()`, `getColumnType()` |
| `Feature\InternalCasting` (`castingBefore()`, `castingAfter()`, `castingAfterDocuments()`), `Feature\UTCCasting` (`setUTCDatetime()`) | `Feature\Casting` (`castBefore()`, `castAfter()` over a page, `castDatetime()`) |
| `Feature\ConnectionId` (`getConnectionId()`), `Capability::Hostname`, `Capability::Reconnection` | `Feature\Connection` (`id()`, `hostname()`, `ping()`, `reconnect()`) |
| `Feature\QueryBuilder::getBuilder()`, `getSchema()`, `createSchemaBuilder()` | `builder()`, `schema()` |
| `Feature\Spatial::decodePoint()`, `decodeLinestring()`, `decodePolygon()`, `Database::encodeSpatialData()` | `Feature\Spatial::encode()`, `decode()` |
| `Feature\Upserts::upsertDocuments(Document $collection, string $attribute, array $changes)`, `Change::setOld()`, `setNew()`, `getOld()`, `getNew()` | `upsertDocument()`, `upsertDocuments(Document $collection, array $changes, ?string $increase = null)`, `$change->old`, `$change->new` |
| `Capability::Casting` | `Feature\Casting`: the library casts unless the adapter implements it |
| `Capability::AtomicTransactions`, `BatchCreateAttributes`, `BatchOperations`, `BoundaryInclusive`, `CacheSkipOnFailure`, `JSONOverlaps`, `MultiDimensionDistance`, `NumericCasting`, `OptionalSpatial`, `PCRE`, `POSIX`, `QueryContains`, `Regex`, `StatisticalAggregates`, `BitwiseAggregates` | Removed. SQLite refuses the statistical and bitwise aggregates with `Exception\Query` |
| `Capability::Index`, `UniqueIndex`, `Fulltext`, `FulltextWildcard`, `MultipleFulltextIndexes`, `TrigramIndex`, `TTLIndexes`, `ObjectIndexes`, `CastIndexArray`, `IdenticalIndexes`, `SpatialIndexNull`, `SpatialIndexOrder`, `NestedTransactions` | `IndexKey`, `IndexUnique`, `IndexFulltext`, `IndexFulltextWildcard`, `IndexFulltextMultiple`, `IndexTrigram`, `IndexTtl`, `IndexObject`, `IndexArrayCast`, `IndexIdentical`, `IndexSpatialNull`, `IndexSpatialOrder`, `TransactionNested` |
| `Adapter::getMaxUIDLength()` and the other limit getters, `Database::getMaxUIDLength()` | `limits()`; `Database::getMaxUidLength()` |
| `SQL::getPDO()` (public, deprecated) | `getDriver()` |
| `Database::execute()` | `query()` for reads (a list of `Document`), `mutate()` for writes (the affected row count) |
| `Database::enableProfiling()`, `disableProfiling()`, `Profiler\QueryProfiler`, `Profiler\QueryLog` | `setProfiling(bool)`, `Utopia\Database\Profiler`, `Profiler\Log` |
| `Database::setTypeRegistry()`, `Type\Custom`, `Type\TypeRegistry`, an associative constructor `$filters` | `setFilters()`, `Filter\Codec`, `Filter\Registry`, a list of `Filter\Codec` |
| `Cache\QueryCache` with `$cacheName` and `writerTimeout` arguments | `Cache\Query`, which uses those of the `Database` calling it |
| `Authorization::withStatus()` | `skip()`, or `setStatus()` inside `skip()` |
| `Hook\Lifecycle::handle(Event $event, mixed $data)` | `handle(Event\Domain $event)` |
| `Event\Documents\Created`, `Updated`, `Deleted` | `Event\Document\BatchCreated`, `BatchUpdated`, `BatchDeleted` |
| `Event\Domain::$occurredAt` | Removed |
| `Cache\Invalidator` as a `Hook\Lifecycle` | A hook of its own kind; `addHook()` still takes it |
| `Hook\Write` extending `Utopia\Query\Hook\Write`, its row-level `after*(string $table, ...)` methods, the closure-bag write context, `Document::SKIP_PERMISSIONS_UPDATE` | `decorateRow(array $row, Hook\RowMetadata $metadata)`, the document-level `afterDocument*()` methods and `Hook\WriteContext::skipPermissions()` |
| `new Relationships($database)` | `new Relationships()`, attached by `addHook()` |
| `Adapter::hasTenantHook()`, `hasPermissionHook()` | Removed |
| `Hook\PermissionFilter`, `PermissionJoinFilter`, `PermissionAllowNullUid`, `OuterJoinPermissionFilter` | `Adapter\SQL\Hook\Permission\Filter`, `Join`, `AllowNullUid`, `OuterJoin` |
| `Hook\TenantFilter`, `RawTenantFilter`, `OuterJoinTenantFilter`, `RawOuterJoinTenantFilter` | `Adapter\SQL\Hook\Tenant\Filter`, `Raw`, `OuterJoin`, `RawOuterJoin` |
| `Hook\JoinChain`, `OuterJoinChainFilter`, `AllowNullColumn` | `Adapter\SQL\Hook\Join\Chain`, `Join\OuterChain`, `Column\AllowNull` |
| `Hook\Read`, `Hook\Mongo\PermissionFilter`, `Hook\Mongo\TenantFilter` | `Hook\Mongo\Read`, `Hook\Mongo\Permission`, `Hook\Mongo\Tenant` |
| `Traits\`, `Builder\PostgreSQL`, `State\Scope` | `Trait\`, `Builder\Postgres`, `State\Frame` |
| `Validator\Query\JoinedCollection`, `JoinedAttributes` | `Validator\Query\Joined\Collection`, `Joined\Attributes` |
| A plain `class` extending `Hook\PermissionFilter`; `Hook\TenantFilter`, `Hook\Mongo\PermissionFilter` and `Hook\Mongo\TenantFilter` as base classes | `Adapter\SQL\Hook\Permission\Filter` is `readonly`, so a subclass, such as one an adapter's `newPermissionHook()` or `newJoinPermissionHook()` override returns, is declared `readonly class`; `Adapter\SQL\Hook\Tenant\Filter`, `Hook\Mongo\Permission` and `Hook\Mongo\Tenant` are `final readonly` |
| The protected SQL adapter methods `compileAdapterFilter()`, `getOperatorBuilderExpression()`, `getOperatorUpsertExpression()` and `getVectorOrderRaw()` returning `array{expression: string, bindings: list<mixed>}`; `compileAdapterFilter()`'s `$joins` as `list<array{table: string, alias: string}>` | They return `Adapter\SQL\Expression` (`$sql`, `$bindings`), nullable where the array was; `$joins` is a `list<Adapter\SQL\JoinAlias>` (`$table`, `$alias`) |
| `Cache\Region` with writable `$ttl` and `$enabled`; `Profiler\QueryLog` as a base class | `Cache\Region` and `Profiler\Log` are `final readonly`: pass a new `Region` to `Cache\Query::setRegion()` |
| `Storage::PERMS_SUFFIX`, `PERM_DOCUMENT`, `PERM_TYPE`, `PERM_PERMISSION` | `Storage::PERMISSIONS_TABLE_SUFFIX`, `PERMISSIONS_DOCUMENT`, `PERMISSIONS_TYPE`, `PERMISSIONS_PERMISSION` |
| `Query::join($collection, $left, $right, $operator, $alias)` and the other column-form joins, `getJoinAlias()`, `Storage::joinAlias()` | `Query::join($collection, $alias, [Query::on(...)])`, `getAlias()`; every join names its alias |
| `Query::groupForDatabase()` | `Query::groupByType()`, which returns a `ParsedQuery` |
| `Document::INTERNAL_ID` | `Document::SEQUENCE` |
| `Hook\Permissions::UNCHANGED`, `WriteContext::skipPermissions()` without arguments | `WriteContext::skipPermissions(Document $document)`, fed by `Adapter::updateDocuments(..., $skipPermissions)` |
| `Snapshot::$skipDuplicates`, `skippingDuplicates()` | `Snapshot::$ignoreDuplicates`, `isIgnoringDuplicates()` |
| `Mirror::onError(callable(string $action, \Throwable $error))`, `awaitReplications()` without a timeout | `onError(callable(Mirror\Failure $failure))`, `awaitReplications(?int $timeout = null)` |
| `Adapter::setDebug()`, `getDebug()`, `resetDebug()` | `setMetadata()`, `getMetadata()`, `resetMetadata()` |
| `Exception::__construct(string $message, ...)` with a string code cast to 0 | Every argument optional; a string code kept in `$state` |

## Rules for features new in 8.0

These features do not exist in 7.x. Their rules are listed here because they differ from what a reader of the 7.x
API might expect. [CHANGELOG.md](CHANGELOG.md) describes the features themselves.

### Joins

Every join names its alias and takes its conditions as a list: `Query::join(string $collection, string $alias, array
$on)`, `leftJoin()`, `rightJoin()` and `fullOuterJoin()` take `Query::on(string $left, string $right, string $operator
= '=')` conditions and filters, and `crossJoin(string $collection, string $alias)` and `naturalJoin()` take none. In
`on()` the left column belongs to the main collection or to a join declared before it, and the right one to the
joined collection. An ON
list member that is not `on()` or a filter (`limit()`, `select()`, an order, ...) throws
`Utopia\Query\Exception\ValidationException` when the query is built.

```php
$database->find('reviews', [
    Query::join('movies', 'm', [Query::on('movie', '$id')]),
    Query::select(['body', 'm.name']),
]);
```

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
- A join without a select, or with `select(['*'])`, returns under each join alias the joined collection's `$id` and
  attributes as `alias.$id` and `alias.attribute`, plus each joined internal attribute the read orders by
  (`alias.$sequence`, `alias.$createdAt`, `alias.$updatedAt`), so its rows can be passed back as a cursor.
- A select may name `alias.*`, alone or next to other selects: it returns the joined row as a direct read of the
  joined collection returns it, its `$id`, `$sequence`, `$createdAt`, `$updatedAt`, `$permissions` and attributes as
  `alias.$id`, `alias.$sequence`, ... and `alias.attribute`. It never returns the joined `$tenant`: select
  `alias.$tenant` to read it. A row an outer join left unmatched holds null for each of them. An aggregation query
  still rejects `alias.*` as an ungrouped select, and an alias the query does not join is not found.
- Joined columns named next to `*` are returned with everything `*` returns: `select(['*', 'alias.$createdAt'])`
  returns the main document, the joined `$id` and attributes, and `alias.$createdAt`.
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
  joins below five keep semi-joins. Right after a collection is created or bulk-loaded, until InnoDB's automatic
  statistics recalculation has run (seconds, with the default `STATS_AUTO_RECALC`), a left join on a joined
  collection's own `$id` can be slow. Run `ANALYZE TABLE` after a bulk load to avoid that window.
- On PostgreSQL, planning a read takes about 2.6 to 3.2 times longer with each join from the fifth: on a
  development machine, about 2.7 ms with five permission-checked joins, 6.9 ms with six and 20.7 ms with seven. It
  stops growing at eight joins, where PostgreSQL's default `join_collapse_limit` ends its search for a join order.
  Keep a read to about five joins, or split it into several reads.
- On MariaDB and MySQL, a one-to-many joined read is ordered by the joined `$id` behind the main `$sequence` only when
  its rows show the join (no `select()`, `*`, or a joined attribute) or it pages with a cursor; see
  [Paging a joined read](#paging-a-joined-read). Without a bound, the engine sorts the whole join before applying the
  limit. A read bounds the sort when every join is a left join, no filter or search names a joined attribute, the order
  starts with main attributes (the main `$sequence` or `$id` among them) and it has a limit: the read first picks the
  main documents its page can reach (`offset + limit`, plus the cursor's own), including those matched by a fulltext
  search on main attributes, and joins only those. Inner, right and full outer one-to-many joins, filters and searches
  on joined attributes and orders that start with a joined attribute still sort the whole join on MariaDB and MySQL.
  PostgreSQL is not affected. Index the join keys, and for a large one-to-many read select main attributes only or use
  left joins.
- `Database::updateDocuments()` and `Database::deleteDocuments()` do not accept join queries. They throw
  `Utopia\Database\Exception\Query` with `Join queries are not supported for bulk updates` or
  `Join queries are not supported for bulk deletes`.
- On adapters without joins or aggregations (Memory, MongoDB, Redis), `find()`, `aggregate()`, `count()` and
  `sum()` reject those queries during validation with `Invalid query method: <method>`.
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
  of the same name. A read whose `select()` names attributes without `*` has to select every joined value it orders
  by, a paged join's `alias.$id` included, to be paged; `alias.*` selects them all. A read without a select, or
  with `*`, returns them.
  `cursor()` checks the last row of each full batch before yielding the batch, so such a read throws
  before the first row; a read that fits in one batch is not paged and needs no such value.
- A value may be null (a row an outer join did not match, a nullable attribute). Nulls keep the engine's position:
  first in ascending order on MariaDB, MySQL and SQLite, last on PostgreSQL, and the cursor pages through them.
- A row a right or full outer join returned without a main document (its `$id` is `''`) is a valid cursor for that
  read. A read without joins or `distinct()` still refuses a cursor document without an `$id`
  (`Invalid query: Invalid cursor: …`).
- `getDocument()` with a join that matches several joined rows returns the one with the lowest `$sequence`, join by
  join in join order.

### Aggregations

Aggregation queries run through `Database::aggregate($collection, $queries)`, which returns the rows as arrays (see
[Bulk writes and reads](#bulk-writes-and-reads)); `find()` refuses them.

```php
$rows = $database->aggregate('orders', [
    Query::count('*', 'orders'),
    Query::sum('total', 'revenue'),
    Query::groupBy(['status']),
]);
// [['status' => 'paid', 'orders' => 12, 'revenue' => 340.5], ...]
```

- An aggregation query is one with an aggregate (`count`, `countDistinct`, `sum`, `avg`, `min`, `max`, the
  statistical and the bitwise aggregates) or a `groupBy()`. A `groupBy()` without an aggregate counts. A select in
  an aggregation query may name only the attributes the query groups by; any other select throws
  `Utopia\Database\Exception\Query`
  (`Invalid query: Cannot select "<attribute>": an aggregation query can only select the attributes it groups by`).
  Group by the attribute or leave it out of the select. `*` and relationship wildcards (`key.*`, `parent.child.*`)
  are accepted and ignored, so a listing that always adds them keeps working. A join alias's `alias.*` is rejected
  like any ungrouped select. A join alias cannot equal a relationship key of the main collection (see
  [Joins](#joins)), so `key.*` is always the relationship's wildcard. The rule applies to `aggregate()`, `count()`
  and `sum()`, which validate their query set the same way.
- An order in an aggregation query may name only an aggregate alias or an attribute the query groups by, and
  `orderRandom()` is accepted; any other order throws `Utopia\Database\Exception\Query`
  (`Invalid query: Cannot order by "<attribute>": an aggregation query can only order by its groups and aggregates`).
  A grouped attribute that only a joined collection declares matches its bare name and its aliased name alike.
- A `Utopia\Database\Validator\Query\Select` or `Validator\Query\Order` built directly applies its rule only when
  it is handed the query set's aggregates and groups (`setAggregations()`, `setGroupBy()`), as
  `Utopia\Database\Validator\Queries\Base` does.
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
- SQLite has no statistical (`stddev`, `stddevPop`, `stddevSamp`, `variance`, `varPop`, `varSamp`) or bitwise
  (`bitAnd`, `bitOr`, `bitXor`) aggregate functions, and `aggregate()` throws `Utopia\Database\Exception\Query` for
  them there.
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
- On MariaDB under shared tables, an aggregation query or a `distinct()` read can examine many more rows than on
  dedicated tables: the engine reads the tenant's rows in the order of the index it groups by and checks permissions
  row by row, instead of checking permissions first. With 50,000 documents and one tenant it takes about 5 times as
  long as on dedicated tables (43 to 50 ms against 9 to 10 ms on a development machine). The more tenants a table
  holds, the fewer rows each tenant's range covers. MySQL and PostgreSQL read the same way in both modes.

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

### Query builder: `from()`, `query()`, `mutate()` and `rawQuery()`

`Database::from($collection)` returns a utopia-php/query builder over the collection's table.
`Database::query($statement)` runs a read as written and returns a list of `Document`, and
`Database::mutate($statement)` runs a write and returns the affected row count. They are available on the SQL
adapters only
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

`setQueryCache(new Cache\Query($cache))` caches `find()` results per hostname, database, namespace, tenant and
collection. Writes through the `Database` invalidate only the scope they write in. `purgeCachedDocument()` and
`purgeCachedQueries($collection)` invalidate it as well; call one of them after changing data behind the library's
back.

- Use a cache adapter with generations (`Utopia\Cache\Feature\Leasable`: Redis and Redis\Multiplexing, and Pool,
  Sharding and CircuitBreaker when the adapters they wrap have them). With any other adapter a collection's cache
  stays off for one region TTL after each write, because concurrent writers cannot be told apart.
- Collection listings (`find('_metadata')`, `listCollections()`) are never cached.
- Under tenant-per-document, a write invalidates the scope of each written document's tenant.

### Pools and profiling

- **Query profiling.** `Database::setProfiling(true)` attaches a `Utopia\Database\Profiler` that
  records the SQL statements the adapter runs. The profiler keeps the newest `Profiler::DEFAULT_CAPACITY`
  (1000) entries; older entries are dropped as new ones arrive. Change the bound with
  `$database->getProfiler()?->setCapacity($entries)` (at least 1). `getQueryCount()` and `getTotalTime()` cover every
  statement logged since the last `reset()`, including dropped ones, so they can exceed what `getLogs()` returns.
  `setProfiling(false)` stops recording and detaches the profiler from the adapter; what was captured stays readable
  through `getProfiler()->getLogs()` until `reset()`. Behind `Adapter\Pool` a connection carries the handle's
  profiler only while it is checked out. A `Pool` subclass that checks connections out itself (with
  `$this->pool->use(...)`) should call `$this->releaseBorrowed($adapter)` before giving the connection back.
  Each `Utopia\Database\Profiler\Log` carries the statement's bound values (`bindings`), the collection it
  reads (`collection`, for the statements `getDocument()`, `find()`, `count()` and `sum()` run) and the operation
  that ran it (`operation`, the `Utopia\Database\Event` value, such as `document_find`). Statements that bind by
  hand (`rawQuery()`, schema changes) log no bindings, and writes log no collection. `Profiler\Log` has no
  `explainPlan`.
- **Capability questions behind a `Pool`.** `supports()`, `capabilities()` and `hasFeature()` ask a connection the
  first time and are then answered without one, for every handle built over the same `Utopia\Pools\Pool`.
  `supports(Capability::DefinedAttributes)` is the exception: it reports the schema mode of the connection that
  answers, so it always asks one. `Database::setLocks()` reaches every borrowed connection.
- **Transactions behind a `Pool`.** `withTransaction()` pins one connection for the coroutine that calls it and the
  coroutines it starts. Other coroutines sharing the handle borrow connections of their own and run outside the
  transaction; in 7.x their statements ran on the pinned connection, inside the transaction, and their own
  `withTransaction()` became a savepoint in it. A coroutine started inside the transaction shares the pinned
  connection, so it must not run a statement while its parent runs one. Every call on the pinned connection runs
  under the calling coroutine's tenant. The protected `Pool::$pinnedAdapter` property is removed: a subclass reads
  the pinned connection through `pin()`, and can override it.
- **Read/write splitting (`Adapter\ReadWritePool`).** Reads go to the read pool and writes to the write pool. After
  a write returns, or a `withTransaction()` block finishes, reads stay on the write pool for the sticky window
  (`setStickyDuration()`, default 5000 ms; `setSticky(false)` turns it off), so a caller reads its own writes.
  `getDocument(..., forUpdate: true)` and `rawQuery()` always use the write pool, since a locking read must run on
  the primary and raw SQL may write; both also open the sticky window. So do the reads that decide a write: the
  batch `updateDocuments()` and `deleteDocuments()` select, and the document `upsertDocuments()` compares against. Calls that touch no data (capability checks
  such as `supports()`, `capabilities()` and `hasFeature()`, `limits()`, value casting, and configuration such as
  `setSchemaless()`) are answered wherever a read would be and never open the sticky window.
  `hostname()` always names the write pool's host, because it namespaces document and query cache keys, and is
  looked up once per handle. `getDriver()` counts as a write: code that runs statements through the raw driver gets
  a write-pool connection.

## Known limitations

- A transaction begun on the adapter directly (`$database->getAdapter()->startTransaction()`) is not seen by the
  cache invalidation or the `document_purge` queue that `withTransaction()` keeps. Each write inside it invalidates
  the caches and fires `document_purge` when that write returns, inside the adapter transaction and before it
  commits, and a rollback does not withdraw them. Reads inside it are not served from the document cache. Group
  writes with `withTransaction()` instead.

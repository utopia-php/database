# Changelog

## 8.0.0 (unreleased)

8.0 is a major release. Read [UPGRADE.md](UPGRADE.md) before you upgrade from 7.x: it lists every change you may
have to make, with the 7.x and 8.0 forms side by side.

### Breaking changes

- Document permissions and relationships are hooks, and a `Database` registers neither on its own: add
  `Hook\Permissions` and `Hook\Relationships` with `addHook()`. See
  [Register the permission and relationship hooks](UPGRADE.md#register-the-permission-and-relationship-hooks).
- The string constants of `Database`, `Query`, `Operator` and `Document` are replaced by enums, and the methods that
  took or returned those strings take or return enum cases. `Query::getMethod()` returns a `Utopia\Query\Method`
  case, so a comparison with a string is always `false`. See
  [Constants are now enums](UPGRADE.md#constants-are-now-enums) and [Queries](UPGRADE.md#queries).
- `Helpers\ID`, `Helpers\Permission` and `Helpers\Role` are `Utopia\Database\Id`, `Permission` and `Role`;
  `Mirroring\Filter` is `Mirror\Filter`; several validators moved. No aliases are kept. See
  [Namespaces](UPGRADE.md#namespaces).
- The schema methods take and return value objects: `createCollection(Collection)`,
  `createAttribute(string, Attribute)`, `createIndex(string, Index)`, `createRelationship(string, Relationship)`, and
  `updateAttribute()`, `updateCollection()` and `updateRelationship()` take `AttributeUpdate`, `CollectionUpdate` and
  `RelationshipUpdate`. `Attribute`, `Index` and `Relationship` are `final readonly` with private constructors and a
  factory per type; `Collection::create()` builds a collection. `getCollection()` throws `Exception\NotFound` for a
  missing collection, and `findCollection()` returns `null`. `updateAttributeRequired()`, `updateAttributeFormat()`,
  `updateAttributeFormatOptions()`, `updateAttributeFilters()` and `updateAttributeDefault()` are removed. See
  [Schema: value objects](UPGRADE.md#schema-value-objects).
- Stored attribute values are normalised per type (datetime `signed: false`; integer `size` 0 or 8; boolean, double
  and id `size: 0`; strings `signed: true`), fulltext and TTL indexes store no `orders`, fulltext indexes no
  `lengths`, and an index other than TTL no longer stores `ttl: 1`. `Attribute::fromDocument()` and
  `Index::fromDocument()` read 7.x metadata with the same normalisation, so a legacy attribute or index equals the
  one 8.0 creates.
- A stored attribute of an unknown type (`uuid7` included) or a stored relationship `onDelete` of `setDefault` or
  `noAction` makes its collection unreadable until the metadata row is repaired. See
  [Stored metadata](UPGRADE.md#stored-metadata).
- `Database::on()`, `Database::before()`, `Mirror::on()`, `Adapter::before()` and the `Database::EVENT_*` constants
  are removed: register `Hook\Lifecycle` and `Hook\Transform` hooks with `Database::addHook()`. A lifecycle hook
  receives one typed event object per event (`handle(Event\Domain $event)`). See
  [Lifecycle events are hooks](UPGRADE.md#lifecycle-events-are-hooks).
- Bulk writes take one `onNext(Document $document, ?Document $previous)` callback and no `$onError`;
  `Database::BATCH_SIZE` replaces `INSERT_BATCH_SIZE` and `DELETE_BATCH_SIZE`, and a larger batch size throws
  `Exception\Limit`. `upsertDocumentsWithIncrease()` is `upsertDocuments(..., increase:)`. `iterate()` and
  `foreach()` are removed: `cursor()`, where `limit()` caps the total. `find()` refuses aggregate and `groupBy()`
  queries: use `aggregate()`. See [Bulk writes and reads](UPGRADE.md#bulk-writes-and-reads).
- Every configuration toggle is `set<Noun>(bool)` with an `is`/`has` reader: `setValidation()`, `setFiltering()`,
  `setLocks()`, `isPreservingDates()`, `hasSharedTables()`, `ignoreDuplicates()` and the others replace
  `enableValidation()`, `disableValidation()`, `enableFilters()`, `disableFilters()`, `enableLocks()`,
  `getPreserveDates()`, `getSharedTables()` and `skipDuplicates()`. See
  [Configuration toggles](UPGRADE.md#configuration-toggles).
- Instance filters are `Filter\Codec` objects: the `Database` constructor takes a list of them instead of an
  associative array. See [Filters](UPGRADE.md#filters).
- The 51 `Adapter::getSupportFor*()` methods are removed: check `supports(Capability::...)` or
  `hasFeature(Feature\...::class)`. The 16 per-limit getters are `Adapter::limits()`, an `Adapter\Limits` value,
  and `Database::profile()` snapshots limits, capabilities and features for the validators. The mandatory adapter
  contract is the abstract methods of `Adapter`; `Adapter\Feature` holds the optional features only.
  `Adapter\SQLite` extends `Adapter\SQL` instead of `Adapter\MariaDB`. See [Adapters](UPGRADE.md#adapters).
- `Document::getRead()`, `getCreate()`, `getUpdate()`, `getDelete()`, `getWrite()`, `find()`, `findAndReplace()` and
  `findAndRemove()` are removed, and `getArrayCopy()` takes no arguments: use `getPermissionsByType()`, `only()` and
  `except()`.
- `Exception\Order` and `Exception\Operator` extend `Exception\Query`, so a `catch (Exception\Query)` also catches
  them. See [Errors](UPGRADE.md#errors).
- The validators that read adapter limits and capabilities take an `Adapter\Profile`. Every validator's `isValid()`
  takes `mixed $value`, so a named argument `permissions:`, `roles:`, `document:` or `input:` is `value:`. See
  [Validators and helpers](UPGRADE.md#validators-and-helpers).
- Cache key names changed: do not share a cache between 7.x and 8.0 processes, and flush it after the last 7.x
  process has stopped. See [Caches](UPGRADE.md#caches).
- `Exception\Unique` has the message `Document with the requested unique attributes already exists` on every
  adapter (was `Unique index violation`). On MariaDB and MySQL an unknown column throws `Exception\NotFound`.
  `Database::sum()` throws `Exception\Query` for an attribute that is not a single number. See
  [Errors](UPGRADE.md#errors).
- `Query::DEFAULT_ALIAS` is `table_main` (was `main`), `Query::groupByType()` returns a `Utopia\Database\ParsedQuery`
  object (with `orderAttributes` and `orderTypes`, throwing `Exception\Query` for a cursor that is not a document),
  and `Query::TYPE_ELEM_MATCH` is removed.
- `new Document()` and `Document::setAttribute('$permissions', ...)` reject non-string permissions with
  `Exception\Structure`.
- Hook failures follow 7.x event by event, with three differences: an `\Error` always reaches the caller, an isolated
  failure no longer skips the other hooks, and `document_purge` from `updateDocument()` and `deleteDocument()` fires
  after the write's transaction commits. See [Hook failures](UPGRADE.md#hook-failures).
- `createDocuments()` under `ignoreDuplicates()` no longer counts skipped documents or passes them to `onNext`, on
  every adapter.
- `increaseDocumentAttribute()` and `decreaseDocumentAttribute()` refuse a fractional change value or a fractional
  `max`/`min` on an integer attribute with `Exception\Type`, and the numeric operators refuse a fractional limit on
  an integer attribute with `Exception\Structure`, before anything is written. Both accept the same whole-number
  forms (for example `5`, `'5'`, `'5.0'` and `5.0`). See [Documents](UPGRADE.md#documents).
- Scopes such as `Authorization::skip()`, `silent()`, `skipRelationships()`, `withTenant()` and the other scoped
  toggles apply to the calling coroutine and the coroutines it starts, not to other coroutines sharing the handle.
  See [Coroutines](UPGRADE.md#coroutines).
- `Mirror::onError()` callbacks receive a `Mirror\Failure`. See [Mirror](UPGRADE.md#mirror).
- `Adapter\Pool` extension points renamed: `syncBorrowed()`, `releaseBorrowed()`, `hostname()` and `$arguments`.
  See [Pool extension points](UPGRADE.md#pool-extension-points).

### Added

- **Joins.** `Query::join($collection, $alias, $on)`, `leftJoin()`, `rightJoin()` and `fullOuterJoin()` with a list
  of `Query::on()` conditions and filters, and `crossJoin($collection, $alias)`, on the SQL adapters
  (`Capability::Joins`), in `find()`, `count()`, `sum()` and `getDocument()`.
  - A joined collection is read exactly as a direct `find()` on it would be: every row with a collection-level
    grant, the rows the caller holds document-level read on under document security, and
    `Exception\Authorization` otherwise. Adding a join never changes which main-collection rows are visible.
  - A joined read returns what the same joins return over the documents direct reads of each collection return,
    for every join type and chain of joins: an unreadable document never hides a row an outer join keeps, and a row
    whose only match is unreadable comes back unmatched.
  - Under shared tables every table of a join is limited to the selected tenant before rows are paired, for all join
    types including PostgreSQL's native full outer join; rows without a tenant are never joined. A later right or
    full outer join never pairs with another tenant's rows of an earlier table.
  - A query may declare at most 8 joins (`Too many joins: at most 8 are allowed`), with or without validation
    (`skipValidation()`).
  - Join aliases must be identifiers, unique within the query regardless of case, and different from
    `Query::DEFAULT_ALIAS` and from the key of a relationship attribute of the main collection; anything else throws
    `Exception\Query`. A join alias may use any letter case or be a reserved word.
  - A join without a select returns the main document plus, under each join alias, the joined collection's `$id`
    and attributes as `alias.$id` and `alias.attribute`. The joined collection's internal attributes (`$tenant`,
    `$permissions`, `$sequence`, `$createdAt`, `$updatedAt`) and its relationship attributes that hold a column (the
    side that stores the related document's id) are returned only when a select names them, or, for the internal
    attributes other than `$tenant`, when the select names `alias.*` or the read orders by them. Joined values are
    never returned under a bare attribute name.
  - `select('alias.*')`, alone or next to other selects, returns the joined row as a direct read of the joined
    collection returns it: its `$id`, `$sequence`, `$createdAt`, `$updatedAt`, `$permissions` and attributes, but not
    its `$tenant`. Joined columns named next to `*` (`select(['*', 'alias.$createdAt'])`) are returned with
    everything `*` returns. An order may name a joined attribute by its bare name when only one join's collection
    declares it and the main collection does not; a name several joins declare throws `Exception\Query`.
  - Joined attributes are returned as a direct read of the joined collection returns them: cast to their types and
    passed through every decode filter they declare, so encrypted attributes are decrypted, JSON and arrays decoded
    and datetimes formatted. This applies to the implicit projection and to `select('alias.attribute')` alike. A
    decode filter receives a document built from the joined row: `$id`, `$collection` and the joined attributes the
    query returned (`$sequence` and the other internal attributes only when the query returns them). When an outer
    join matches no row, its attributes are null, whether or not the select names `alias.$id` (outside `distinct()`
    reads, which select only what they name). A cursor taken from a joined result can be passed back with
    `cursorAfter()` or `cursorBefore()`: its joined values are encoded with the joined collection's filters.
  - A column under a join alias (`alias.column`) must be valid on the joined collection for the query type it is
    used in: an attribute the joined collection declares, or an internal attribute the query type accepts on the
    main collection (`alias.$permissions` can be selected but not filtered or ordered by, and `alias.$collection` is
    never accepted). Otherwise `Exception\Query` is thrown. This applies to `find()`, `count()`, `sum()` and to the
    join conditions and selects of `getDocument()`.
  - A join condition compares columns its tables hold: in `Query::on($left, $right)` the left column belongs to the main collection or to a join declared before it, the right one to
    the joined collection. An unknown column, a relationship side that holds no column, or a join named before it is
    declared throws `Exception\Query`.
  - A join's ON list holds `on()` conditions and plain filters (comparisons, ranges, null checks, `contains`,
    `containsAny`, `notContains`, starts/ends-with, and `and()`/`or()` of them); anything else throws
    `Exception\Query` (`Unsupported join ON condition: <method>`) in `find()`, `count()`, `sum()` and `getDocument()`.
  - A cursor over a joined read names the joined row: paging a one-to-many, left, right or full outer join returns
    every row once in both directions, through rows an outer join did not match and rows without a main document. A
    cursor missing an order value is refused by name instead of borrowing the main document's value, and `cursor()`
    over a joined read whose rows lack such a value throws before yielding a row instead of after the first batch. `getDocument()` with a join pairs the lowest-sequence joined row. See
    [Paging a joined read](UPGRADE.md#paging-a-joined-read).
  - With joins, a bare attribute in an aggregate function or `groupBy()` refers to the main collection's attribute
    when the main collection declares it, else to the attribute of the one joined collection that declares it. A
    name no collection declares, or that more than one join declares, throws `Exception\Query`; qualify it with the
    join alias (`alias.attribute`).
  - `search()` and `notSearch()` on a joined attribute require a fulltext index on that attribute of the joined
    collection. An encrypted attribute of a joined collection cannot be filtered.
  - MariaDB, MySQL and SQLite run a full outer join as two queries joined by `UNION ALL`. They accept one full outer
    join per query, and a right join after it has to join on a table joined before the full outer join or on the full
    outer joined table (directly or through other joins); other chains throw `Exception\Query`. PostgreSQL runs full
    outer joins natively and has no such limit. A full outer join combined with a right join reads, under shared
    tables, what a dedicated database reads.
  - On MariaDB, MySQL and SQLite, aggregates, `groupBy()`, `having()`, `distinct()`, ordering and paging over a full
    outer join apply once to the whole joined result, exactly as PostgreSQL's native full outer join does. On these
    engines a `distinct()` query over a full outer join can only be ordered by selected attributes; any other order
    throws `Exception\Query`. Full outer joins on these engines are ordered through internal columns that never
    appear in results, so every attribute and join alias reads back unchanged.
  - `updateDocuments()` and `deleteDocuments()` reject join queries.
  - A `Validator\Queries\Documents` or `Validator\Queries\Document` you build yourself accepts joins and
    aggregations as the `Adapter\Profile` it is given allows (`Capability::Joins`, `Capability::Aggregations`).
    `Validator\Queries\Base::setJoinedCollections()` gives a query validator you build yourself the collections its
    query sets may join. Without it, bare joined attribute names and searches on a join alias are rejected.
- **Aggregations.** `Query::count()`, `countDistinct()`, `sum()`, `avg()`, `min()`, `max()`, the statistical
  aggregates (`stddev()`, `stddevPop()`, `stddevSamp()`, `variance()`, `varPop()`, `varSamp()`), the bitwise
  aggregates (`bitAnd()`, `bitOr()`, `bitXor()`), `groupBy()` and `having()`, through
  `Database::aggregate($collection, $queries)` on the SQL adapters (`Capability::Aggregations`). It returns a list of
  rows (arrays), names an unaliased aggregate `<method>_<attribute>` (`<method>` for `count('*')`) and fires
  `document_aggregate`.
  - In an aggregation query (one with an aggregate or a `groupBy()`), a select may name only the attributes the query
    groups by. Any other selected attribute throws `Exception\Query`, including `$collection`, `$tenant` unless it is
    grouped, a related document's attribute and a join alias's `alias.*`. `*` and relationship wildcards at any depth
    (`key.*`, `parent.child.*`) are accepted and ignored: the rows hold the groups and the aggregates only. A join
    alias cannot equal a relationship key of the main collection, so `key.*` is always the relationship's wildcard.
    This applies to `aggregate()`, `count()` and `sum()`.
  - `having()` conditions follow the filter rules: they compare aggregate aliases or `groupBy` attributes only,
    aliases at the top level, and the fulltext and value-count rules apply inside them.
  - `sum`, `avg`, `stddev*`, `variance` and `var*` require a numeric, non-array attribute, `bitAnd`, `bitOr` and
    `bitXor` an integer one, and `min` and `max` one whose values are ordered (not an array, object, boolean, spatial
    or vector attribute), on joined collections too. Only `count` accepts `*`. Aggregates and `groupBy()` refuse a
    relationship side that holds no column.
  - `Database::sum()` resolves a bare attribute name as `find()` does: the main collection's attribute, else the one
    join that declares it.
  - A joined group sharing its column name with another group of the query is returned under its qualified name
    (`groupBy(['name', 'note.name'])` returns `name` and `note.name`); a joined group alone keeps its bare name.
  - Over an empty result, `count` and `countDistinct` return `0` and every other aggregate returns `null`, on every
    engine.
  - SQLite has no statistical or bitwise aggregate functions: `aggregate()` rejects those aggregates there with
    `Exception\Query`. Aggregate aliases are identifiers of at most 63 characters,
    and an alias cannot repeat another aggregate's alias or the name a grouped attribute is returned under.
  - Internal attributes under a join alias (`note.$id`, `note.$createdAt`, ...) can be grouped by. `$collection` is
    never aggregated or grouped by, and `$tenant` (on the main collection or under an alias) only under shared
    tables. An aggregate over a main attribute named like its own alias reads the main table when a joined
    collection declares the same attribute.
  - A search and a vector query filter an aggregation query; vector distance orders row reads only.
- **`Query::distinct()`** on the SQL adapters. A distinct read is ordered, and paged by a cursor, along its explicit
  orders only; a `search()` and a vector query filter it without ordering it, and its rows carry no `$distance`. On
  PostgreSQL and MySQL a `distinct()` query ordered by an unselected attribute throws `Exception\Query`. A distinct
  row has no `$id`, so a cursor pages a distinct read by its order values: the read needs a `select()` of named
  attributes and an order on each of them, or it throws `Exception\Query`.
- **Query builder.** `Database::from($collection)` returns a utopia-php/query builder over a collection's table,
  `Database::query()` runs a built read and returns a list of `Document`, `Database::mutate()` runs a built write and
  returns the affected row count, and `Database::schema()` returns a schema builder. `from()`, `query()` and
  `mutate()` check no permissions, bypass the caches, run no hooks and throw `Exception\Authorization` unless
  authorization is disabled. Under shared tables every statement a `from()` builder runs stays within the tenant
  selected when it was handed out. `from($collection, $alias)` names the main table by the alias. A SQL adapter's
  `builder()` (`Feature\QueryBuilder`) is the builder `from()` builds on: its `from()` and, once it has read a
  collection, its join methods take collection ids, it reads that one collection only, and `fromTable()` reads a
  table by its stored name before any `from()`.
- **`find()` query cache.** `Database::setQueryCache(new Cache\Query($cache))` caches `find()` results per
  hostname, database, namespace, tenant and collection, and writes through the `Database` invalidate only the scopes
  they write in: under shared tables with tenant-per-document, the scope of each written document's tenant.
  Collection listings are never cached. A cache hit costs 3 cache round trips.
- **Hooks.** `Database::addHook()` registers `Hook\Lifecycle` (side effects on events), `Hook\Decorator` (modifies
  the documents a read or write returns), `Hook\Transform` (rewrites SQL before it runs; `removeTransform()` removes
  one), `Hook\Write` (rows of its own for each document write, such as `Hook\Permissions`), `Hook\Relationships` and
  `Cache\Invalidator`, and throws for any other hook. `removeHook()` unregisters a hook or every hook of a class. A
  hook implementing `Hook\Attachable` receives the database in `attach()`, so `new Relationships()` takes no
  database. A lifecycle hook that also implements `Hook\Named` replaces the hook registered under the same name, and
  one that implements `Hook\Selective` receives only the events its `handles()` accepts.
- **Typed events.** `Hook\Lifecycle::handle(Event\Domain $event)` receives one `final readonly` event object per
  event, under `Event\{Database,Collection,Attribute,Index,Document,Permission}`, with a typed payload; bulk writes
  are `Event\Document\BatchCreated`, `BatchUpdated`, `BatchDeleted` and `BatchUpserted` with their `count` (and
  `created`/`updated` for upserts). New events: `database_update`, `document_upsert`, `document_aggregate`,
  `attribute_rename` and `indexes_create`. An event object is built only when a registered hook handles its event.
  `Event\DispatcherHook` forwards them to listeners per event class and to a PSR-14 dispatcher.
- **Value objects.** `Attribute`, `Index` and `Relationship` are `final readonly`, built by one factory per type
  (`Attribute::string()`, `Attribute::integer(width: IntegerWidth::Bits64)`, `Index::key()`, `Index::ttl()`,
  `Relationship::oneToMany()`, ...), with `fromDocument()`/`toDocument()` for storage and `apply()`/`with*()` for
  changed copies. `Collection::create()` builds a collection, whose `attributes()` and `indexes()` return typed lists.
  `AttributeUpdate`, `CollectionUpdate` and `RelationshipUpdate` describe sparse updates (`Unchanged::Value` keeps a
  default or format). `Format` holds a format name and its options. `Attribute::TYPES` lists the storable column
  types and `Attribute::availableTypes(Adapter\Profile)` narrows it to an adapter. `Attribute::storedType()` and
  `typeFromStored()` convert between `ColumnType` cases and stored type strings. A `float` attribute type
  (`ColumnType::Float`) joins `double`.
- **Schema methods.** `findCollection()`, `collectionExists()`, `createIndexes()` (validates every index, writes the
  definition once), `Database::update()` (renames a database on every adapter), `internalAttributes()` and
  `collectionDefinition(): Collection`. The create and update methods return the stored value object.
  `getSchemaAttributes()` and `getSchemaIndexes()` return `Schema\Column` and `Schema\Index` values, and PostgreSQL
  introspects its schema (`Capability::SchemaIntrospection`, with MariaDB, MySQL and SQLite).
- **Enums.** `Utopia\Database\Event`, `PermissionType`, `RelationshipType`, `RelationshipSide`,
  `RelationshipDeleteAction`, `SetType`, `OperatorType`, `Filter`, `IntegerWidth`, `Unchanged` and `Capability`, and
  from utopia-php/query `Method`, `ColumnType`, `IndexType`, `OrderDirection` and `CursorDirection`.
- **Adapter model.** `Adapter::supports(Capability)`, `capabilities()` and `hasFeature()`; the optional
  `Adapter\Feature` interfaces `Casting`, `Connection`, `QueryBuilder`, `RawQuery`, `Relationships`, `Schemaless`,
  `Spatial`, `Timeouts` and `Upserts`; `Adapter::limits()` (`Adapter\Limits`) and `Database::profile()`
  (`Adapter\Profile`); `Database::setSchemaless()`; `Database::getHostname()`. `Adapter::relaxAttributeRequired()` and
  the protected `Adapter\SQL::getSpatialColumnSrid(): ?int` (the SRID written into spatial column definitions, or
  `null` for a dialect that cannot declare one, such as MariaDB).
- **Filter codecs.** `Filter\Codec` (`name()`, `encode()`, `decode()`), `Filter\Callback` and `Filter\Registry`. The
  `Database` constructor takes a list of codecs, and `setFilters(Filter\Registry)` gives a handle a registry several
  handles can share. A codec applies only to those handles and takes precedence over a global filter of the same
  name (`Database::addFilter()`); the constructor's codecs take precedence over both. A codec named after a built-in
  filter throws `Exception\Duplicate`. Document and query cache keys include each registered codec, by class, or by
  `signature()` for a `Filter\Signed` codec such as `Filter\Callback`.
- **`Adapter\ReadWritePool`.** Sends reads to a read pool and writes to a write pool. Reads stay on the primary for a
  sticky window after a write or a transaction commits (`setStickyDuration()`, default 5000 ms; `setSticky()`), and
  `getDocument(..., forUpdate: true)` and `rawQuery()` always use the write pool. Metadata and configuration calls
  never re-open the window, and `hostname()` always names the write pool's host.
- **Query profiling.** `Database::setProfiling()`, `isProfiling()` and `getProfiler()` with
  `Utopia\Database\Profiler`, which keeps the newest `Profiler::DEFAULT_CAPACITY` (1000) entries (`setCapacity()`,
  `getCapacity()`); `getQueryCount()` and `getTotalTime()` cover every query since the last `reset()`. Pooled
  connections carry the profiler of the handle that borrowed them only while they are checked out. Each
  `Profiler\Log` carries the statement's bound values, collection and operation (`Event` value).
- **`Utopia\Database\PDO::configure()`** for session settings that must survive a reconnect.
- **Documents.** `Document::only()` and `except()`, `fromArray()`, `getArray()`, `getDocument()`, `getDocuments()`
  and the `Document::ID`, `SEQUENCE`, `COLLECTION`, `CREATED_AT`, `UPDATED_AT`, `PERMISSIONS`, `TENANT` and
  `DISTANCE` key constants.
- **`Utopia\Database\Builder\SQLite`**, the query builder the SQLite adapter uses (`dialectBuilder()` returns it). It
  extends `Utopia\Query\Builder\SQLite` and adds `ESCAPE '\'` to every LIKE predicate.
- `Database::cursor()` iterates over a query's matches in batches of `CURSOR_BATCH_SIZE` (100) by default (a
  `limit()` in the queries caps the iteration, an `offset()` or `cursorAfter()` positions the first batch only),
  `Database::rawQuery()` runs a SQL statement as written and returns its rows as documents (like `from()`, `query()`
  and `mutate()` it checks no permissions, applies no tenant scope and runs only inside
  `getAuthorization()->skip()`, otherwise `Exception\Authorization`), `Query::containsString()` matches a substring
  of a string attribute, and the Redis adapter supports upserts.
- `Query::exists()` and `Query::notExists()` run on the SQL adapters. Each value names an attribute of the
  collection that holds a column, or an `alias.attribute` of a join; other names throw `Exception\Query`.
- `Authorization::withRoles()` and `Adapter::withTenant()` scope the roles and the tenant to the calling coroutine,
  and `Hook\Relationships::withEnabled()` and `withCheckExist()` scope the hook's flags the same way.
  `Database::snapshot()`, `Database::withSnapshot()` and `Hook\Relationships::withSnapshot()` are `@internal`.
- Scoped toggles take their value: `withValidation()`, `withFiltering()`, `withPreserveDates()` and
  `withPreserveSequence()`.
- `Database::setCacheWriterTimeout()` bounds how long an unfinished invalidation keeps a collection's document and
  query cache off.
- `Mirror::awaitReplications(?int $timeout = null)` waits until every replication queued through the mirror has
  reached the destination or has been reported to `onError()`, for example before a worker stops. A timeout of 0
  returns at once; a negative one throws.
  `Mirror\Failure` describes a failed replication.
- `Exception\Schema`, the parent of every schema violation, `Exception::$state` (a string code such as a SQLSTATE),
  `Exception\Unique::MESSAGE`, `Exception\Mismatch` (a `Duplicate` for a shared-table column of another type),
  `Exception\Contention` (a `Transaction` for a lock conflict with a concurrent transaction),
  `Exception\Unconfirmed` (a MongoDB commit whose result could not be confirmed), `Exception\Refused` (an adapter
  that returned `false` from a schema change) and `Validator\Structure`'s `storedAttributes` parameter.

### Changed

- A schema change the adapter returns `false` for throws `Exception\Refused`. A refused `renameAttribute()`,
  `renameIndex()` or `updateRelationship()` has one message naming its keys (`Failed to rename attribute 'a' to 'b'`)
  instead of being wrapped like an adapter error (`Failed to rename attribute 'a' to 'b': Failed to rename
  attribute`), and a refused `renameIndex()` no longer tries to complete an earlier rename. These three also let an
  adapter's `Exception\Duplicate` or `Exception\NotFound` through unwrapped, so `renameAttribute()` of a column the
  table lacks throws `Exception\NotFound` on MariaDB and MySQL as documented. `create()`, `update()`, `delete()`,
  `createCollection()` and `deleteCollection()` refuse a `false` too instead of ignoring it, so `update()` and
  `delete()` return `true` whenever they return. See [Errors](UPGRADE.md#errors).
- A filter on a path into an object attribute takes keys of `a-z`, `A-Z`, `0-9`, `_` and `-` only, on every adapter,
  and PostgreSQL refuses any other key also when validation is skipped. See [Queries](UPGRADE.md#queries).
- `createAttributes()` fires `attribute_create` once per attribute and then `attributes_create` once with the list
  (7.x fired `attribute_create` once, with an array, and never fired `attributes_create`).
- `silent($callback, $hooks)` silences the `Hook\Named` hooks with those names. A nested `silent()` never narrows
  the silence around it, and silences apply to the calling coroutine and the coroutines it starts.
- `Authorization::skip()`, `Database::skipRelationships()`, `skipRelationshipsExistCheck()`, `silent()`,
  `withFiltering()`, `skipFilters()`, `withValidation()`, `skipValidation()`, `withPreserveDates()`,
  `withPreserveSequence()`, `withTenant()`, `withRequestTimestamp()`, `ignoreDuplicates()` and
  `Authorization::withRoles()` are scoped to the calling coroutine
  and the coroutines it starts; sibling coroutines sharing the handle or the `Authorization` no longer see them. The
  plain setters (`setStatus()`, `enable()`, `disable()`, `reset()`, `setTenant()`, ...) still change the shared
  value, except inside a scope over the same state (`disable()` inside `skip()`, `setTenant()` inside
  `withTenant()`), including one the coroutine inherited from the coroutine that started it: there the change
  applies to the calling coroutine and the coroutines it starts, and lasts until the scope ends. A coroutine sees a
  scope only while every coroutine between it and the scope's owner is running; work that can outlive its starter
  opens its own scopes. A coroutine cut off from a scope, because a coroutine between it and the scope's
  owner has returned, reads the shared values or a scope opened outside every coroutine, and while a scope over the
  same state is open on the handle its writes stay with it and the coroutines it starts; for `Authorization`,
  `skip()` and `withRoles()` count together. Such a coroutine changes and restores state with those
  scopes, not with a pair of setters. See [Coroutines](UPGRADE.md#coroutines).
- Relationship population reads its chunks of related ids concurrently only on `Adapter\Pool`, inside a coroutine
  and outside `withTransaction()`, and only as many at once as `Hook\Relationships::READ_CONCURRENCY` (4) and
  `Pool::getReadConcurrency()` (the connections the pool can hand out without waiting, less one) allow; elsewhere it
  reads them one after another. Related documents are merged in chunk order.
- Linking an existing many-to-many related document needs update permission on it, as one-to-one, one-to-many and
  many-to-one links already do, on every create and update path and at every nesting depth (7.x needed only read).
  Without it the write throws `Exception\Authorization`. See [Relationships](UPGRADE.md#relationships).
- Inside a coroutine, a `Mirror` replicates `createDocuments()`, `updateDocuments()`, `upsertDocument()`,
  `upsertDocuments()`, `deleteDocument()` and `deleteDocuments()` in a coroutine of
  its own, so the call returns once the source write is done (7.x wrote the destination before returning). Each
  replication runs under the authorization status and roles, tenant, relationship and silence state and toggles
  the caller had when it made the call. A mirror applies its changes to the destination one at a time, in the order
  they were made through it: the calls that reach the destination before returning (`createDocument()`,
  `updateDocument()`, `increaseDocumentAttribute()`, `decreaseDocumentAttribute()`, schema changes, `create()`,
  `delete()`, `exists()`, `setTimeout()`, `clearTimeout()`) first wait for the queued replications, and later
  replications wait for them. The write filters' document hooks run when the replication applies. Outside a coroutine
  every replication finishes before the call returns.
- A `Mirror` uses its source's `Authorization` and leaves the source's and destination's in place; in 7.x it started
  with one of its own, which it also gave the source's adapter.
- `Mirror::createDocument()` returns the document written to the source, as `updateDocument()` does, instead of the
  destination's copy.
- `notContains` on an array attribute excludes documents whose array is NULL or missing on every adapter; SQLite
  and MongoDB used to include them.
- On MongoDB, `startsWith()` and `endsWith()` match at the start and at the end of the value. They matched the
  value anywhere in the string, so `startsWith('foo')` returned `barfoo`. Both stay case-sensitive.
- On SQLite, document ids compare case-insensitively, as on MariaDB: `getDocument('Doc')` finds `doc`.
- An upsert that creates a document applies every operator to the attribute's default as it does for an existing
  document: `dateAddDays()`/`dateSubDays()` shift the date, `arrayFilter()` filters the array, and the maximum or
  minimum of increment, decrement, multiply, divide and power is honoured.
- `Database::setTimeout()` and `clearTimeout()` throw `Adapter does not support timeouts` on SQLite, Memory and
  Redis; 7.x's SQLite ignored them. See [Errors](UPGRADE.md#errors).
- `setSchemaless(true)` throws on an adapter without a schemaless mode (every adapter but MongoDB). 7.x's
  `setSupportForAttributes(false)` returned the requested value on Memory and Redis without applying it.
- Schema calls no longer retry deterministic failures of their metadata write (validation, authorization, missing
  or duplicate documents, limits, conflicts, indexes, dependencies, operators, timeouts, truncation) and no longer
  sleep before rethrowing them. They no longer run a metadata transaction again after its own retries gave up on a
  transient failure either: a lock conflict there is attempted 3 times instead of 9.
- `withTransaction()` no longer retries deterministic failures: an invalid document (`Structure`), `NotFound`,
  `Query`, `Type`, `Index`, `Dependency`, `Truncate`, any other typed library failure and any other exception (for
  example `InvalidArgumentException`) run the callback once and are rethrown without sleeping, also in a nested call.
  Only a failure that can succeed on another attempt is still retried twice with 7.x's backoff: an
  `Exception\Transaction` (including `Contention`), a lost connection, an engine lock conflict the adapter did not
  map, or a MongoDB error labelled transient, a network error before the commit or a command that was never sent.
  Only the outermost `withTransaction()` retries it: a nested call rolls back to its savepoint and rethrows, so a
  lock conflict that keeps failing runs a nested callback 3 times instead of 9. A MongoDB commit whose result is
  unknown retries only the commit, up to 3 more times with a `majority` write concern, then fires the queued
  `document_purge` events of the last attempt and throws `Exception\Unconfirmed` without running the callback again
  (7.x ran the callback again); a schema call keeps the table, column or index of a definition whose commit is
  unconfirmed. A MongoDB commit the server reports aborted, on the first attempt or on a commit retry, runs the
  callback again: 7.x reported a first commit the server had aborted as a success, losing its writes. On SQL a
  connection lost during `COMMIT` still runs the callback again, so that work is at-least-once. See
  [Errors](UPGRADE.md#errors).
- `createCollection()`, `createAttribute()`, `createAttributes()` and `createIndex()` keep the table, column or
  index, and `updateAttribute()`, `renameAttribute()`, `renameIndex()`, `deleteAttribute()` and `deleteIndex()` keep
  their change, when only the cache invalidation after their committed definition failed or its commit is
  unconfirmed, and do not repeat the write. `deleteCollection()` and `deleteRelationship()` recreate the dropped
  table or columns empty in that case and throw the failure wrapped. `createRelationship()` keeps a committed
  relationship and still creates its indexes in that case; when its indexes fail and the definitions cannot be
  removed, the columns are kept with them.
- `createCollection()` validates attribute types up front, like `createAttribute()`, and throws
  `Unknown attribute type: <type>. Must be one of <types>` for an unknown one. `updateAttribute()` updates `id`
  attributes and refuses relationship attributes (`Cannot update relationship as an attribute`).
- An empty attribute `format` (7.x metadata stores `''`) reads as `null`.
- `Database::VAR_BIGINT` (`'bigint'`) becomes `ColumnType::BigInteger`, whose value is `'biginteger'`. Collection
  metadata still stores `'bigint'`, as in 7.x, so existing metadata needs no migration. Write a stored type with
  `Attribute::storedType()` and read one with `Attribute::typeFromStored()`.
- `updateAttribute()` with `required: false` relaxes the column's `NOT NULL`; 7.x's `updateAttributeRequired(false)`
  changed only the metadata. `AttributeUpdate(default: null)` removes the default, `format: null` removes the
  format, and a default sent with `required: true` throws. A `Mirror` replicates attribute updates.
- A 64-bit integer attribute (`IntegerWidth::Bits64`, 7.x size 8) gets the bounds `[PHP_INT_MIN, PHP_INT_MAX]`, or
  `[0, PHP_INT_MAX]` unsigned, in `increaseDocumentAttribute()`, `decreaseDocumentAttribute()` and the numeric
  operators; 7.x applied the 32-bit range.
- PostgreSQL introspects its schema and reconciles orphan columns and indexes in `createAttribute()` and
  `createIndex()`, as MariaDB, MySQL and SQLite do.
- An unaliased aggregate is returned under `<method>_<attribute>`, or `<method>` for `count('*')`.
- `cursor()` treats a `limit()` as a cap on the documents it yields.
- `Adapter\Mongo::exists()` reports a database only when the server lists it (it returned `true` for every name),
  and `collectionExists()` looks in the database it is given.
- `Database::delete()` still flushes the whole cache it was given, as in 7.x: on a cache shared with other databases
  (one Redis), give each database handle a cache of its own.
- An index of an unknown stored type is read as a key index; creating one is refused.
- `Connection::hasError()` classifies by driver error code (MySQL and MariaDB 1053, 2002, 2006, 2013 and 4031;
  SQLSTATE class 08; PostgreSQL 57P01 to 57P05) before matching messages, and its message list matches Swoole 6.2.
- `Database::setMetadata()` comments precede every statement the SQL adapters prepare, ahead of registered
  `Transform` hooks. Arrays, `null` and objects are rendered as JSON. Keys and values are normalised: comment
  delimiters are split, control characters become spaces and invalid UTF-8 is replaced.
- `purgeCachedQueries()` also purges the `find()` query cache, and returns `false` when either purge fails.
- `deleteDocument()` fires `document_update` for each document on the other side of a two-way relationship that the
  delete changed, as 7.4.0 does. When a hook throws, `document_delete` and every related `document_update` still
  fire, and the first exception reaches the caller afterwards. Finding the peers a cascade left costs a read per
  related collection, which a delete skips unless an active lifecycle hook handles `document_update`. See
  [`document_update` for related documents a delete changed](UPGRADE.md#document_update-for-related-documents-a-delete-changed).
- The locked read of `updateDocument()`, `increaseDocumentAttribute()`, `decreaseDocumentAttribute()` and
  `deleteDocument()`, and the page reads of `updateDocuments()` and `deleteDocuments()`, use the collection
  definition the write resolved instead of reading it again: a schema change committed after the write resolved its
  definition, or between two pages, is not seen by that read or those pages, as the write validated and encoded
  under the earlier one. A bulk write reads `_metadata` at most once per call instead of once per page. A `Database`
  subclass's `getDocument()` and `find()` overrides are no longer called for these reads, and its `getCollection()`
  and `findCollection()` overrides are not called by `getDocument(Database::METADATA, ...)` while no
  `Hook\Decorator` is registered or events are silenced. See [Caches](UPGRADE.md#caches).
- `Adapter\Pool::syncBorrowed()` replays metadata, the profiler and transforms onto a connection only when the
  connection's differ from the pool's, as it already did for write hooks; the tenant, schemaless mode, timeouts and
  the other settings are still synced on every delegated call. A `Pool` subclass or custom connection adapter can
  no longer rely on `resetMetadata()`, `setMetadata()`, `setProfiler()`, `resetTransforms()` or `addTransform()`
  being called on every delegated call. See [Pool extension points](UPGRADE.md#pool-extension-points).

### Deprecated

- `Query::contains()`. Use `containsString()` for substring matching on string attributes and `containsAny()` for
  array attributes. Queries whose method is `contains` (for example parsed from JSON) keep working.

### Removed

- `Database::on()`, `Database::before()`, `Mirror::on()` and `Adapter::before()`.
- The protected `Adapter\Pool::$pinnedAdapter` property. A subclass reads the connection the calling coroutine's
  transaction pinned through `pin()`. See [Pools and profiling](UPGRADE.md#pools-and-profiling).
- The `Database::VAR_*`, `INDEX_*`, `ORDER_*`, `PERMISSION_*`, `RELATION_*`, `CURSOR_*` and `EVENT_*` constants, the
  `Query::TYPE_*` constants, the `Operator::TYPE_*` constants and the `Document::SET_TYPE_*` constants, replaced by
  enums, and `Database::SPATIAL_TYPES`, `ATTRIBUTE_FILTER_TYPES`, `INTERNAL_ATTRIBUTES`, `INSERT_BATCH_SIZE` and
  `DELETE_BATCH_SIZE`. See [Constants are now enums](UPGRADE.md#constants-are-now-enums).
- `Utopia\Database\Helpers\*`, `Mirroring\Filter`, `Validator\Attribute`, `Validator\Index`, `Validator\Queries`,
  `Validator\IndexedQueries` and `Validator\ObjectValidator`, moved without aliases. See
  [Namespaces](UPGRADE.md#namespaces).
- `updateAttributeRequired()`, `updateAttributeFormat()`, `updateAttributeFormatOptions()`,
  `updateAttributeFilters()`, `updateAttributeDefault()` and `getInternalAttributes()`. See
  [Schema: value objects](UPGRADE.md#schema-value-objects).
- `upsertDocumentsWithIncrease()`, `iterate()`, `foreach()` and the `$onError` parameters. See
  [Bulk writes and reads](UPGRADE.md#bulk-writes-and-reads).
- `enableValidation()`, `disableValidation()`, `enableFilters()`, `disableFilters()`, `enableLocks()`,
  `getPreserveDates()`, `getPreserveSequence()`, `getDropUnknownAttributes()`, `getSharedTables()`,
  `getTenantPerDocument()`, `skipDuplicates()`, `clearAllDocumentTypes()`, `getKeywords()` and
  `Authorization::setDefaultStatus()`. See [Configuration toggles](UPGRADE.md#configuration-toggles).
- `Document::getRead()`, `getCreate()`, `getUpdate()`, `getDelete()`, `getWrite()`, `find()`, `findAndReplace()`,
  `findAndRemove()` and the `getArrayCopy()` filter parameters. See [Documents](UPGRADE.md#documents).
- The 51 `Adapter::getSupportFor*()` methods and `getSupportNonUtfCharacters()`. See
  [Capabilities and features](UPGRADE.md#capabilities-and-features).
- The 16 per-limit adapter getters (`getLimitFor*()`, `getMax*()`, `getCountOfDefault*()`, `getIdAttributeType()`,
  `getDocumentSizeLimit()`, `getKeywords()`, `getInternalIndexesKeys()`). See
  [Limits and profile](UPGRADE.md#limits-and-profile).
- The base adapter's `ping()`, `reconnect()`, `getHostname()`, `getConnectionId()`, `setSupportForAttributes()`,
  `clearTimeouts()`, `enableAlterLocks()`, `setDebug()`, `getDebug()`, `resetDebug()`, `castingBefore()`,
  `castingAfter()`, `setUTCDatetime()`, `decodePoint()`, `decodeLinestring()` and `decodePolygon()`, and the
  per-adapter methods listed in [Methods an adapter no longer has](UPGRADE.md#methods-an-adapter-no-longer-has):
  `Mongo::getSchemaAttributes()` and `getSchemaIndexes()` answer `[]`, `Mongo::getConnectionId()` is `id()`, and
  `Memory::upsertDocuments()` is gone with `Feature\Upserts`.
- `SQL::getPDOAttributes()` and the protected `SQL::getPDO()` (use `getDriver()`). `Change::getOld()`, `setOld()`,
  `getNew()` and `setNew()` (use the readonly `old` and `new`).
- The SQL adapters' string query builders and the hooks behind them (`getSQLConditions()`, `getSQLCondition()`,
  `getSQLPermissionsCondition()`, `getFulltextValue()`, `getTenantQuery()`, `getLikeOperator()`,
  `getRegexOperator()`, `getAttributeProjection()`, `getRandomOrder()`, the insert and upsert statement hooks, and
  others). See [Removed adapter methods](UPGRADE.md#removed-adapter-methods).
- `Mongo::getTenantQuery()`, which returned an empty string, and `SQL::getSpatialTypeFromWKT()`. Nothing in the
  library calls either. See [Removed adapter methods](UPGRADE.md#removed-adapter-methods).
- The protected adapter methods nothing called: `getSQLIndexType()` on `SQL` and `SQLite`,
  `Postgres::getSQLSchema()`, `Postgres::encodeArray()`, `Postgres::decodeArray()` and
  `Memory::unregisterRelationshipField()`. See [Removed adapter methods](UPGRADE.md#removed-adapter-methods).
- `Adapter\SQL::setFloatPrecision()`, `Adapter\SQLite::setEmulateMySQL()`, `getEmulateMySQL()`,
  `Database::getInstanceFilters()`, `Mirror::getWriteFilters()` and `Validator\Structure::getFormats()`, which
  nothing calls. See [Removed unused public methods](UPGRADE.md#removed-unused-public-methods).
- Renamed protected adapter methods with upper-case acronyms or abbreviations (`getSQLTable()`, `getSQLType()`,
  `getOperatorSQL()`, `bindOperatorParams()`, `getIdentifierQuoteChar()`, ...) and `Redis::tx()`. See
  [Writing or subclassing an adapter](UPGRADE.md#writing-or-subclassing-an-adapter).

### Fixed

- `deleteRelationship()` recreates the relationship's indexes, or its junction collection's definition, when the
  adapter refuses or fails to drop the relationship, instead of leaving them dropped while the relationship stays.
- On MongoDB, `createCollection()` of an existing collection throws `Exception\Duplicate`, as on the SQL adapters,
  instead of adopting it without its indexes, unless tables are shared or it is the metadata collection, which it
  reuses after creating its indexes again. A collection it created but could not give its indexes is dropped,
  outside a transaction and unless tables are shared, and a refused `createCollection()` stores no definition.
- `createAttributes()` drops the columns it created one at a time before a later column failed, instead of leaving
  them in the schema without a definition.
- `updateRelationship()` completes an earlier attempt only when every column it renames is already under its new
  name and no longer under its old one, instead of adopting any column under the new key.
- On PostgreSQL with shared tables, `renameIndex()` no longer reports a rename done because another tenant's copy
  of the index has either name; only the tenant's own index, or the one created with the shared table, counts.
- `updateDocument()` no longer fails with `Exception\Structure` (`Unknown attribute`) when a schema change commits
  between its collection definition lookup and its locked read of the document.
- `Mirror::clearDocumentTypes()` keeps the metadata collection's `Collection` type, so the mirror's
  `getCollection()` still returns a `Collection` (it returned a plain `Document`).
- `Adapter::hasFeature()` no longer tells PHPStan the adapter implements the feature: a `Pool` answers `true` for a
  feature it forwards without implementing it, so code narrowed by the check called methods the pool does not have.
  A spatial index created through a pooled adapter is no longer refused by an `instanceof` check.
- `sum()` and `count()` throw `Exception\Query` for a `$max` of 0 or less instead of returning 0.
- A relationship with a stored `onDelete` of `setDefault` or `noAction` is refused with `Exception\Relationship`;
  it was accepted, and deleting the parent left dangling children.
- A join's ON list holding anything but `on()` conditions and filters (`limit()`, `select()`, ...) throws when the
  query is built, and the join keeps its alias (utopia-php/query 0.7).
- `Collection::create(documentSecurity: false)` stores `false` (it read back `true`), and a collection's other keys
  are either stored or refused with `Exception\Structure`, never dropped.
- A `Mirror` honours the `Hook\Relationships` it is given (for example `prepare: false`) on its source and its
  destination; it rebuilt the hook with the default settings.
- In `deleteDocuments()`'s `onNext`, the second argument is a separate copy of the stored document, so a callback
  that changes the first argument no longer changes what it is told the previous state was.
- `findOne()` fires nothing when no document matches (it fired `document_find` with `false`), and `aggregate()`
  fires `document_aggregate` instead of `document_find`.
- `addHook()` throws for a hook it does not recognise instead of registering nothing.
- `increaseDocumentAttribute()` and `decreaseDocumentAttribute()` throw `Exception\Type` for a change value of 0 or
  less instead of `InvalidArgumentException`.
- `MySQL::getSpatialSqlType()` is protected, as on the other SQL adapters; MySQL made it public.
- UPGRADE.md, README.md, AGENTS.md and docs/add-new-adapter.md no longer misstate the last 7.x release (7.4.0), the
  query validator parameters, the adapter contract and the project layout.
- A well-known-text polygon with holes decodes into its rings on MariaDB, MySQL and PostgreSQL; the rings were
  merged into one.
- Deleting a many-to-many relationship on Redis and Memory drops its junction collection, as the SQL and MongoDB
  adapters do; recreating the relationship hit the leftover junction.
- The point, linestring and polygon filters no longer swallow an encoder failure and write the value as given.
- `aggregate()` throws `Exception\Query` for two aggregates that would come back under the same default alias,
  instead of collapsing their values into one key when validation is off.
- A `Hook\Relationships` added to a second database throws instead of rebinding to it, and a query cache shared by
  several databases uses each caller's cache name and writer timeout instead of the last one it was set on.
- `Mirror\Failure` names `DocumentUpsert` for a failed `upsertDocument()` (it was `null`) and `IndexesCreate` for a
  failed `createIndexes()`.

- With validation skipped, the SQL adapters filter the attribute of `sum()` and the grouped columns they qualify
  with a join alias, and every identifier they quote escapes the quote character.
- A delete retried on the same `Database` after its cascade failed (for example on a `Restricted` related document
  or a permission failure) now runs the cascade.
- `Mirror` forwards `setCacheName()`, `setGlobalCollections()`, `resetGlobalCollections()`,
  `setTenantPerDocument()`, `setCacheWriterTimeout()`, `setTimeout()`, `clearTimeout()`, `setMetadata()`,
  `resetMetadata()`, `setFiltering()`, `withFiltering()`, `skipFilters()`, `setLocks()`, `setProfiling()`,
  `setMigrating()` and `setFilters()` to its source and destination.
- Scopes entered on a `Mirror` apply to its source: `withTenant()`, `withPreserveDates()` and
  `withPreserveSequence()` to its destination as well, `skipRelationships()`, `skipRelationshipsExistCheck()` and
  `withRequestTimestamp()` to the source only. `withRequestTimestamp()` runs its callback once.
- `Mirror::setLocks()` reports a destination that cannot apply the setting through `onError()` (method
  `setLocks`) instead of throwing after the source applied it, and `Mirror::create()` throws when the destination
  cannot create the database.
- A `Mirror` write filter whose `beforeCreateCollection()`, `beforeUpdateCollection()`, `beforeUpdateAttribute()` or
  `beforeCreateIndex()` returns `null` skips that change on the destination, as `Mirror\Filter` documents; the
  mirror returns the source's result, and a collection whose creation was skipped is not replicated.
- `Mirror::createDocument()` and `updateDocument()` restore the destination's preserve-dates setting after
  replicating, also when the destination write fails.
- `Mirror::upsertDocument()` and an increasing `Mirror::upsertDocuments()` run on the source and replicate to the
  destination.
- `createCollection()`, `createAttribute()` and `createAttributes()` no longer modify the `Attribute` and `Index`
  objects passed to them.
- `resetMetadata()` removes the query comments at once; the previous metadata no longer annotates later statements.
- Without Swoole's library, MySQL 8.0.24+'s idle disconnect (error 4031) is recognised as a lost connection and
  reconnected, instead of failing the first query after every idle period.
- Linking a related document at any nesting depth of an update needs update permission on that document, and throws
  `Exception\Authorization` without writing anything when the caller lacks it.
- `deleteDocuments()` with a select no longer skips a one-to-one or one-to-many `Cascade` or ignores `Restrict` on
  one-to-one, one-to-many and many-to-many relationships. Cascade and restrict targets are read from storage with
  permissions skipped, as `SetNull` already did, so related documents the caller cannot read are cascaded (subject
  to the caller's delete permission on each) or block a `Restrict` delete.
- A nested one-to-one write that throws no longer leaves an entry on the relationship write stack. Before, every
  later write on the same `Database` treated its nested relationships as one level deeper and dropped the deepest
  ones without an error.
- Coroutines sharing a `Database` keep their own relationship write and cascade stacks (also in 7.x). Before, a
  coroutine in the middle of a nested relationship write made another coroutine's nested writes look deeper, so their
  deepest related documents were dropped without an error, and a coroutine in the middle of a cascading delete could
  stop another coroutine's cascade, leaving its related documents behind.
- `Adapter\Pool` pins a transaction's connection for the coroutine that opened the transaction and the coroutines it
  starts (also in 7.x). Before, every coroutine sharing the handle ran its statements on that connection while the
  transaction was open, so they were committed or rolled back with it and their own `withTransaction()` became a
  savepoint in it. Each call on the pinned connection also runs under its own coroutine's tenant; before, two
  coroutines with different `withTenant()` scopes overwrote each other's tenant on it.
- A filter on a nested relationship path (for example `Query::equal('children.tags.name', [...])`) no longer throws
  `Exception\Query` when a step of the path matches more documents than `getMaxQueryValues()`: each step reads its
  matches in chunks within the limit. A path that passes through the parent side of a one-to-many or the child side
  of a many-to-one no longer throws `Cannot select attributes`.
- Linking a document by value in a two-way one-to-one update stores the back-reference on the related document, and
  writes its nested documents as deep as `createDocument()` does.
- A new related document created through a many-to-many update keeps the `$permissions` it was given. Before, they
  were always replaced by the parent's. A related document created without `$permissions` still takes the parent's.
- A nested `withTransaction()` whose enclosing transaction was lost (for example, the server ended the session)
  throws `Exception\Transaction`, and so does the outer call. Before, the nested call began a fresh top-level
  transaction and committed its own writes alone while the outer call returned normally. A top-level
  `withTransaction()` whose commit finds the connection no longer holds the transaction throws
  `Exception\Transaction` instead of returning as if its work were stored; `commitTransaction()` no longer returns
  `false` for it.
- A nested `withTransaction()` whose statement lost a deadlock on MariaDB or MySQL rethrows the deadlock
  (`Exception\Contention`), and the outermost call runs again as in 7.x. The engine rolls the whole transaction back,
  so nothing of that attempt is stored. The 8.0 pre-releases reported such a transaction as lost and did not run it
  again.
- `Utopia\Database\PDO` refuses statements after a reconnect lost the open transaction, until the transaction is
  rolled back, so a swallowed connection error can no longer make later statements autocommit.
  `Utopia\Database\PDO::reconnect()` replays attributes set with `setAttribute()` after connecting.
- Increasing or decreasing an optional numeric attribute that was never set stores the result (the unset value
  counts as zero), on every adapter; before, the SQL adapters left the column NULL and MongoDB rejected the update. A
  `max`/`min` bound also treats the unset value as zero.
- On MariaDB and MySQL, `updateDocument()` stores a case-only rename (`abc` → `ABC`) even when it is looked up by the
  new casing, and moves the document's permission rows with it.
- `find()` no longer changes the cursor document passed to `Query::cursorAfter()`/`cursorBefore()`.
- `updateDocuments()` inside `withRequestTimestamp()` compares each stored document's `$updatedAt` with the request
  timestamp, instead of always throwing `Exception\Conflict`.
- `updateDocuments()` hands `onNext` every attribute it wrote decoded, once, including under a `select` that leaves
  the attribute out and after a retried transaction; nothing the `select` left out is added.
- In a two-way one-to-one relationship, `updateDocument()` rejects a document already linked to another document with
  `Exception\Duplicate` (`Document already has a related document`) before writing anything, and no longer rejects
  a free document whose id matches a linked document of the other collection.
- `createDocuments()` under `ignoreDuplicates()` writes permissions only for the documents it inserted. A replayed id
  no longer adds its permissions to the stored document (MariaDB, MySQL and SQLite; also present in 7.x), and a row
  skipped for another unique value leaves no permission rows behind. On MongoDB it matches ids as its `_uid` index
  does (ignoring case and accents), instead of failing on an id stored with different case, and it reports only the
  documents its upserts inserted: it reads back the `$sequence` each was given, so an id the index matches or one
  another writer stores first is not counted. A document it inserts without a `$sequence` gets a UUID v7 one, as
  `createDocuments()` without `ignoreDuplicates()` gives it, instead of a server-generated `ObjectId`.
- `Adapter::find()` with no limit and an offset returns the rows after the offset on every SQL engine instead of
  throwing (MariaDB, MySQL and SQLite rejected `OFFSET` without `LIMIT`; also in 7.x).
- A failed rollback of a metadata write no longer replaces or mislabels the error that failed the write.
- A failed `createCollection()` whose rollback also fails throws the metadata failure (message and previous
  exception) instead of the rollback's error; the rollback failure is logged.
- `createAttributes()` rolls back every column it created when a driver error (for example a lock timeout or a lost
  connection on PostgreSQL) interrupts dropping one of them, and throws the metadata failure with that error
  appended, instead of letting the driver error escape and leaving the remaining columns. A PHP `Error` (for
  example a `TypeError`) raised while dropping a column is rethrown unchanged instead of being collected.
- `renameIndex()` fails with `Failed to rename index '<old>' to '<new>'` and keeps the old key in the metadata when
  the schema has the index under neither name, instead of recording a rename that did not happen. This holds on
  PostgreSQL, MariaDB, MySQL, MongoDB, Memory and Redis; PostgreSQL, Memory and Redis used to report such a rename
  as done. SQLite rebuilds the index under the new name from its definition, so its schema matches the metadata.
  An index the schema already has under the new name completes the rename on every adapter but MongoDB, which
  drops the old index first and fails with the driver's IndexNotFound, as before. Under shared tables a tenant's
  rename also completes while the collection's shared index has either name (on PostgreSQL, while another tenant's
  copy of it does); MongoDB's shared tables are unchanged.
- A failed `updateRelationship()` restores the definitions it had already written (parent, two-way child, junction
  keys) and rethrows the original error. Its rollback reverses the column rename before renaming the indexes back,
  so SQLite, Memory and MongoDB rebuild each index over the column it covers instead of losing it.
- Type mismatch messages spell bigint `bigint`, as stored. Spatial attribute defaults are validated.
- A stored value that newer validation rules reject no longer blocks `updateDocument()` of other attributes, and a
  stored `json` value with a non-string permission no longer makes `getDocument()`, `find()` or `updateDocument()`
  throw, including through mapped document types and the caches.
- `createAttribute()` and `createAttributes()` refuse a varchar of size 0 or above the maximum varchar length, as
  `createCollection()` does and as 7.x did.
- A failed `createCollection()` (a declared index that fails, a timeout, a spatial index with orders, a permissions
  table that fails) drops the tables it created, so the collection can be created again. It used to leave them, and
  later creates failed with `Collection already exists`. When that drop fails as well (for example inside an aborted
  PostgreSQL transaction or after a lost connection), the drop failure is logged and the error that failed the
  create is thrown.
- `createIndex()` compares an index that exists in the schema but not in the metadata with the request (columns,
  prefix lengths, key, unique, fulltext or spatial) on adapters with schema index introspection: a match is adopted,
  a mismatch is dropped and recreated, as in 7.3.12.
- `createIndex()` and `createIndexes()` refuse a unique index over documents that already share a value with
  `Exception\Unique` and keep no metadata for it, as 7.4.1 does. The adapter's error was taken for an index already
  in the schema, so the index was recorded without being built; SQLite threw a raw `PDOException` instead.
- An index key longer than the engine allows throws `Exception\Index` (`Index key length exceeds the maximum`) on
  MariaDB and MySQL, and an index row too large throws `Exception\Limit` (`Index row size exceeds the maximum`) on
  PostgreSQL, as 7.4.1 does. Both threw a raw `PDOException`.
- A document stored before an attribute became required, and so holding null for it, can be updated and upserted
  without a value for that attribute, as 7.4.1 does; every write failed with `Missing required attribute`. Creating a
  document without it, or clearing a stored value, still fails.
- PostgreSQL: `distanceLessThan()` uses the spatial index; `ST_Distance()` alone cannot, so a radius query filtered
  every row. It leads with `ST_DWithin()` on the geometry, or, for a distance in meters from a point to a point
  attribute, with a degree box that holds every point in range. Lines, polygons and boxes that would reach a pole or
  the antimeridian keep the exact comparison only, which stays in every case, so the boundary is still exclusive. 7.x
  carries the same fix after 7.4.1.
- The SQL adapters load on a PHP without ext-swoole, which composer.json only suggests: no native parameter, return
  or property type names a Swoole class, so PHP no longer fails to check `SQL::getDriver()` and the statement methods
  against their parents (`Could not check compatibility ... class Swoole\Database\PDOProxy is not available`). The
  Swoole proxies stay in their docblock types.
- A distance query whose distance is not finite (`NAN`, `INF`, `-INF`) is rejected with `Distance query requires a
  finite distance`. PostgreSQL compares `'NaN'` as above every number, so its index condition and its exact check
  disagreed on such a distance.
- `updateAttribute()` no longer fails on MongoDB, Memory and Redis when a key or unique index covers the attribute
  (the index was compared with itself).
- `Validator\IndexDefinition` rejects an index definition without a type, with an unknown type, or a TTL index
  without `ttl`, as 7.x did; `Index::fromDocument()` reads an unknown stored type as `key` instead of throwing.
  `Validator\IndexDefinition` counts big integer columns (big integer, id, 64-bit integer) as 8 bytes toward the
  maximum index length, and judges an index on a text attribute declared without a size against the engine's text
  maximum instead of 0.
- `analyzeCollection()` refreshes planner statistics on PostgreSQL and SQLite (the collection's table and its
  permissions table) and returns `true`; it returned `false` there. Call it after bulk loads.
- Engine errors map to library exceptions: lock conflicts (MariaDB/MySQL 1213, 1205; PostgreSQL 40P01, 40001,
  55P03; SQLite `database is locked`) to `Exception\Contention`, a subclass of `Exception\Transaction`; MariaDB/MySQL 1146 and 1072, SQLite `no such column`, PostgreSQL 22021, 42883
  and 42P01 naming an alias to `NotFound`, `Character` and `Query`; PostgreSQL's distinct() order error in any server
  language. PostgreSQL `deleteCollection()` of a collection whose table is gone succeeds again, and MariaDB/MySQL drop
  its permissions table too, so the collection can be created again. See [Errors](UPGRADE.md#errors).
- PostgreSQL: `updateAttribute()` on a datetime attribute (a rename through `newKey`, or any change that rewrites the
  column) no longer fails with an undefined `to_timestamp(timestamp)` function, which left the column renamed and
  the metadata unchanged. `createCollection()` with an index on an object path (`data.country`) creates the index on
  the JSON path instead of failing with `Attribute not found`.
- MariaDB: spatial columns are declared without an SRID on every path (`POINT`, `LINESTRING`, `POLYGON`);
  `createCollection()` and `updateAttribute()` wrote `POINT(4326)`, which declared no SRID on MariaDB either.
- Shared tables: `createAttribute()` and `createAttributes()` no longer drop a column another tenant's collection
  uses when the requested type differs. The attribute is refused with `Exception\Duplicate` (`Attribute exists in
  the shared table with another type`; `Exception\Mismatch` on PostgreSQL); a column of the same type is reused,
  including on MariaDB where the engine spells `INT` as `int(11)` and JSON as `longtext`. A failed `createAttributes()`
  metadata write rolls back only the columns that call created. `createIndex()` over an index another tenant uses
  with another definition throws `Duplicate` (`Index exists in the shared table with another definition`).
- Shared tables: renaming an attribute tenant by tenant (`renameAttribute()`, or `updateAttribute()` with a new key)
  completes for every tenant of a collection id on PostgreSQL, MariaDB, MySQL and SQLite. The first tenant renames
  the shared column; each later tenant finds it renamed and only updates its own metadata (and, for
  `updateAttribute()`, applies its type to the renamed column). A rename left half done by a failed metadata write
  also completes when retried on PostgreSQL. On PostgreSQL, `renameIndex()` completes for a tenant whose collection
  uses an index another tenant created.
- Renaming an attribute onto a column that still sits beside the old one (under shared tables, another tenant's
  attribute) throws `Duplicate` (`Attribute already exists`) on every SQL engine and leaves the metadata and values
  unchanged. MariaDB, MySQL and SQLite used to adopt that column and strand the attribute's values in the old one;
  PostgreSQL threw a generic exception.
- A filter on a joined attribute (`alias.attribute`), in the query or in a join's ON list, and a `having()` condition
  are converted as a filter on the main collection is: `contains`, `containsAny`, `containsAll` and `notContains` on
  a joined array match elements rather than substrings (`containsAny('th.tags', ['a'])` no longer matches
  `['banana']`), and joined datetimes, `alias.$createdAt` and `alias.$updatedAt` included, are compared in UTC
  whatever offset the value carries, in `find()`, `count()`, `sum()` and `getDocument()`. A `having()` condition on
  a `min` or `max` alias is compared as the aggregated attribute.
- A select that names a joined attribute (`select(['name', 'alias.name'])`) returns only what it names: the main
  collection's unselected attributes are no longer returned (arrays as `[]`) next to it.
- SQLite: `contains`, `containsAny`, `containsAll` and `notContains` on array attributes compare elements by value
  (strings, integers, doubles, booleans); they previously matched no element. The query builder's `jsonContains`,
  `jsonNotContains` and `jsonOverlaps` filters on SQLite are fixed the same way.
- SQLite: document id lookups, permission checks and joins on `$id` use the unique `_uid` index again; they
  previously scanned the table, and shared-table self-joins could run for hours. `Query::regex()` works again
  through the adapter's `REGEXP` function.
- SQLite: `search()`/`notSearch()` on a joined attribute (`Query::search('alias.attribute', ...)`) use the joined
  collection's fulltext index, so a multi-word term matches as it does on the main collection; a search on an
  attribute without a fulltext index (validation off) matches a backslash in the term literally; `getSchemaIndexes()`
  lists every index under its index id, as MariaDB and MySQL do, and under shared tables lists the indexes on the
  shared table once each; `deleteIndex()` on one of several fulltext indexes no longer throws `Cannot resolve
  fulltext index`.
- SQLite: `arrayRemove()` removes integers and floats, in upserts as well as updates, and an upsert that creates a
  document refuses an operator result outside the 64-bit integer range with `Exception\Limit` (`Value out of
  range`), as MariaDB, MySQL and PostgreSQL do. A full outer join ordered with `Query::orderRandom()` no longer
  fails on SQLite.
- MongoDB: `count()` throws the driver's error (mapped as for `find()`) instead of returning `0` when the query fails
  for a reason other than a timeout, for example an invalid regular expression.
- MongoDB: unique indexes that `createIndex()` creates on integer, big integer, float, boolean and datetime
  attributes reject duplicates; their partial filter required a string value, so they covered no document. Unique
  indexes created by `createCollection()` also cover integers past 32 bits, big integers inside 32 bits and floats
  stored as integers. A unique index on an attribute without a declared type (an internal attribute such as
  `$createdAt`, or any attribute of a schemaless collection) covers a value of every BSON type the driver writes but
  null, arrays, binary data and timestamps included; it required a string, and 7.4.1 covers strings, numbers,
  booleans, dates and objects only. Key indexes are used by queries: their partial filter now requires only that the index's first
  attribute exists. Existing indexes keep their old filter until they are rebuilt: see
  [MongoDB: rebuild key and unique indexes](UPGRADE.md#mongodb-rebuild-key-and-unique-indexes).
- MongoDB: `containsAll()` works on `find()` (it matched nothing there, while `count()` and `sum()` worked); renaming
  or deleting an attribute whose key contains a dot acts on its stored values; `sum()` filters on and sums attributes
  whose key contains a dot; a stored `null` tenant reads back as `$tenant`. `contains`, `notContains`, `notSearch`,
  `notStartsWith` and `notEndsWith` match values containing `$` followed by letters (such as `$USD`) instead of
  throwing. `Adapter\Mongo::createCollection()` returns `true` for a collection that already exists.
- On an adapter without `Capability::OrderRandom` (MongoDB), `orderRandom()` fails validation with `Exception\Query`
  (`Random order is not supported by this adapter`). MongoDB threw a generic `Exception` from the adapter.
- `find()` refuses `Query::distinct()` on adapters without aggregation support (Memory, Redis, MongoDB) with
  `Exception\Query`, also when validation is skipped; MongoDB returned duplicate rows.
- `Operator::power()` accepts a numeric text exponent (`'2'`) on every adapter again, as in 7.x; non-numeric text is
  still refused. A whole-number float limit on an operator (for example `Operator::increment(1, 9.0e18)` on a big
  integer) is compared exactly on the SQL adapters, and honoured exactly on Memory and Redis when the result leaves
  the native integer range; whole-number limits beyond PHP's int range on unsigned big integers and `'100.0'`-style
  strings are accepted.
- Memory adapter: deleting a document by an id in another casing removes its permissions; under shared tables,
  deleting or revoking a document no longer leaves another tenant's same-id document with stale permissions;
  renaming a document that keeps a value in a unique index no longer throws a unique conflict; `listCollections()`
  includes collections created without a tenant, as the SQL adapters do.
- Redis adapter: `updateDocuments()` and upsert updates enforce unique indexes, including collisions within the
  batch; under tenant-per-document, unique indexes are checked within the document's own tenant; dropping a
  collection deletes only its own keys, and `getSizeOfCollection()` and `getSizeOfCollectionOnDisk()` count only
  them (a collection whose id matched a key segment, such as `doc`, reached other collections' permission data).
- Under shared tables with tenant-per-document, `upsertDocuments()` hands each document to `onNext` with its own
  tenant's `$sequence` when one batch creates the same id under several tenants (SQL adapters).
- The MongoDB adapter returns server-generated `ObjectId` sequences of batch writes as strings instead of `''`, and
  resolves the sequences of documents without a tenant under the adapter's tenant.
- The Redis adapter files each document's permission entries under the document's own tenant.

#### Fixed since the 8.0 pre-releases

These fix builds of the `feat-query-lib` branch that preceded 8.0.0. Most of them restore 7.x behaviour, so they do
not change anything for an upgrade from 7.x.

- **Aggregations, distinct and search:**
  - An aggregation query (an aggregate or a `groupBy()`) next to a fulltext `search()` or a vector query, with no
    explicit order, no longer fails in the engine (MariaDB and MySQL 1064 or 1055, PostgreSQL 42803). On MariaDB it
    no longer returns an extra `_relevance` column. The search and the vector query filter an aggregation query;
    vector distance orders row reads only.
  - A `distinct()` read next to a fulltext `search()` returns each distinct selection once. Before, it returned one
    row per relevance value, each with a `_relevance` column (MariaDB, MySQL, PostgreSQL). Next to a vector query it
    no longer fails on PostgreSQL (42P10, reported as `A distinct() query can only be ordered by a selected attribute
    on this database`). Vector distance orders row reads only. A distinct read is ordered, and paged by a cursor,
    along its explicit orders; the search and the vector query only filter it.
  - A fulltext `search()` read paged with a cursor lists every match exactly once. A search only filters, as in 7.x:
    the read is ordered by its explicit orders and then by `$sequence`, and a cursor pages along them. Search results
    are not ranked by relevance and carry no `_relevance` attribute.
  - Aggregates and `distinct()` no longer surface raw engine errors: an ungrouped select or order next to an
    aggregate, an unsupported aggregate on SQLite, an alias PostgreSQL would truncate, or a main attribute
    aggregated under its own name over a join is rejected or resolved before the statement runs. MariaDB and MySQL map errors 1116 and 1191,
    and MySQL 3065 and PostgreSQL 42P10 (a `distinct()` query ordered by an unselected attribute), to
    `Exception\Query`.
  - A vector search ordered by a joined attribute pages with a cursor on PostgreSQL.
  - On SQLite, `count()` and `sum()` next to a fulltext `search()` apply the search instead of throwing.
  - A fulltext `search()` or `notSearch()` term splits into words at every character other than a letter, a number,
    an underscore or whitespace, as in 7.x. On PostgreSQL `search('title', 'foo/bar')` matched nothing and
    `search('title', 'foo,bar')` (or `;`, `?`, `#`, `%`, `=`, `:`) matched only `foo` directly followed by `bar`;
    both again match either word. MariaDB and MySQL bind the same term as 7.x.
  - A `distinct()` read whose select names only joined columns returns each of them once, under its alias, and
    accepts a joined internal attribute (`alias.$id`).
  - A filter on a joined column is checked against the joined collection's attribute as a filter on the main
    collection is checked against its own: a value of the wrong type, a comparison an array attribute does not take,
    or `contains` on a number is rejected as `Exception\Query` instead of failing in the engine (PostgreSQL 22P02,
    22007, 42883) or as `Unknown PDO Type`. Vector queries cannot target a joined attribute.
  - `Validator\Queries\Base` with a `length` caps every nested query group again, as in 7.x.
  - `count()` and `sum()` with filters or document permissions run one flat aggregate over the table, as 7.x did,
    instead of an aggregate over a derived table; only `$max` and joins keep the derived table.
  - `count()` and `sum()` throw `Exception\Query` for a statement the query builder refuses, and the mapped engine
    exception (`NotFound` for a missing table on SQLite) for an error while preparing, as `find()` does, instead of
    raw `Utopia\Query` and `PDO` exceptions.
  - An unaliased `bitAnd()`, `bitOr()` or `bitXor()` over an empty result returns `null` on MariaDB and MySQL, as an
    aliased one does.
- **Query cache:**
  - The `find()` query cache no longer switches off or discards other tenants' (namespaces', databases') cached
    results when one of them writes.
  - `listCollections()` no longer returns a stale listing when a query cache is installed.
  - `Mirror::setQueryCache()` installs the query cache on the source and destination, so writes through a mirror
    invalidate it, and an `Invalidator` added through a mirror is installed on the mirror too.
  - Under shared tables with tenant-per-document, a write refreshes the cached `find()` results of each written
    document's tenant, whichever tenant is selected on the writing `Database`, and leaves every other tenant's in
    place.
  - A written document's own attribute named `options` no longer names a collection to invalidate.
  - A write to one document no longer retires every cached document of its collection, and single-document writes
    no longer block the collection's cache while they run. `updateDocument()` invalidates the cache once instead of
    twice.
  - Reads and writes no longer leave a key behind in Redis each: a document has one key, with a field per selection,
    as in 7.x, and a collection's `find()` results live in one hash of at most `Cache\Query`'s `slots` fields (1024
    by default), which an invalidation retires by epoch without deleting them, so a write costs the same whatever
    the collection has cached. On the Redis adapters a collection's batch and schema invalidations register as
    fields of one `#owners` key; adapters that store no fields keep a key per invalidation, which their purge
    deletes. On Redis, keys matching `*#owner:*`, document entries whose key ends in `:<hash>#<epoch>` and
    query-cache keys matching `*:qcache:*#active:*` left by earlier builds are no longer read and can be deleted.
  - Cache lookups cost one round trip again: a cached `getDocument()` is two round trips (was 12) and
    `getCollection()`, `find()`, `count()` and `sum()` one (was 6) before their query; single-document writes take
    3 each, at or below 7.x's (create 3, update and delete 6, increase and decrease 4).
  - A cached miss for one casing of a document id no longer hides another casing on engines that compare ids
    case-sensitively (PostgreSQL, MongoDB).
  - Reads inside `withTransaction()` are served from the document cache again, except for documents the transaction
    wrote and collections whose cache it retired: a write no longer reads its collection definition from the
    database inside its own transaction. Reads inside a transaction still never write to the cache. A missing
    collection costs one read of `_metadata` again, as in 7.x.
  - Inside `withTransaction()`, a collection definition read under `skipFilters()` and a filtered read of it no
    longer share one copy: a raw `_metadata` read no longer makes later writes in the transaction fail, and a
    `skipFilters()` read after a filtered one returns the raw definition.
  - With `ReadWritePool`, a document or query result served by a read replica is no longer cached, so a lagging
    replica cannot leave an old version in the cache for other handles.
  - A writer killed between blocking and re-enabling a collection's document or query cache no longer keeps that
    cache off until a flush (see `setCacheWriterTimeout()`).
  - `purgeCachedCollection()` also drops the collection's cached `find()` results, `purgeCachedQueries()` returns
    `false` when the cache fails, as documented, instead of throwing, and a query-cache backend error no longer makes
    `find()` throw: it reads the database and logs a warning.
- **Permissions and tenancy:**
  - Under shared tables, `upsertDocuments()` and `upsertDocument()` store their permission rows under the tenant, on
    an adapter with no earlier write and through `Adapter\Pool` alike.
  - On the SQL adapters, under shared tables with tenant-per-document, `upsertDocuments()` and
    an increasing `upsertDocuments()` remove a revoked permission under the upserted document's own tenant.
  - `listCollections()` and `find(Database::METADATA)` return only the definitions the caller may read, as in 7.x,
    and agree with `count(Database::METADATA)`. Definitions created without a tenant are listed from every tenant of
    a shared pool.
  - The MongoDB adapter filters `find()`, `count()` and `sum()` by document permissions while authorization is
    enabled, whether or not `Hook\Permissions` is registered, and scopes writes and `getDocument()` by tenant only,
    as in 7.x. Writes authorized through update or delete permission succeed without read permission.
  - Under shared tables, the MongoDB adapter upserts each document under its own tenant, falling back to the
    selected one, as in 7.x.
  - A cascade below the first level deletes each related document through `deleteDocument()`, as in 7.x: a related
    document the caller may not read is deleted with the rest, blocks the delete under `Restrict`, and rolls the
    delete back when the caller may not delete it. `updateDocument()` compares a relationship's stored keys at every
    depth before linking.
  - `getDocument()` records a missing document in the document cache only after an unfiltered read confirms it.
  - Document permissions follow every write to `$permissions` (ArrayAccess, references, `exchangeArray()`,
    `unset`), and `getPermissions()` returns a de-duplicated list again.
  - Relationship population that needed more than one query (more related ids than `getMaxQueryValues()`) could leave
    the handle with authorization and relationships disabled, populate related documents the caller may not read, and
    deliver `document_find` events inside `silent()`.
  - Permission-checked reads (`find()`, `count()`, and the raw builder's outer joins under shared tables) work again
    when the database or namespace name starts with a digit or a hyphen; such names are quoted. A permissions table
    or join alias that is unsafe even when quoted (a space, `;`, a quote character, an empty name) is refused with
    `Utopia\Database\Exception`.
  - Scoped toggles (`skipFilters()`, `skipValidation()`, `withPreserveDates()`, `withPreserveSequence()`,
    `withTenant()`, `withRequestTimestamp()`, `ignoreDuplicates()`) no longer reach other coroutines sharing a handle,
    and overlapping scopes in different coroutines no longer leave the handle on another scope's value.
  - A setter called in a coroutine started inside a scope (`addRole()`, `removeRole()` or `cleanRoles()` inside
    `withRoles()`, `disable()` or `reset()` inside `skip()`, `setTenant()` inside `withTenant()`, and the setters of
    the other scoped toggles) changes only what that coroutine and the coroutines it starts see, until the scope
    ends. Before, it changed the shared value: the coroutine that called it did not see the change, every other
    coroutine sharing the handle did, and the change outlived the scope, so a `disable()` inside `skip()` left
    authorization off and an `addRole()` inside `withRoles()` gave every caller the scope's roles.
- **Hooks and events:**
  - Every document write fires `document_purge` again, once per purged document.
  - `silent($callback, $listeners)` no longer silences every hook.
  - `document_purge` for writes inside `withTransaction()` fires after the outer transaction commits instead of
    inside it, and not at all when it rolls back or for a retried attempt. It fires for a committed write even when
    the cache invalidation after the commit fails.
  - A failed nested `withTransaction()` on an adapter without savepoints (MongoDB) no longer drops the
    `document_purge` events of writes that commit with the caller.
  - `deleteCollection()` and `delete()` purge their caches before `collection_delete` and `database_delete` run, and
    `updateRelationship()` fires `attribute_update` after both sides are renamed, so a hook that throws an `\Error`
    leaves no stale cache entry and no metadata that disagrees with the columns.
- **Documents and schema:**
  - A document id of `'unique()'` is stored verbatim again, as in 7.x, including for related documents created
    through relationship attributes. Only an empty id asks the library to generate one.
  - A create that writes its new related documents in one pass files a related document given as an associative
    array under its relationship's collection (MongoDB threw `Duplicate`), leaves a lock conflict to the outermost
    `withTransaction()` to retry, as in 7.x, and no longer applies attribute filters twice when it falls back to
    writing the related documents one by one.
  - `updateDocuments()` with an `Operator` decodes the refetched batch once; `count()` and `sum()` on a missing
    collection throw `Exception\NotFound`; a case-only `$id` rename in `updateDocument()` is applied; and every
    internal metadata write runs Structure validation, as in 7.x.
  - `deleteCollection(Database::METADATA)` succeeds again, as in 7.x: it purges every cached definition before it
    drops the metadata table.
  - Unstorable attribute types (`timestamp`, `serial`, `smallserial`, `bigserial` and the other types no adapter can
    store) are rejected when an attribute is created, including when `createAttribute()` adopts an existing column;
    increments and numeric operators accept integer, bigint, float and double attributes only.
  - A failed column change in `updateAttribute()` leaves the stored definition unchanged: relaxing `required` runs
    before the metadata write.
  - Reads on MongoDB, Memory and Redis return a stored document that has a non-string permission with that entry
    dropped, as the SQL adapters do, instead of throwing `Exception\Structure`.
  - The Redis adapter throws `Exception\Unique`, not a plain `Duplicate`, for unique index violations.
  - Filter codecs stay on the handles that share their `Filter\Registry` instead of replacing global filters.
  - A write no longer throws `Failed to finish document cache invalidation` after it commits when the cache is
    flushed while the write invalidates its collection's cached documents, for example by `delete()` of another
    database that shares the cache.
- **SQL adapters:**
  - A transparent reconnect of `Utopia\Database\PDO` keeps MariaDB and MySQL statement timeouts; the timeout also
    applies to the statement retried after the reconnect.
  - `setMetadata()` values reach the database as query comments again.
  - On SQLite, pattern queries match `_`, `%` and `\` literally again, and index names use the filtered tenant again.
  - `ignoreDuplicates()` on PostgreSQL skips only a stored id again and throws `Unique` for a collision on another
    unique index, as in 7.x.
  - Batch `createAttributes()` works for spatial attributes on MariaDB, required spatial attributes on PostgreSQL
    are nullable columns again, and composite indexes keep the caller's column order.
  - On MySQL, a read with five or more joins no longer spends seconds choosing a join order: the joined collections'
    document permission checks stay subqueries instead of each joining the optimizer's search, which built about ten
    million partial plans for eight checked joins. Reads with up to four joins are planned as before. Outer joins to
    document-security collections no longer run their permission check as a semi-join either (seconds per read, or
    timeouts, with one to four joins).
  - PostgreSQL writes a transaction's statement timeout once, with `SET LOCAL statement_timeout`, and changes it only
    when a statement needs a different one, instead of wrapping every statement of the transaction in `SET LOCAL` and
    `SET LOCAL statement_timeout = DEFAULT`. A timeout scoped to one event still does not reach the other statements
    of the transaction. Outside a transaction the session-level `SET`/`RESET` pair is unchanged.
  - MariaDB and MySQL no longer read the driver to clear a timeout that was never set, so a pooled checkout without a
    timeout issues no statement and does not require a PDO.
  - SQL adapters no longer keep a per-collection spatial column list for the life of the process.
- **Pools:**
  - A borrowed connection keeps its cached limits unless the shared-tables mode changes, and
    `Database::getMinDateTime()` and `getMaxDateTime()` return copies, so a caller can no longer move the limit for
    every validator.
  - Writing a spatial value through `Pool` no longer checks a connection out to encode it.
  - `ReadWritePool` serves reads from the primary after a write or transaction commits, routes locking reads,
    `rawQuery()` and the reads that decide a write (the batch of `updateDocuments()` and `deleteDocuments()`, the
    lookup of `upsertDocuments()`) to the write pool, and no longer keeps reads on the primary after metadata or
    configuration calls.
  - Pooled connections no longer keep the profiler of the handle that last borrowed them.
  - `Adapter\Pool` passes `Database::setLocks()` on to every borrowed connection, and answers capability
    questions without checking a connection out after the first (a cached read no longer needs eight connections).
- **Mirror:**
  - A replicated `upsertDocument()` keeps the source's dates on the destination.
  - A replication no longer changes the caller's authorization status. It runs under the status, roles, tenant,
    relationship and silence state the caller had when it made the call, also after the caller left a `skip()`,
    `skipRelationships()` or `silent()` scope, so a write made inside `skip()` no longer fails on the destination.
  - Changes reach the destination one at a time, in the order they were made through the mirror; a failed
    replication is reported to `onError()` and does not hold back later ones. A write to a document created through a
    relationship, a write made before returning, and a relationship or collection change after queued writes to the
    related collection no longer overtake earlier replications, and replications no longer share the destination's
    connection, preserve-dates or skip-duplicates settings.
  - Outside a coroutine, replications finish before the call returns; before, a destination write that yielded
    never resumed. Synchronous replications inside `withTenant()` use the caller's tenant on the destination.
  - An exception from a write filter's `before*` document hook is reported to `onError()` under the write's method
    and skips that replication, as in 7.x, instead of reaching the caller after the source write.
  - Decorators added through a mirror apply to the documents its writes return and hand `onNext`, not only to reads;
    the destination receives undecorated documents.
- **Performance:**
  - Permission checks no longer use `SELECT DISTINCT` in their subquery (SQLite built a temporary B-tree per read).
  - A delete whose set-null relationship clears related documents keeps them only when `deleteDocument()` reports
    them, for a two-way relationship while an active lifecycle hook handles `document_update`, instead of holding
    every cleared document until the delete returns.
  - The query and document caches list a collection's owner registrations (`HKEYS`) only until the cache shows it
    keeps hash fields, instead of twice per invalidation.
  - SQLite: `createDocument()` reads the new sequence from `PDO::lastInsertId()` instead of a
    `SELECT last_insert_rowid()` statement: one statement fewer per document, as in 7.x.
  - MongoDB: `find()` reads the internal attribute definitions once per process instead of once per returned row, and
    skips list keys when it restores stored field names.
  - MongoDB: `find()`, `createDocuments()`, `updateDocuments()` and `upsertDocuments()` cast a page or batch of
    documents in one `castAfter()` call, one pool checkout instead of one per document.
  - MariaDB and MySQL: a one-to-many joined read breaks ties on the joined `$id` only when its rows show the join or a
    cursor is used. A left-joined read with a limit, no filter or search on a joined attribute and a main order
    (`$sequence` or `$id` among them) joins only the page of main documents it can reach, fulltext searches on main
    attributes included, instead of sorting the whole join. Inner, right and full outer one-to-many joins, filters and
    searches on joined attributes and orders that start with a joined attribute still sort the whole join. See
    [Joins](UPGRADE.md#joins).
  - MySQL: under shared tables, an inner or left join whose own ON no index of the joined collection serves reads the
    tenant's rows of the joined table once, matching its tenant as a range (`_tenant >= ? AND _tenant <= ?`),
    instead of looking them all up again for every row the join pairs (50 s to 0.6 s for four such joins over 50 000
    rows). Joins an index serves, other join types and reads spanning several tenants are unchanged.
- **Tooling:**
  - The `bin/` tasks (`load`, `index`, `query`, `relationships`, `operators`) start again: `bin/cli.php` no longer
    registers a resource with a class the locked `utopia-php/di` does not have, and it loads the autoloader relative
    to itself. `bin/query` builds its connection with `Utopia\Database\PDO`, like the other tasks.

### Known limitations

- A transaction begun on the adapter directly (`getAdapter()->startTransaction()`) is not an invalidation scope: each
  write inside it invalidates the caches and fires `document_purge` before that transaction commits. Use
  `withTransaction()`.
- A join on an unindexed attribute is accepted, but on a large collection it can exceed the statement timeout. On
  MariaDB 10.11, which has no hash join and prunes the join orders it plans, a join whose ON columns are unindexed
  can be very slow: index the attributes your join conditions compare.
- On MySQL, left joins on a joined collection's own `$id` can be slow right after the collection is created or
  bulk-loaded, until InnoDB's automatic statistics recalculation has run (seconds, with the default
  `STATS_AUTO_RECALC`). Run `ANALYZE TABLE` after a bulk load.

### Dependencies

- Requires `utopia-php/query` 0.7 and `utopia-php/async` 0.2. `utopia-php/async` requires `opis/closure`, which the
  library itself does not use. `Utopia\Async\Serializer::unserialize()` no longer decodes closure payloads; use
  `Serializer::unserializeTrusted()` for data from a trusted channel.
- utopia-php/query 0.7 has one join form with a required alias (`Query::join($collection, $alias, $on)`), carries
  join and aggregate aliases in a property (`getAlias()`), types cursors as `array|object`, and refuses an ON list
  member other than `on()` or a filter. Its PostgreSQL builder quotes each key of a JSON path it reads or writes, and
  its MongoDB builder rejects an empty field name or one that starts with `$` in a filter.

### Development

- The test suite fails on PHP warnings, notices and deprecations, on risky tests, and on PHPUnit's own notices and
  deprecations, raised in `src/` or `tests/`.
- CI tests utopia-php/cache 5.x: a second image built with `UTOPIA_CACHE_VERSION=^5.1` runs the unit suite and the
  MariaDB adapter tests against it. Test cache doubles declare `save(..., int $ttl = 0)` so they load on 4.x and 5.x.
- A slim PHPStan rule in the library's own CI rejects magic `PDOStatement` property reads and hooked properties on
  the hot paths (PHP 8.5's tracing JIT miscompiles them, php/php-src#22084). Consumers are not affected.
- The `bin/` benchmark tasks register `Hook\Permissions` and `Hook\Relationships`, so they measure the same work as
  7.4.0.
- `composer.json` declares `8.0.x-dev` as the branch alias of `dev-main`, so `"utopia-php/database": "^8.0"` resolves
  before 8.0.0 is tagged (with `"minimum-stability": "dev"` and `"prefer-stable": true` in the root package).

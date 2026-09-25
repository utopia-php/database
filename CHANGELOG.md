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
- `createCollection()`, `createAttribute()`, `createIndex()` and `createRelationship()` take `Collection`,
  `Attribute`, `Index` and `Relationship` models, and `getCollection()` returns a `Collection`. See
  [Schema: typed models](UPGRADE.md#schema-typed-models).
- `Database::on()`, `Database::before()`, `Mirror::on()`, `Adapter::before()` and the `Database::EVENT_*` constants
  are removed: register `Hook\Lifecycle` and `Hook\Transform` hooks with `Database::addHook()`. See
  [Lifecycle events are hooks](UPGRADE.md#lifecycle-events-are-hooks).
- The 51 `Adapter::getSupportFor*()` methods are removed: check `supports(Capability::...)` or
  `hasFeature(Feature\...::class)`. `Adapter` implements the `Adapter\Feature` interfaces, `Adapter\SQLite` extends
  `Adapter\SQL` instead of `Adapter\MariaDB`, and the adapter signatures take models and enum cases. See
  [Adapters](UPGRADE.md#adapters).
- Cache key names changed: do not share a cache between 7.x and 8.0 processes, and flush it after the last 7.x
  process has stopped. See [Caches](UPGRADE.md#caches).
- `Exception\Unique` has the message `Document with the requested unique attributes already exists` on every
  adapter (was `Unique index violation`). On MariaDB and MySQL an unknown column throws `Exception\NotFound`.
  `Database::sum()` throws `Exception\Query` for an attribute that is not a single number. See
  [Errors](UPGRADE.md#errors).
- `Query::DEFAULT_ALIAS` is `table_main` (was `main`), and `Query::groupByType()` returns a `ParsedQuery` object
  (`Query::groupForDatabase()` returns the 7.x array shape).
- `new Document()` and `Document::setAttribute('$permissions', ...)` reject non-string permissions with
  `Exception\Structure`.
- Hook failures follow 7.x event by event, with three differences: an `\Error` always reaches the caller, an isolated
  failure no longer skips the other hooks, and `document_purge` from `updateDocument()` and `deleteDocument()` fires
  after the write's transaction commits. See [Hook failures](UPGRADE.md#hook-failures).

### Added

- **Joins.** `Query::join()`, `leftJoin()`, `rightJoin()`, `fullOuterJoin()` and `crossJoin()`, with conditions
  given inline or with `Query::on()`, on the SQL adapters (`Capability::Joins`), in `find()`, `count()`, `sum()` and
  `getDocument()`.
  - A joined collection is read exactly as a direct `find()` on it would be: every row with a collection-level
    grant, the rows the caller holds document-level read on under document security, and
    `Exception\Authorization` otherwise. Adding a join never changes which main-collection rows are visible.
  - A joined read returns what the same joins return over the documents direct reads of each collection return,
    for every join type and chain of joins: an unreadable document never hides a row an outer join keeps, and a row
    whose only match is unreadable comes back unmatched.
  - Under shared tables every table of a join is limited to the selected tenant before rows are paired, for all join
    types including PostgreSQL's native full outer join; rows without a tenant are never joined. A later right or
    full outer join never pairs with another tenant's rows of an earlier table.
  - A query may declare at most 8 joins (`Too many joins: at most 8 are allowed`).
  - Join aliases must be identifiers, unique within the query regardless of case, and different from
    `Query::DEFAULT_ALIAS`; anything else throws `Exception\Query`. Generated aliases (`j0`, `j1`, ...) never repeat a
    declared one. A join alias may use any letter case or be a reserved word.
  - A join without a select returns the main document plus, under each join alias, the joined collection's `$id`
    and attributes as `alias.$id` and `alias.attribute`. The joined collection's internal attributes (`$tenant`,
    `$permissions`, `$sequence`, `$createdAt`, `$updatedAt`) and its relationship attributes are returned only when
    a select names them. Joined values are never returned under a bare attribute name.
  - Joined attributes are returned as a direct read of the joined collection returns them: cast to their types and
    passed through every decode filter they declare, so encrypted attributes are decrypted, JSON and arrays decoded
    and datetimes formatted. This applies to the implicit projection and to `select('alias.attribute')` alike. A
    decode filter receives a document built from the joined row: `$id`, `$collection` and the joined attributes the
    query returned (`$sequence` and the other internal attributes only when selected). When a join matches no row
    and its `alias.$id` is in the result, its attributes are null. A cursor taken from a joined result can be passed
    back with `cursorAfter()` or `cursorBefore()`: its joined values are encoded with the joined collection's
    filters.
  - A column under a join alias (`alias.column`) must be valid on the joined collection for the query type it is
    used in: an attribute the joined collection declares, or an internal attribute the query type accepts on the
    main collection (`alias.$permissions` can be selected but not filtered or ordered by, and `alias.$collection` is
    never accepted). Otherwise `Exception\Query` is thrown. This applies to `find()`, `count()`, `sum()` and to the
    join conditions and selects of `getDocument()`.
  - A join condition compares columns its tables hold, in both the `join(table, left, right, operator, alias)` and
    the `on()` form: the left column belongs to the main collection or to a join declared before it, the right one to
    the joined collection. An unknown column, a relationship side that holds no column, or a join named before it is
    declared throws `Exception\Query`.
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
  - A `Validator\Queries\Documents` you build yourself accepts joins and aggregations only with its new
    `supportForJoins` and `supportForAggregations` constructor flags, which `Database` sets from the adapter's
    capabilities. `Validator\Queries::setJoinedCollections()` gives a query validator you build yourself the
    collections its query sets may join. Without it, bare joined attribute names and searches on a join alias are
    rejected.
- **Aggregations.** `Query::count()`, `countDistinct()`, `sum()`, `avg()`, `min()`, `max()`, the statistical
  aggregates (`stddev()`, `stddevPop()`, `stddevSamp()`, `variance()`, `varPop()`, `varSamp()`), the bitwise
  aggregates (`bitAnd()`, `bitOr()`, `bitXor()`), `groupBy()` and `having()`, in `find()` on the SQL adapters
  (`Capability::Aggregations`). `Database::aggregate()` is an alias of `find()` for these queries.
  - In an aggregation query (one with an aggregate or a `groupBy()`), a select may name only the attributes the query
    groups by. Any other selected attribute throws `Exception\Query`, including `$collection`, `$tenant` unless it is
    grouped, a related document's attribute and a join alias's `alias.*`. `*` and relationship wildcards at any depth
    (`key.*`, `parent.child.*`) are accepted and ignored: the rows hold the groups and the aggregates only. A
    wildcard whose first segment is a join alias of the query is that alias's wildcard, even when the main collection
    has a relationship with the same key. This applies to `find()`, `count()` and `sum()`.
  - `having()` conditions follow the filter rules: they compare aggregate aliases or `groupBy` attributes only,
    aliases at the top level, and the fulltext and value-count rules apply inside them.
  - `sum`, `avg`, `stddev*`, `variance` and `var*` require a numeric, non-array attribute, and `bitAnd`, `bitOr` and
    `bitXor` an integer one, on joined collections too. Only `count` accepts `*`.
  - Over an empty result, `count` and `countDistinct` return `0` and every other aggregate returns `null`, on every
    engine.
  - Adapters report `Capability::StatisticalAggregates` and `Capability::BitwiseAggregates`, and SQLite reports
    neither: `find()` rejects those aggregates there. Aggregate aliases are identifiers of at most 63 characters,
    and an alias cannot repeat another aggregate's alias or the name a grouped attribute is returned under.
  - Internal attributes under a join alias (`note.$id`, `note.$createdAt`, ...) can be grouped by. `$collection` is
    never aggregated or grouped by, and `$tenant` (on the main collection or under an alias) only under shared
    tables. An aggregate over a main attribute named like its own alias reads the main table when a joined
    collection declares the same attribute.
  - A search and a vector query filter an aggregation query; vector distance orders row reads only.
- **`Query::distinct()`** on the SQL adapters. A distinct read is ordered, and paged by a cursor, along its explicit
  orders only; a `search()` and a vector query filter it without ordering it, and its rows carry no `$distance`. On
  PostgreSQL and MySQL a `distinct()` query ordered by an unselected attribute throws `Exception\Query`.
- **Query builder.** `Database::from($collection)` returns a utopia-php/query builder over a collection's table,
  `Database::execute()` runs a built statement, and `Database::schema()` returns a schema builder. `from()` and
  `execute()` check no permissions, bypass the caches, run no hooks and throw `Exception\Authorization` unless
  authorization is disabled. Under shared tables every statement a `from()` builder runs stays within the tenant
  selected when it was handed out.
- **`find()` query cache.** `Database::setQueryCache(new Cache\QueryCache($cache))` caches `find()` results per
  hostname, database, namespace, tenant and collection, and writes through the `Database` invalidate only the scopes
  they write in: under shared tables with tenant-per-document, the scope of each written document's tenant.
  Collection listings are never cached. A cache hit costs 3 cache round trips.
- **Hooks.** `Database::addHook()` registers `Hook\Lifecycle` (side effects on events), `Hook\Decorator` (modifies
  the documents a read or write returns), `Hook\Transform` (rewrites SQL before it runs; `removeTransform()` removes
  one), `Hook\Write` (row writes, such as `Hook\Permissions`) and `Hook\Relationships`. A lifecycle hook that also
  implements `Hook\Named` replaces the hook registered under the same name. `Event\DispatcherHook` is a lifecycle
  hook with listeners per domain event class (`Event\Document\Created`, `Updated`, `Deleted`,
  `Event\Collection\Created`, `Deleted`).
- **Typed models.** `Collection`, `Attribute`, `Index` and `Relationship`, with factories per type
  (`Attribute::string()`, `Index::key()`, `Relationship::oneToMany()`, ...) and one class per storable attribute type
  in `Utopia\Database\Attribute`. `Attribute::TYPES` lists the storable column types and `Attribute::availableTypes()`
  narrows it to an adapter's capabilities. `Attribute::persistedType()`, `normalizeType()` and `tryNormalizeType()`
  convert between `ColumnType` cases and stored type strings. A `float` attribute type (`ColumnType::Float`) joins
  `double`.
- **Enums.** `Utopia\Database\Event`, `PermissionType`, `RelationType`, `RelationSide`, `SetType`, `OperatorType` and
  `Capability`, and from utopia-php/query `Method`, `ColumnType`, `IndexType`, `Order`, `OrderDirection`,
  `ForeignKeyAction` and `CursorDirection`.
- **Adapter capabilities.** `Adapter::supports(Capability)`, `capabilities()` and `hasFeature()`, and the
  `Adapter\Feature` interfaces. `Adapter::relaxAttributeRequired()` and the protected
  `Adapter\SQL::getSpatialColumnSrid(): ?int` (the SRID written into spatial column definitions, or `null` for a
  dialect that cannot declare one, such as MariaDB).
- **Custom types.** Implement `Utopia\Database\Type\Custom` (`name()`, `encode()`, `decode()`) and register the type
  on a `Utopia\Database\Type\TypeRegistry`. Give the registry to a `Database` with `setTypeRegistry()`, and list the
  type's name in an attribute's `filters`. The type applies only to handles that share that registry. On those
  handles it takes precedence over a global filter of the same name (`Database::addFilter()`). Filters passed to the
  `Database` constructor take precedence over both. `register()` rejects the built-in filter names
  (`Database::DEFAULT_FILTERS`) with `Utopia\Database\Exception\Duplicate`. Document and query cache keys include
  each registered type's class.
- **`Adapter\ReadWritePool`.** Sends reads to a read pool and writes to a write pool. Reads stay on the primary for a
  sticky window after a write or a transaction commits (`setStickyDuration()`, default 5000 ms; `setSticky()`), and
  `getDocument(..., forUpdate: true)` and `rawQuery()` always use the write pool. Metadata and configuration calls
  never re-open the window, and `getHostname()` always names the write pool's host.
- **Query profiling.** `Database::enableProfiling()`, `disableProfiling()` and `getProfiler()` with
  `Profiler\QueryProfiler`, which keeps the newest `QueryProfiler::DEFAULT_CAPACITY` (1000) entries
  (`setCapacity()`, `getCapacity()`); `getQueryCount()` and `getTotalTime()` cover every query since the last
  `reset()`. Pooled connections carry the profiler of the handle that borrowed them only while they are checked out.
- **`Utopia\Database\PDO::configure()`** for session settings that must survive a reconnect.
- **Documents.** `Document::fromStorage()`, which hydrates a stored document like the constructor but drops
  non-string permissions instead of rejecting them, and `fromRow()`, `fromArray()`, `getArray()`, `getDocument()`,
  `getDocuments()` and the `Document::ID`, `SEQUENCE`, `COLLECTION`, `CREATED_AT`, `UPDATED_AT`, `PERMISSIONS`,
  `TENANT` and `DISTANCE` key constants.
- **`Utopia\Database\Builder\SQLite`**, the query builder the SQLite adapter uses (`getBuilder()` returns it). It
  extends `Utopia\Query\Builder\SQLite` and adds `ESCAPE '\'` to every LIKE predicate.
- `Database::cursor()` iterates over a query's matches in batches, `Database::rawQuery()` runs a SQL statement as
  written and returns its rows as documents, `Query::containsString()` matches a substring of a string attribute,
  and the Redis adapter supports upserts.

### Changed

- `createAttributes()` fires `attribute_create` once per attribute, with a `Document` payload, and then
  `attributes_create` once with the list (7.x fired `attribute_create` once, with an array, and never fired
  `attributes_create`).
- `silent($callback, $listeners)` silences the `Hook\Named` hooks with those names. A nested `silent()` never narrows
  the silence around it, and silences apply to the calling coroutine only.
- `createCollection()` validates attribute types up front, like `createAttribute()`, and throws
  `Unknown attribute type: <type>. Must be one of <types>` for an unknown one. `updateAttribute()` updates `id`
  attributes and refuses relationship attributes (`Cannot update relationship as an attribute`).
- `Database::ATTRIBUTE_FILTER_TYPES` is renamed `Database::ATTRIBUTE_FILTER_COLUMN_TYPES` and holds `ColumnType`
  cases.
- An empty attribute `format` (7.x metadata stores `''`) reads as `null`.
- `Database::VAR_BIGINT` (`'bigint'`) becomes `ColumnType::BigInteger`, whose value is `'biginteger'`. Collection
  metadata still stores `'bigint'`, as in 7.x, so existing metadata needs no migration. Write a stored type with
  `Attribute::persistedType()` and read one with `Attribute::normalizeType()`.
- `Connection::hasError()` classifies by driver error code (MySQL and MariaDB 1053, 2002, 2006, 2013 and 4031;
  SQLSTATE class 08; PostgreSQL 57P01 to 57P05) before matching messages, and its message list matches Swoole 6.2.
- `Database::setMetadata()` comments precede every statement the SQL adapters prepare, ahead of registered
  `Transform` hooks. Arrays, `null` and objects are rendered as JSON. Keys and values are normalised: comment
  delimiters are split, control characters become spaces and invalid UTF-8 is replaced.
- `purgeCachedQueries()` also purges the `find()` query cache, and returns `false` when either purge fails.

### Deprecated

- `Query::contains()`. Use `containsString()` for substring matching on string attributes and `containsAny()` for
  array attributes. Queries whose method is `contains` (for example parsed from JSON) keep working.

### Removed

- `Database::on()`, `Database::before()`, `Mirror::on()` and `Adapter::before()`.
- The `Database::VAR_*`, `INDEX_*`, `ORDER_*`, `PERMISSION_*`, `RELATION_*`, `CURSOR_*` and `EVENT_*` constants, the
  `Query::TYPE_*` constants (except `TYPE_ELEM_MATCH`), the `Operator::TYPE_*` constants and the
  `Document::SET_TYPE_*` constants, replaced by enums. See [Constants are now enums](UPGRADE.md#constants-are-now-enums).
- The 51 `Adapter::getSupportFor*()` methods. See
  [Capabilities and feature interfaces](UPGRADE.md#capabilities-and-feature-interfaces).
- The SQL adapters' string query builders and the hooks behind them (`getSQLConditions()`, `getSQLCondition()`,
  `getSQLPermissionsCondition()`, `getFulltextValue()`, `getTenantQuery()`, the insert and upsert statement hooks,
  and others). See [Removed adapter methods](UPGRADE.md#removed-adapter-methods).

### Fixed

- A delete retried on the same `Database` after its cascade failed (for example on a `Restricted` related document
  or a permission failure) now runs the cascade.
- `Mirror` forwards `setCacheName()`, `setGlobalCollections()`, `resetGlobalCollections()`,
  `setTenantPerDocument()`, `setTimeout()`, `clearTimeout()`, `setMetadata()`, `resetMetadata()`, `enableFilters()`,
  `disableFilters()`, `skipFilters()`, `enableLocks()`, `enableProfiling()`, `disableProfiling()`, `setMigrating()`
  and `setTypeRegistry()` to its source and destination.
- Scopes entered on a `Mirror` (`withTenant()`, `withPreserveDates()`, `withPreserveSequence()`,
  `skipRelationships()`, `skipRelationshipsExistCheck()`, `withRequestTimestamp()`) apply to its source, and
  `withRequestTimestamp()` runs its callback once.
- `Mirror::upsertDocument()` and `Mirror::upsertDocumentsWithIncrease()` run on the source, replicate to the
  destination and fire `documents_upsert` once to hooks registered through the mirror.
- `createCollection()`, `createAttribute()` and `createAttributes()` no longer modify the `Attribute` and `Index`
  objects passed to them.
- `resetMetadata()` removes the query comments at once; the previous metadata no longer annotates later statements.
- Without Swoole's library, MySQL 8.0.24+'s idle disconnect (error 4031) is recognised as a lost connection and
  reconnected, instead of failing the first query after every idle period.
- Linking a related document at any nesting depth of an update needs update permission on that document, and throws
  `Exception\Authorization` without writing anything when the caller lacks it.
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
  - A `distinct()` read whose select names only joined columns returns each of them once, under its alias, and
    accepts a joined internal attribute (`alias.$id`).
  - A filter on a joined column is checked against the joined collection's attribute as a filter on the main
    collection is checked against its own: a value of the wrong type, a comparison an array attribute does not take,
    or `contains` on a number is rejected as `Exception\Query` instead of failing in the engine (PostgreSQL 22P02,
    22007, 42883) or as `Unknown PDO Type`. Vector queries cannot target a joined attribute.
  - `Validator\Queries` with a `length` caps every nested query group again, as in 7.x.
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
- **Permissions and tenancy:**
  - Under shared tables, `upsertDocuments()` and `upsertDocument()` store their permission rows under the tenant, on
    an adapter with no earlier write and through `Adapter\Pool` alike.
  - On the SQL adapters, under shared tables with tenant-per-document, `upsertDocuments()` and
    `upsertDocumentsWithIncrease()` remove a revoked permission under the upserted document's own tenant.
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
- **Hooks and events:**
  - Every document write fires `document_purge` again, once per purged document.
  - `silent($callback, $listeners)` no longer silences every hook.
  - `deleteCollection()` and `delete()` purge their caches before `collection_delete` and `database_delete` run, and
    `updateRelationship()` fires `attribute_update` after both sides are renamed, so a hook that throws an `\Error`
    leaves no stale cache entry and no metadata that disagrees with the columns.
- **Documents and schema:**
  - A document id of `'unique()'` is stored verbatim again, as in 7.x, including for related documents created
    through relationship attributes. Only an empty id asks the library to generate one.
  - `updateDocuments()` with an `Operator` decodes the refetched batch once; `count()` and `sum()` on a missing
    collection throw `Exception\NotFound`; incrementing an unset optional number treats it as `0`; a case-only `$id`
    rename in `updateDocument()` is applied; and every internal metadata write runs Structure validation, as in 7.x.
  - Unstorable attribute types (`timestamp`, `serial`, `smallserial`, `bigserial` and the other types no adapter can
    store) are rejected when an attribute is created, including when `createAttribute()` adopts an existing column;
    increments and numeric operators accept integer, bigint, float and double attributes only.
  - A failed column change in `updateAttribute()` leaves the stored definition unchanged: relaxing `required` runs
    before the metadata write.
  - Reads on MongoDB, Memory and Redis return a stored document that has a non-string permission with that entry
    dropped, as the SQL adapters do, instead of throwing `Exception\Structure`.
  - The Redis adapter throws `Exception\Unique`, not a plain `Duplicate`, for unique index violations.
  - Custom types stay on the handles that share their `TypeRegistry` instead of replacing global filters.
- **SQL adapters:**
  - A transparent reconnect of `Utopia\Database\PDO` keeps MariaDB and MySQL statement timeouts; the timeout also
    applies to the statement retried after the reconnect.
  - `setMetadata()` values reach the database as query comments again.
  - On SQLite, pattern queries match `_`, `%` and `\` literally again, and index names use the filtered tenant again.
  - Batch `createAttributes()` works for spatial attributes on MariaDB, required spatial attributes on PostgreSQL
    are nullable columns again, and composite indexes keep the caller's column order.
  - On MySQL, a read with five or more joins no longer spends seconds choosing a join order: the joined collections'
    document permission checks stay subqueries instead of each joining the optimizer's search, which built about ten
    million partial plans for eight checked joins. Reads with up to four joins are planned as before.
- **Pools:**
  - `ReadWritePool` serves reads from the primary after a write or transaction commits, routes locking reads,
    `rawQuery()` and the reads that decide a write (the batch of `updateDocuments()` and `deleteDocuments()`, the
    lookup of `upsertDocuments()`) to the write pool, and no longer keeps reads on the primary after metadata or
    configuration calls.
  - Pooled connections no longer keep the profiler of the handle that last borrowed them.

### Known limitations

- `Database::sum()` reads a bare attribute name from the main collection; qualify a joined attribute with its join
  alias (`sum('orders', 'item.price', [$join])`).
- `groupBy()` over a main and a joined attribute of the same name (`groupBy(['name', 'note.name'])`) returns both
  groups under that one name, holding the joined value.
- A `distinct()` read with a cursor and no explicit order ignores the cursor, because it has no order to page along.
  Page a distinct read with an explicit order on a selected attribute.

### Dependencies

- Requires `utopia-php/query` 0.6 and `utopia-php/async` 0.1. `utopia-php/async` requires `opis/closure`, which the
  library itself does not use.

### Development

- The test suite fails on PHP warnings, notices and deprecations, on risky tests, and on PHPUnit's own notices and
  deprecations, raised in `src/` or `tests/`.
- `composer.json` declares `8.0.x-dev` as the branch alias of `dev-main`, so `"utopia-php/database": "8.*"` resolves
  before 8.0.0 is tagged (with `"minimum-stability": "dev"` and `"prefer-stable": true` in the root package).

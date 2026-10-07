<?php

namespace Utopia\Database;

use DateTime as NativeDateTime;
use DateTimeZone;
use Exception;
use Swoole\Coroutine;
use Throwable;
use Utopia\Cache\Cache;
use Utopia\Console;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Profile;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Cache\Scope;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Operator as OperatorException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Exception\Unconfirmed as UnconfirmedException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Named;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Hook\Selective;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Profiler\QueryProfiler;
use Utopia\Database\State\Snapshot;
use Utopia\Database\State\Value;
use Utopia\Database\Type\TypeRegistry;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\BigInt;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

/**
 * High-level database interface providing CRUD operations for documents, collections, attributes, indexes, and relationships with built-in caching, filtering, validation, and authorization.
 */
class Database
{
    use Traits\Attributes;
    use Traits\Collections;
    use Traits\Databases;
    use Traits\Documents;
    use Traits\Indexes;
    use Traits\Relationships;
    use Traits\Transactions;

    // Max limits
    public const MAX_INT = 2147483647;

    public const MAX_BIG_INT = PHP_INT_MAX;

    public const MAX_DOUBLE = PHP_FLOAT_MAX;

    public const MAX_VECTOR_DIMENSIONS = 16000;

    public const string VECTOR_DISTANCE = Document::DISTANCE;

    public const MAX_ARRAY_INDEX_LENGTH = 255;

    public const MAX_UID_DEFAULT_LENGTH = 36;

    // Maximum byte capacity for TEXT
    public const MAX_TEXT_BYTES = 65535;
    public const MAX_MEDIUMTEXT_BYTES = 16777215;
    public const MAX_LONGTEXT_BYTES = 4294967295;

    // Min limits
    public const MIN_INT = -2147483648;

    // Global SRID for geographic coordinates (WGS84)
    public const DEFAULT_SRID = 4326;

    public const EARTH_RADIUS = 6371000;

    public const RELATION_MAX_DEPTH = 3;

    public const RELATION_QUERY_CHUNK_SIZE = 5000;

    public const string METADATA = '_metadata';

    // Lengths
    public const LENGTH_KEY = 255;

    // Cache
    public const TTL = 60 * 60 * 24; // 24 hours

    private const CACHE_EMPTY_MARKER = '$empty';

    /**
     * Failures that fail the same way on every attempt, so withRetries() rethrows them at once: every typed failure
     * of this library except Transaction (and Contention), which another attempt can clear. Mismatch and Unique are
     * Duplicates. Unconfirmed is listed because another attempt could write twice.
     *
     * @var list<class-string<Throwable>>
     */
    private const array DETERMINISTIC_FAILURES = [
        AuthorizationException::class,
        CharacterException::class,
        ConflictException::class,
        DependencyException::class,
        DuplicateException::class,
        IndexException::class,
        LimitException::class,
        NotFoundException::class,
        OperatorException::class,
        OrderException::class,
        QueryException::class,
        RelationshipException::class,
        RestrictedException::class,
        StructureException::class,
        TimeoutException::class,
        TruncateException::class,
        TypeException::class,
        UnconfirmedException::class,
    ];

    public const INSERT_BATCH_SIZE = 1_000;

    public const DELETE_BATCH_SIZE = 1_000;

    /**
     * @var list<string>
     */
    public const array DEFAULT_FILTERS = [
        'json',
        'datetime',
        ColumnType::Point->value,
        ColumnType::Linestring->value,
        ColumnType::Polygon->value,
        ColumnType::Vector->value,
        ColumnType::Object->value,
    ];

    public const INTERNAL_ATTRIBUTE_KEYS = [
        Storage::UID,
        Storage::CREATED_AT,
        Storage::UPDATED_AT,
        Storage::PERMISSIONS,
    ];

    public const INTERNAL_INDEXES = [
        Storage::SEQUENCE,
        Storage::UID,
        Storage::CREATED_AT,
        Storage::UPDATED_AT,
        Storage::INDEX_PERMISSIONS_ID,
        Storage::PERMISSIONS,
    ];

    /**
     * The optional adapter features a profile records.
     */
    private const array FEATURES = [
        Feature\Casting::class,
        Feature\Connection::class,
        Feature\QueryBuilder::class,
        Feature\RawQuery::class,
        Feature\Relationships::class,
        Feature\Schemaless::class,
        Feature\Spatial::class,
        Feature\Timeouts::class,
        Feature\Upserts::class,
    ];

    private const string COLLECTION_NAME = Collection::NAME;

    private const string COLLECTION_ATTRIBUTES = Collection::ATTRIBUTES;

    private const string COLLECTION_INDEXES = Collection::INDEXES;

    private const string COLLECTION_DOCUMENT_SECURITY = Collection::DOCUMENT_SECURITY;

    private const string INDEX_ATTRIBUTES = 'attributes';

    /**
     * Keys of a collection definition that createCollection() sets itself rather than carrying over as metadata.
     */
    private const array COLLECTION_RESERVED_KEYS = [
        Document::ID => true,
        Document::SEQUENCE => true,
        Document::COLLECTION => true,
        Document::TENANT => true,
        Document::CREATED_AT => true,
        Document::UPDATED_AT => true,
        Document::PERMISSIONS => true,
        self::COLLECTION_NAME => true,
        self::COLLECTION_ATTRIBUTES => true,
        self::COLLECTION_INDEXES => true,
        self::COLLECTION_DOCUMENT_SECURITY => true,
    ];

    protected Adapter $adapter;

    protected Cache $cache;

    protected string $cacheName = 'default';

    /**
     * @var array<string, array{encode: callable, decode: callable, signature: string}>
     */
    protected static array $filters = [];

    protected static bool $defaultFiltersRegistered = false;

    /**
     * @var array<int, list<Attribute>>
     */
    private static array $internalAttributes = [];

    private static ?Collection $definition = null;

    /**
     * @var array<string, array{encode: callable, decode: callable, signature: string}>
     */
    protected array $instanceFilters = [];

    /**
     * @var array<Lifecycle>
     */
    protected array $lifecycleHooks = [];

    /**
     * @var array<Hook\Decorator>
     */
    protected array $decorators = [];

    /** @var Value<bool>|null Whether every lifecycle hook is silenced. */
    private ?Value $silenced = null;

    /** @var Value<array<string, true>>|null Names of the silenced lifecycle hooks. */
    private ?Value $silencedListeners = null;

    /** @var array<int, array<string, string>> Pending query-cache tombstones by coroutine id. */
    protected array $queryCacheMutations = [];

    /** @var array<int, array<string, string>> Pending document-cache tombstones by coroutine id. */
    protected array $documentCacheMutations = [];

    /** @var Value<NativeDateTime|null>|null */
    private ?Value $requestTimestamp = null;

    /** @var Value<bool>|null */
    private ?Value $filtering = null;

    /** @var Value<array<string, bool>|null>|null */
    private ?Value $filterExclusions = null;

    /** @var Value<bool>|null */
    private ?Value $validation = null;

    /** @var Value<bool>|null */
    private ?Value $datePreservation = null;

    /** @var Value<bool>|null */
    private ?Value $sequencePreservation = null;

    /** @var Value<bool>|null */
    private ?Value $duplicateSkipping = null;

    protected ?NativeDateTime $timestamp {
        get => $this->requestTimestamp()->get();
        set {
            $this->requestTimestamp()->set($value);
        }
    }

    protected ?Relationships $relationshipHook = null;

    protected bool $filter {
        get => $this->filtering()->get();
        set {
            $this->filtering()->set($value);
        }
    }

    /**
     * @var array<string, bool>|null
     */
    protected ?array $disabledFilters {
        get => $this->filterExclusions()->get();
        set {
            $this->filterExclusions()->set($value);
        }
    }

    protected bool $validate {
        get => $this->validation()->get();
        set {
            $this->validation()->set($value);
        }
    }

    protected bool $dropUnknownAttributes = false;

    protected bool $preserveDates {
        get => $this->datePreservation()->get();
        set {
            $this->datePreservation()->set($value);
        }
    }

    protected bool $preserveSequence {
        get => $this->sequencePreservation()->get();
        set {
            $this->sequencePreservation()->set($value);
        }
    }

    protected bool $skipDuplicates {
        get => $this->duplicateSkipping()->get();
        set {
            $this->duplicateSkipping()->set($value);
        }
    }

    protected int $maxQueryValues = 5000;

    protected bool $migrating = false;

    private ?Profile $profile = null;

    /**
     * List of collections that should be treated as globally accessible
     *
     * @var array<string, bool>
     */
    protected array $globalCollections = [];

    /**
     * Type mapping for collections to custom document classes
     *
     * @var array<string, class-string<Document>>
     */
    protected array $documentTypes = [];

    protected ?TypeRegistry $typeRegistry = null;

    protected ?QueryCache $queryCache = null;

    protected ?Invalidator $invalidator = null;

    protected ?QueryProfiler $profiler = null;

    private Authorization $authorization;

    /**
     * Construct a new Database instance with the given adapter, cache, and optional instance-level filters.
     *
     * @param Adapter $adapter The database adapter to use for storage operations.
     * @param Cache $cache The cache instance for document and collection caching.
     * @param array<string, array{encode: callable, decode: callable}> $filters Instance-level encode/decode filters.
     */
    public function __construct(
        Adapter $adapter,
        Cache $cache,
        array $filters = []
    ) {
        $this->adapter = $adapter;
        $this->cache = $cache;
        foreach ($filters as $name => $callbacks) {
            $filters[$name]['signature'] = self::computeCallableSignature($callbacks['encode'])
                . ':' . self::computeCallableSignature($callbacks['decode']);
        }
        $this->instanceFilters = $filters;

        $this->setAuthorization(new Authorization());
        $this->documentTypes[self::METADATA] = Collection::class;

        self::registerDefaultFilters();
    }

    protected static function registerDefaultFilters(): void
    {
        if (self::$defaultFiltersRegistered) {
            return;
        }
        self::$defaultFiltersRegistered = true;

        self::addFilter(
            'json',
            /**
             * @return mixed
             */
            static function (mixed $value) {
                $value = ($value instanceof Document) ? $value->getArrayCopy() : $value;

                if (! is_array($value) && ! $value instanceof \stdClass) {
                    return $value;
                }

                return json_encode($value);
            },
            /**
             * @return mixed
             *
             * @throws Exception
             */
            static function (mixed $value, mixed $document = null, mixed $database = null, string $attribute = '') {
                if (! is_string($value)) {
                    return $value;
                }

                $decoded = json_decode($value, true) ?? [];
                if (! is_array($decoded)) {
                    return $decoded;
                }

                /** @var array<string, mixed> $decoded */
                if (array_key_exists(Document::ID, $decoded)) {
                    return Document::fromStorage($decoded);
                }

                $decoded = array_map(static function ($item) {
                    if (! is_array($item) || ! array_key_exists(Document::ID, $item)) {
                        return $item;
                    }
                    /** @var array<string, mixed> $item */

                    return Document::fromStorage($item);
                }, $decoded);

                return $decoded;
            }
        );

        self::addFilter(
            'datetime',
            /**
             * @return mixed
             */
            static function (mixed $value) {
                if (is_null($value)) {
                    return;
                }
                if (! is_string($value)) {
                    return $value;
                }
                try {
                    $value = new NativeDateTime($value);
                    $value->setTimezone(new DateTimeZone(date_default_timezone_get()));

                    return DateTime::format($value);
                } catch (Throwable) {
                    return $value;
                }
            },
            /**
             * @return string|null
             */
            static function (?string $value) {
                return DateTime::formatTz($value);
            }
        );

        self::addFilter(
            ColumnType::Point->value,
            /**
             * An invalid geometry is returned as given, for the structure validator to reject.
             *
             * @return mixed
             */
            static function (mixed $value, Document $document, Database $database) {
                if (! is_array($value) || ! $database->adapter->hasFeature(Feature\Spatial::class)) {
                    return $value;
                }
                /** @var Adapter&Feature\Spatial $adapter */
                $adapter = $database->adapter;

                try {
                    return $adapter->encode($value, ColumnType::Point);
                } catch (StructureException) {
                    return $value;
                }
            },
            /**
             * @return array|null
             */
            static function (?string $value, Document $document, Database $database) {
                if ($value === null) {
                    return null;
                }
                if ($database->adapter->hasFeature(Feature\Spatial::class)) {
                    /** @var Adapter&Feature\Spatial $adapter */
                    $adapter = $database->adapter;

                    return $adapter->decode($value, ColumnType::Point);
                }

                return null;
            }
        );

        self::addFilter(
            ColumnType::Linestring->value,
            /**
             * An invalid geometry is returned as given, for the structure validator to reject.
             *
             * @return mixed
             */
            static function (mixed $value, Document $document, Database $database) {
                if (! is_array($value) || ! $database->adapter->hasFeature(Feature\Spatial::class)) {
                    return $value;
                }
                /** @var Adapter&Feature\Spatial $adapter */
                $adapter = $database->adapter;

                try {
                    return $adapter->encode($value, ColumnType::Linestring);
                } catch (StructureException) {
                    return $value;
                }
            },
            /**
             * @return array|null
             */
            static function (?string $value, Document $document, Database $database) {
                if (is_null($value)) {
                    return null;
                }
                if ($database->adapter->hasFeature(Feature\Spatial::class)) {
                    /** @var Adapter&Feature\Spatial $adapter */
                    $adapter = $database->adapter;

                    return $adapter->decode($value, ColumnType::Linestring);
                }

                return null;
            }
        );

        self::addFilter(
            ColumnType::Polygon->value,
            /**
             * An invalid geometry is returned as given, for the structure validator to reject.
             *
             * @return mixed
             */
            static function (mixed $value, Document $document, Database $database) {
                if (! is_array($value) || ! $database->adapter->hasFeature(Feature\Spatial::class)) {
                    return $value;
                }
                /** @var Adapter&Feature\Spatial $adapter */
                $adapter = $database->adapter;

                try {
                    return $adapter->encode($value, ColumnType::Polygon);
                } catch (StructureException) {
                    return $value;
                }
            },
            /**
             * @return array|null
             */
            static function (?string $value, Document $document, Database $database) {
                if (is_null($value)) {
                    return null;
                }
                if ($database->adapter->hasFeature(Feature\Spatial::class)) {
                    /** @var Adapter&Feature\Spatial $adapter */
                    $adapter = $database->adapter;

                    return $adapter->decode($value, ColumnType::Polygon);
                }

                return null;
            }
        );

        self::addFilter(
            ColumnType::Vector->value,
            /**
             * @return mixed
             */
            static function (mixed $value) {
                if (! \is_array($value)) {
                    return $value;
                }
                if (! \array_is_list($value)) {
                    return $value;
                }
                foreach ($value as $item) {
                    if (! \is_int($item) && ! \is_float($item)) {
                        return $value;
                    }
                }

                /** @var array<int|float> $value */
                return \json_encode(\array_map(fn (int|float $v): float => (float) $v, $value));
            },
            /**
             * @return array|null
             */
            static function (?string $value) {
                if (is_null($value)) {
                    return null;
                }
                $decoded = self::decodeObject($value);

                return is_array($decoded) || $decoded instanceof \stdClass ? $decoded : $value;
            }
        );

        self::addFilter(
            ColumnType::Object->value,
            /**
             * @return mixed
             */
            static function (mixed $value) {
                if (! \is_array($value) && ! $value instanceof \stdClass) {
                    return $value;
                }

                return \json_encode($value);
            },
            /**
             * @return array|null
             */
            static function (mixed $value) {
                if (is_null($value)) {
                    return;
                }
                // can be non string in case of mongodb as it stores the value as object
                if (! is_string($value)) {
                    return $value;
                }
                $decoded = self::decodeObject($value);

                return is_array($decoded) || $decoded instanceof \stdClass ? $decoded : $value;
            }
        );
    }

    private static function decodeObject(string $value): mixed
    {
        if (preg_match('/\{\s*\}/', $value) === 0) {
            return json_decode($value, true);
        }

        return self::toAssociative(json_decode($value));
    }

    private static function toAssociative(mixed $value): mixed
    {
        if ($value instanceof \stdClass) {
            $properties = (array)$value;

            return $properties === [] ? $value : array_map(self::toAssociative(...), $properties);
        }

        if (is_array($value)) {
            return array_map(self::toAssociative(...), $value);
        }

        return $value;
    }

    private static function valuesEqual(mixed $value, mixed $old): bool
    {
        if ($value instanceof \stdClass && $old instanceof \stdClass) {
            return self::valuesEqual((array)$value, (array)$old);
        }

        if (is_array($value) && is_array($old)) {
            if (array_keys($value) !== array_keys($old)) {
                return false;
            }

            foreach ($value as $key => $item) {
                if (!self::valuesEqual($item, $old[$key])) {
                    return false;
                }
            }

            return true;
        }

        return $value === $old;
    }

    /**
     * Set database to use for current scope
     *
     *
     * @throws DatabaseException
     */
    public function setDatabase(string $name): static
    {
        $this->adapter->setDatabase($name);

        return $this;
    }

    /**
     * Get Database.
     *
     * Get Database from current scope
     *
     * @throws DatabaseException
     */
    public function getDatabase(): string
    {
        return $this->adapter->getDatabase();
    }

    /**
     * Set Namespace.
     *
     * Set namespace to divide different scope of data sets
     *
     *
     * @return $this
     *
     * @throws DatabaseException
     */
    public function setNamespace(string $namespace): static
    {
        $this->adapter->setNamespace($namespace);

        return $this;
    }

    /**
     * Get Namespace.
     *
     * Get namespace of current set scope
     */
    public function getNamespace(): string
    {
        return $this->adapter->getNamespace();
    }

    /**
     * The type of the internal sequence: integer on SQL, uuid7 on MongoDB.
     */
    public function getIdAttributeType(): ColumnType
    {
        return $this->adapter->limits()->idType;
    }

    /**
     * The adapter's limits and capabilities and this database's mode, built once and rebuilt when
     * shared tables, migration or the schemaless mode change. DefinedAttributes is asked of the
     * adapter on every check, as the database asks it.
     */
    public function profile(): Profile
    {
        if ($this->profile !== null) {
            return $this->profile;
        }

        $adapter = $this->adapter;
        $capabilities = \array_filter(
            Capability::cases(),
            static fn (Capability $capability): bool => $capability !== Capability::DefinedAttributes && $adapter->supports($capability),
        );

        return $this->profile = new Profile(
            $adapter->limits(),
            \array_values($capabilities),
            \array_values(\array_filter(self::FEATURES, $adapter->hasFeature(...))),
            $adapter->getSharedTables(),
            $this->migrating,
            static fn (): bool => $adapter->supports(Capability::DefinedAttributes),
        );
    }

    /**
     * Turn the adapter's schemaless mode on or off. An adapter without a schemaless mode always
     * enforces its schema, so it only accepts false.
     *
     * @throws DatabaseException
     */
    public function setSchemaless(bool $schemaless): static
    {
        if ($this->adapterHasFeature(Feature\Schemaless::class)) {
            $this->adapter->setSchemaless($schemaless);
        } elseif ($schemaless) {
            throw new DatabaseException('Adapter does not support schemaless');
        }

        $this->resetProfile();

        return $this;
    }

    private function resetProfile(): void
    {
        $this->profile = null;
        $this->documentsValidatorCache = [];
    }

    /**
     * Get Database Adapter
     */
    public function getAdapter(): Adapter
    {
        return $this->adapter;
    }

    /**
     * Pool answers for the adapter it borrows without implementing the Feature interface, but declares every
     * Feature method, so a true answer makes those methods callable on the adapter either way.
     *
     * @template T of object
     *
     * @param  class-string<T>  $feature
     *
     * @phpstan-assert-if-true T $this->adapter
     */
    private function adapterHasFeature(string $feature): bool
    {
        return $this->adapter->hasFeature($feature);
    }

    /**
     * Get a utopia-php/query Builder over a collection's table, for statements the document API
     * cannot express. Its statements run as written: they check no permissions, read past and never
     * purge the document and query caches (purgeCachedDocument() what they change), keep no `_perms`
     * rows, validate nothing and run no hooks or events, so a Mirror does not replicate them. It is
     * therefore only handed out, and its statements only run, while authorization is disabled:
     * inside getAuthorization()->skip().
     *
     * Skipping authorization lifts permissions, never tenancy: under shared tables every statement
     * stays within the tenant selected when the builder was handed out (see SQL::builder() for
     * what that covers). Another tenant's rows are read by selecting that tenant, with setTenant()
     * or withTenant().
     *
     * @throws AuthorizationException While authorization is enabled
     * @throws DatabaseException When the adapter has no query builder
     */
    public function from(string $collection): \Utopia\Query\Builder
    {
        $this->requireSkippedAuthorization();

        if (! $this->adapterHasFeature(Feature\QueryBuilder::class)) {
            throw new DatabaseException('Query builder is not supported by this adapter');
        }

        $builder = $this->adapter->builder($collection);
        $builder->setExecutor(fn (\Utopia\Query\Builder\Statement $statement) => $this->execute($statement));

        return $builder;
    }

    /**
     * Get a utopia-php/query Schema builder for DDL operations.
     */
    public function schema(): \Utopia\Query\Schema
    {
        if (! $this->adapterHasFeature(Feature\QueryBuilder::class)) {
            throw new DatabaseException('Schema builder is not supported by this adapter');
        }

        $schema = $this->adapter->schema();
        $schema->setExecutor(fn (\Utopia\Query\Builder\Statement $statement) => $this->execute($statement));

        return $schema;
    }

    /**
     * Run a statement as written, with everything from() says it bypasses; a builder runs its SELECT.
     *
     * @return array<Document>|int The rows a read returns, or how many rows a write changed
     * @throws AuthorizationException While authorization is enabled
     * @throws DatabaseException When the adapter cannot run raw statements
     */
    public function execute(\Utopia\Query\Builder|\Utopia\Query\Builder\Statement $query): array|int
    {
        $this->requireSkippedAuthorization();

        if (! $this->adapterHasFeature(Feature\RawQuery::class)) {
            throw new DatabaseException('Raw queries are not supported by this adapter');
        }

        $result = $query instanceof \Utopia\Query\Builder\Statement ? $query : $query->build();

        if ($result->readOnly) {
            return $this->adapter->rawQuery($result->query, $result->bindings);
        }

        return $this->adapter->rawMutation($result->query, $result->bindings);
    }

    /**
     * @throws AuthorizationException
     */
    private function requireSkippedAuthorization(): void
    {
        if ($this->authorization->getStatus()) {
            throw new AuthorizationException('The query builder bypasses permissions, caches and events: build and run it inside getAuthorization()->skip()');
        }
    }

    public function setTypeRegistry(?TypeRegistry $typeRegistry): static
    {
        $this->typeRegistry = $typeRegistry;

        return $this;
    }

    public function getTypeRegistry(): ?TypeRegistry
    {
        return $this->typeRegistry;
    }

    public function setQueryCache(?QueryCache $queryCache): static
    {
        $this->lifecycleHooks = \array_values(\array_filter(
            $this->lifecycleHooks,
            static fn (Lifecycle $hook): bool => ! $hook instanceof Invalidator,
        ));
        $this->invalidator = null;
        $this->queryCache = $queryCache;

        if ($queryCache !== null) {
            $this->invalidator = new Invalidator($queryCache);
        }

        return $this;
    }

    public function getQueryCache(): ?QueryCache
    {
        return $this->queryCache;
    }

    public function enableProfiling(): static
    {
        if ($this->profiler === null) {
            $this->profiler = new QueryProfiler();
        }

        $this->profiler->enable();
        $this->adapter->setProfiler($this->profiler);

        return $this;
    }

    public function disableProfiling(): static
    {
        if ($this->profiler !== null) {
            $this->profiler->disable();
        }

        $this->adapter->setProfiler(null);

        return $this;
    }

    public function getProfiler(): ?QueryProfiler
    {
        return $this->profiler;
    }

    /**
     * Set the cache instance
     *
     *
     * @return $this
     */
    public function setCache(Cache $cache): static
    {
        $this->cache = $cache;

        return $this;
    }

    /**
     * Get the cache instance
     */
    public function getCache(): Cache
    {
        return $this->cache;
    }

    /**
     * Set the name to use for cache
     *
     * @return $this
     */
    public function setCacheName(string $name): static
    {
        $this->cacheName = $name;

        return $this;
    }

    /**
     * Get the cache name
     */
    public function getCacheName(): string
    {
        return $this->cacheName;
    }

    /**
     * Set shard tables
     *
     * Set whether to share tables between tenants
     */
    public function setSharedTables(bool $sharedTables): static
    {
        $this->adapter->setSharedTables($sharedTables);
        $this->resetProfile();

        return $this;
    }

    /**
     * Get shared tables
     *
     * Get whether to share tables between tenants
     */
    public function getSharedTables(): bool
    {
        return $this->adapter->getSharedTables();
    }

    /**
     * Set Tenant
     *
     * Set tenant to use if tables are shared
     */
    public function setTenant(int|string|null $tenant): static
    {
        $this->adapter->setTenant($tenant);

        return $this;
    }

    /**
     * Get Tenant
     *
     * Get tenant to use if tables are shared
     */
    public function getTenant(): int|string|null
    {
        return $this->adapter->getTenant();
    }

    /**
     * With Tenant
     *
     * Execute a callback with a specific tenant. Scoped to the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function withTenant(int|string|null $tenant, callable $callback): mixed
    {
        return $this->adapter->withTenant($tenant, $callback);
    }

    /**
     * Set whether to allow creating documents with tenant set per document.
     */
    public function setTenantPerDocument(bool $enabled): static
    {
        $this->adapter->setTenantPerDocument($enabled);

        return $this;
    }

    /**
     * Get whether to allow creating documents with tenant set per document.
     */
    public function getTenantPerDocument(): bool
    {
        return $this->adapter->getTenantPerDocument();
    }

    /**
     * Sets instance of authorization for permission checks
     */
    public function setAuthorization(Authorization $authorization): self
    {
        $this->adapter->setAuthorization($authorization);
        $this->authorization = $authorization;

        return $this;
    }

    /**
     * Get Authorization
     */
    public function getAuthorization(): Authorization
    {
        return $this->authorization;
    }

    /**
     * Set maximum query execution time
     *
     * @throws Exception
     */
    public function setTimeout(int $milliseconds, Event $event = Event::All): static
    {
        if (! $this->adapterHasFeature(Feature\Timeouts::class)) {
            throw new DatabaseException('Adapter does not support timeouts');
        }

        $this->adapter->setTimeout($milliseconds, $event);

        return $this;
    }

    /**
     * Clear maximum query execution time
     */
    public function clearTimeout(Event $event = Event::All): void
    {
        if (! $this->adapterHasFeature(Feature\Timeouts::class)) {
            throw new DatabaseException('Adapter does not support timeouts');
        }

        $this->adapter->clearTimeout($event);
    }

    /**
     * Get the current relationship hook.
     *
     * @return Relationships|null The relationship hook, or null if not set.
     */
    public function getRelationshipHook(): ?Relationships
    {
        return $this->relationshipHook;
    }

    /**
     * Set whether to preserve original date values instead of overwriting with current timestamps.
     *
     * @param bool $preserve True to preserve dates on write operations.
     * @return $this
     */
    public function setPreserveDates(bool $preserve): static
    {
        $this->datePreservation()->set($preserve);

        return $this;
    }

    public function getDropUnknownAttributes(): bool
    {
        return $this->dropUnknownAttributes;
    }

    /**
     * Drop attributes missing from the collection schema instead of rejecting the write.
     *
     * Enable this where the schema is owned by the application rather than the caller, so a
     * deploy that writes an attribute before its migration has run degrades to a warning
     * instead of failing every write.
     */
    public function setDropUnknownAttributes(bool $drop): static
    {
        $this->dropUnknownAttributes = $drop;

        return $this;
    }

    /**
     * Get whether date preservation is enabled.
     *
     * @return bool True if dates are being preserved.
     */
    public function getPreserveDates(): bool
    {
        return $this->datePreservation()->get();
    }

    /**
     * Execute a callback with date preservation enabled, restoring the previous state afterward.
     * Scoped to the calling coroutine and the coroutines it starts.
     *
     * @param callable $callback The callback to execute.
     * @return mixed The callback's return value.
     */
    public function withPreserveDates(callable $callback): mixed
    {
        return $this->datePreservation()->with(true, $callback);
    }

    /**
     * Execute a callback with skipDuplicates enabled, restoring the previous state afterward.
     * Scoped to the calling coroutine and the coroutines it starts.
     *
     * @template T
     * @param callable(): T $callback
     * @return T
     */
    public function skipDuplicates(callable $callback): mixed
    {
        return $this->duplicateSkipping()->with(true, $callback);
    }

    /**
     * Set whether to preserve original sequence values instead of auto-generating them.
     *
     * @param bool $preserve True to preserve sequence values on write operations.
     * @return $this
     */
    public function setPreserveSequence(bool $preserve): static
    {
        $this->sequencePreservation()->set($preserve);

        return $this;
    }

    /**
     * Get whether sequence preservation is enabled.
     *
     * @return bool True if sequence values are being preserved.
     */
    public function getPreserveSequence(): bool
    {
        return $this->sequencePreservation()->get();
    }

    /**
     * Execute a callback with sequence preservation enabled, restoring the previous state afterward.
     * Scoped to the calling coroutine and the coroutines it starts.
     *
     * @param callable $callback The callback to execute.
     * @return mixed The callback's return value.
     */
    public function withPreserveSequence(callable $callback): mixed
    {
        return $this->sequencePreservation()->with(true, $callback);
    }

    /**
     * Set the migration mode flag, which relaxes certain constraints during data migrations.
     *
     * @param bool $migrating True to enable migration mode.
     * @return $this
     */
    public function setMigrating(bool $migrating): self
    {
        $this->migrating = $migrating;
        $this->resetProfile();

        return $this;
    }

    /**
     * Check whether the database is currently in migration mode.
     *
     * @return bool True if migration mode is active.
     */
    public function isMigrating(): bool
    {
        return $this->migrating;
    }

    /**
     * Set the maximum number of values allowed in a single query (e.g., IN clauses).
     *
     * @param int $max The maximum number of query values.
     * @return $this
     */
    public function setMaxQueryValues(int $max): self
    {
        if ($this->maxQueryValues !== $max) {
            // Validator cache key encodes maxQueryValues; entries built under
            // the previous limit must be discarded so subsequent validation
            // honors the new ceiling.
            $this->documentsValidatorCache = [];
        }

        $this->maxQueryValues = $max;

        return $this;
    }

    /**
     * Get the maximum number of values allowed in a single query.
     *
     * @return int The current maximum query values limit.
     */
    public function getMaxQueryValues(): int
    {
        return $this->maxQueryValues;
    }

    /**
     * Set list of collections which are globally accessible
     *
     * @param  array<string>  $collections
     * @return $this
     */
    public function setGlobalCollections(array $collections): static
    {
        foreach ($collections as $collection) {
            $this->globalCollections[$collection] = true;
        }

        return $this;
    }

    /**
     * Get list of collections which are globally accessible
     *
     * @return array<string>
     */
    public function getGlobalCollections(): array
    {
        return \array_keys($this->globalCollections);
    }

    /**
     * Clear global collections
     */
    public function resetGlobalCollections(): void
    {
        $this->globalCollections = [];
    }

    /**
     * Set custom document class for a collection
     *
     * @param  string  $collection  Collection ID
     * @param  string  $className  Fully qualified class name that extends Document
     *
     * @throws DatabaseException
     */
    public function setDocumentType(string $collection, string $className): static
    {
        if (! \class_exists($className)) {
            throw new DatabaseException("Class {$className} does not exist");
        }

        if (! \is_subclass_of($className, Document::class)) {
            throw new DatabaseException("Class {$className} must extend ".Document::class);
        }

        $this->documentTypes[$collection] = $className;

        return $this;
    }

    /**
     * Get custom document class for a collection
     *
     * @param  string  $collection  Collection ID
     * @return class-string<Document>|null
     */
    public function getDocumentType(string $collection): ?string
    {
        return $this->documentTypes[$collection] ?? null;
    }

    /**
     * Clear document type mapping for a collection
     *
     * @param  string  $collection  Collection ID
     */
    public function clearDocumentType(string $collection): static
    {
        unset($this->documentTypes[$collection]);

        return $this;
    }

    /**
     * Clear all document type mappings
     */
    public function clearAllDocumentTypes(): static
    {
        $this->documentTypes = [self::METADATA => Collection::class];

        return $this;
    }

    /**
     * Whether ALTER TABLE statements take LOCK=SHARED, on adapters that support it.
     */
    public function setLocks(bool $locks): static
    {
        if ($this->adapter->supports(Capability::AlterLock)) {
            $this->adapter->setLocks($locks);
        }

        return $this;
    }

    /**
     * Enable validation
     *
     * @return $this
     */
    public function enableValidation(): static
    {
        $this->validation()->set(true);

        return $this;
    }

    /**
     * Disable validation
     *
     * @return $this
     */
    public function disableValidation(): static
    {
        $this->validation()->set(false);

        return $this;
    }

    /**
     * Whether document structure validation is currently enabled.
     */
    public function isValidationEnabled(): bool
    {
        return $this->validation()->get();
    }

    /**
     * Skip Validation
     *
     * Execute a callback without validation. Scoped to the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function skipValidation(callable $callback): mixed
    {
        return $this->validation()->with(false, $callback);
    }

    /**
     * Register a hook into the database pipeline.
     *
     * Dispatches by type:
     * - {@see Hook\Lifecycle} — side effects on database events (auditing, logging); a
     *   {@see Named} one replaces the lifecycle hook registered under its name
     * - {@see Hook\Decorator} — document transformation on read/write results
     * - {@see Hook\Relationships} — relationship resolution and mutation
     * - {@see Hook\Write} — row-level write interception (permissions, tenant)
     * - {@see Hook\Transform} — raw SQL transformation before execution
     *
     * @throws DatabaseException When the hook is none of these
     */
    public function addHook(\Utopia\Query\Hook $hook): static
    {
        if (
            ! $hook instanceof Lifecycle
            && ! $hook instanceof Hook\Decorator
            && ! $hook instanceof Relationships
            && ! $hook instanceof Hook\Write
            && ! $hook instanceof Transform
        ) {
            throw new DatabaseException('Unknown hook: '.$hook::class);
        }

        if ($hook instanceof Lifecycle) {
            if ($hook instanceof Invalidator) {
                $this->lifecycleHooks = \array_values(\array_filter(
                    $this->lifecycleHooks,
                    static fn (Lifecycle $registered): bool => ! $registered instanceof Invalidator,
                ));
                $this->invalidator = $hook;
            } else {
                $this->registerLifecycleHook($hook);
            }
        }

        if ($hook instanceof Hook\Decorator) {
            $this->decorators[] = $hook;
        }

        if ($hook instanceof Relationships) {
            $this->relationshipHook = $hook;
        }

        if ($hook instanceof Hook\Write) {
            $this->adapter->addWriteHook($hook);
        }

        if ($hook instanceof Transform) {
            $this->adapter->addTransform($hook::class, $hook);
        }

        return $this;
    }

    private function registerLifecycleHook(Lifecycle $hook): void
    {
        if ($hook instanceof Named) {
            foreach ($this->lifecycleHooks as $index => $registered) {
                if ($registered instanceof Named && $registered->getName() === $hook->getName()) {
                    $this->lifecycleHooks[$index] = $hook;

                    return;
                }
            }
        }

        $this->lifecycleHooks[] = $hook;
    }

    /**
     * Apply all registered decorators to a single document.
     */
    protected function decorateDocument(Event $event, Document $collection, Document $document): Document
    {
        if ($this->areEventsSilenced()) {
            return $document;
        }

        foreach ($this->decorators as $decorator) {
            $document = $decorator->decorate($event, $collection, $document);
        }

        return $document;
    }

    /**
     * Apply all registered document decorators to an array of documents.
     *
     * @param  array<Document>  $documents
     * @return array<Document>
     */
    protected function decorateDocuments(Event $event, Document $collection, array $documents): array
    {
        if (empty($this->decorators)) {
            return $documents;
        }

        foreach ($documents as $i => $document) {
            $documents[$i] = $this->decorateDocument($event, $collection, $document);
        }

        return $documents;
    }


    /**
     * Remove a query transform hook from the adapter.
     */
    public function removeTransform(string $name): static
    {
        $this->adapter->removeTransform($name);

        return $this;
    }

    /**
     * Silence lifecycle hooks for calls inside the callback: every hook, or only the
     * {@see Named} hooks listed. A nested silence never narrows the one around it, and
     * silences are scoped to the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @param  array<string>|null  $listeners  Names of the hooks to silence; null silences every hook
     * @return T
     */
    public function silent(callable $callback, ?array $listeners = null): mixed
    {
        if ($listeners !== null) {
            return $this->silenceListeners($callback, $listeners);
        }

        return $this->silenced()->with(true, $callback);
    }

    /**
     * @template T
     *
     * @param  callable(): T  $callback
     * @param  array<string>  $listeners
     * @return T
     */
    private function silenceListeners(callable $callback, array $listeners): mixed
    {
        $silencedListeners = $this->silencedListeners();

        return $silencedListeners->with($silencedListeners->get() + \array_fill_keys($listeners, true), $callback);
    }

    protected function areEventsSilenced(): bool
    {
        return $this->silenced()->get();
    }

    /**
     * @return Value<bool>
     */
    private function silenced(): Value
    {
        return $this->silenced ??= new Value(false);
    }

    /**
     * @return Value<array<string, true>>
     */
    private function silencedListeners(): Value
    {
        if ($this->silencedListeners === null) {
            /** @var Value<array<string, true>> $silencedListeners */
            $silencedListeners = new Value([]);
            $this->silencedListeners = $silencedListeners;
        }

        return $this->silencedListeners;
    }

    /**
     * Capture the authorization status and roles, relationship, silence, tenant and toggle state the calling
     * coroutine sees, so work started elsewhere can run under it with withSnapshot().
     */
    public function snapshot(): Snapshot
    {
        return new Snapshot(
            authorization: $this->authorization->getStatus(),
            roles: $this->authorization->getRoles(),
            relationships: $this->relationshipHook?->isEnabled() ?? true,
            existCheck: $this->relationshipHook?->shouldCheckExist() ?? true,
            population: $this->relationshipHook?->isInBatchPopulation() ?? false,
            silenced: $this->areEventsSilenced(),
            silencedListeners: $this->silencedListeners()->get(),
            tenant: $this->adapter->getTenant(),
            filters: $this->filtering()->get(),
            disabledFilters: $this->filterExclusions()->get(),
            validation: $this->validation()->get(),
            preserveDates: $this->datePreservation()->get(),
            preserveSequence: $this->sequencePreservation()->get(),
            skipDuplicates: $this->duplicateSkipping()->get(),
            requestTimestamp: $this->requestTimestamp()->get(),
        );
    }

    /**
     * Run the callback under a snapshot's state. The state is scoped to the calling coroutine and the coroutines it
     * starts, so what the callback changes never reaches the coroutine the snapshot was taken in.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function withSnapshot(Snapshot $snapshot, callable $callback): mixed
    {
        $hook = $this->relationshipHook;
        $scoped = fn () => $this->silenced()->with(
            $snapshot->silenced,
            fn () => $this->silencedListeners()->with(
                $snapshot->silencedListeners,
                fn () => $this->withToggles($snapshot, $callback),
            ),
        );

        $authorized = fn () => $this->authorization->withRoles(
            $snapshot->roles,
            $hook === null ? $scoped : fn () => $hook->withSnapshot($snapshot, $scoped),
        );

        return $this->authorization->withStatus($snapshot->authorization, $authorized);
    }

    /**
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    private function withToggles(Snapshot $snapshot, callable $callback): mixed
    {
        $timestamped = fn (): mixed => $this->requestTimestamp()->with($snapshot->requestTimestamp, $callback);
        $deduplicated = fn (): mixed => $this->duplicateSkipping()->with($snapshot->skipDuplicates, $timestamped);
        $sequenced = fn (): mixed => $this->sequencePreservation()->with($snapshot->preserveSequence, $deduplicated);
        $dated = fn (): mixed => $this->datePreservation()->with($snapshot->preserveDates, $sequenced);
        $validated = fn (): mixed => $this->validation()->with($snapshot->validation, $dated);
        $excluded = fn (): mixed => $this->filterExclusions()->with($snapshot->disabledFilters, $validated);
        $filtered = fn (): mixed => $this->filtering()->with($snapshot->filters, $excluded);

        return $this->adapter->withTenant($snapshot->tenant, $filtered);
    }

    /**
     * @return Value<NativeDateTime|null>
     */
    private function requestTimestamp(): Value
    {
        if ($this->requestTimestamp === null) {
            /** @var Value<NativeDateTime|null> $requestTimestamp */
            $requestTimestamp = new Value(null);
            $this->requestTimestamp = $requestTimestamp;
        }

        return $this->requestTimestamp;
    }

    /**
     * @return Value<bool>
     */
    private function filtering(): Value
    {
        return $this->filtering ??= new Value(true);
    }

    /**
     * @return Value<array<string, bool>|null>
     */
    private function filterExclusions(): Value
    {
        if ($this->filterExclusions === null) {
            /** @var Value<array<string, bool>|null> $filterExclusions */
            $filterExclusions = new Value([]);
            $this->filterExclusions = $filterExclusions;
        }

        return $this->filterExclusions;
    }

    /**
     * @return Value<bool>
     */
    private function validation(): Value
    {
        return $this->validation ??= new Value(true);
    }

    /**
     * @return Value<bool>
     */
    private function datePreservation(): Value
    {
        return $this->datePreservation ??= new Value(false);
    }

    /**
     * @return Value<bool>
     */
    private function sequencePreservation(): Value
    {
        return $this->sequencePreservation ??= new Value(false);
    }

    /**
     * @return Value<bool>
     */
    private function duplicateSkipping(): Value
    {
        return $this->duplicateSkipping ??= new Value(false);
    }

    protected function skippingDuplicates(): bool
    {
        return $this->duplicateSkipping()->get();
    }

    private function getEventContext(): int
    {
        if (! \extension_loaded('swoole')) {
            return -1;
        }

        $context = Coroutine::getCid();

        return \is_int($context) ? $context : -1;
    }

    /**
     * Register a global attribute filter with encode and decode callbacks for data transformation.
     *
     * @param string $name The unique filter name.
     * @param callable $encode Callback to transform the value before storage.
     * @param callable $decode Callback to transform the value after retrieval.
     */
    public static function addFilter(string $name, callable $encode, callable $decode): void
    {
        self::registerDefaultFilters();

        self::$filters[$name] = [
            'encode' => $encode,
            'decode' => $decode,
            'signature' => self::computeCallableSignature($encode) . ':' . self::computeCallableSignature($decode),
        ];
    }

    private static function computeCallableSignature(callable $callable): string
    {
        if (\is_string($callable)) {
            return $callable;
        }

        if (\is_array($callable)) {
            $class = \is_object($callable[0]) ? \get_class($callable[0]) : $callable[0];
            return $class . '::' . $callable[1];
        }

        $closure = \Closure::fromCallable($callable);
        $ref = new \ReflectionFunction($closure);
        return ($ref->getFileName() ?: 'unknown') . ':' . $ref->getStartLine();
    }

    /**
     * Enable filters
     *
     * @return $this
     */
    public function enableFilters(): static
    {
        $this->filtering()->set(true);

        return $this;
    }

    /**
     * Disable filters
     *
     * @return $this
     */
    public function disableFilters(): static
    {
        $this->filtering()->set(false);

        return $this;
    }

    /**
     * Skip filters
     *
     * Execute a callback without filters, or without the named ones.
     * Scoped to the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @param  array<string>|null  $filters
     * @return T
     */
    public function skipFilters(callable $callback, ?array $filters = null): mixed
    {
        if (empty($filters)) {
            return $this->filtering()->with(false, $callback);
        }

        return $this->filtering()->with(
            $this->filtering()->get(),
            fn (): mixed => $this->filterExclusions()->with(\array_fill_keys($filters, true), $callback),
        );
    }

    /**
     * Encode Document
     *
     * @param  bool  $applyDefaults  Whether to apply default values to null attributes
     *
     * @throws DatabaseException
     */
    public function encode(Document $collection, Document $document, bool $applyDefaults = true): Document
    {
        $known = [];

        foreach (Collection::fromDocument($collection)->attributesWith($this->internalAttributes()) as $attribute) {
            $key = $attribute->key;
            $array = $attribute->array;
            $default = $attribute->default;
            $filters = $attribute->filters;
            $known[$key] = true;
            $exists = $document->offsetExists($key);
            $value = $exists ? $document[$key] : null;

            if (($key === Document::CREATED_AT || $key === Document::UPDATED_AT) && \is_string($value) && empty($value)) {
                $document->setAttribute($key, null);

                continue;
            }

            if ($key === Document::PERMISSIONS) {
                continue;
            }

            // Continue on optional param with no default
            if (! $exists && $default === null) {
                continue;
            }

            // Skip encoding for Operator objects
            if ($value instanceof Operator) {
                continue;
            }

            // Assign default when no value is provided or the value is explicitly null
            if ($value === null && $default !== null) {
                // Skip applying defaults during updates to avoid resetting unspecified attributes
                if (! $applyDefaults) {
                    continue;
                }
                $value = ($array) ? $default : [$default];
            } else {
                $value = ($array) ? $value : [$value];
            }

            if ($value === null) {
                continue;
            }

            /** @var array<int|string, mixed> $value */
            if (! empty($filters)) {
                foreach ($value as $index => $node) {
                    if ($node !== null) {
                        foreach ($filters as $filter) {
                            $node = $this->encodeAttribute($filter, $node, $document);
                        }
                        $value[$index] = $node;
                    }
                }
            }

            if (! $array) {
                $value = $value[0];
            }
            $document->setAttribute($key, $value);
        }

        return $this->removeUnknownAttributes($collection, $document, $known);
    }

    /**
     * Remove attributes the collection schema does not declare.
     *
     * Used ahead of change detection on update and upsert so a dropped key is not
     * counted as a write. encode() also calls this once it has collected the known
     * set while iterating the schema.
     *
     * @param  array<string, true>|null  $known  Attribute ids already collected
     */
    protected function removeUnknownAttributes(Document $collection, Document $document, ?array $known = null): Document
    {
        if (! $this->dropUnknownAttributes || ! $this->adapter->supports(Capability::DefinedAttributes)) {
            return $document;
        }

        if ($known === null) {
            $known = [];
            foreach (Collection::fromDocument($collection)->attributes() as $attribute) {
                $known[$attribute->key] = true;
            }
        }

        $dropped = [];
        $documentKeys = [];
        foreach ($document as $key => $value) {
            $documentKeys[] = (string) $key;
        }

        foreach ($documentKeys as $key) {
            if (\str_starts_with($key, '$') || isset($known[$key])) {
                continue;
            }

            $dropped[] = $key;
            $document->removeAttribute($key);
        }

        if (! empty($dropped)) {
            Console::warning(
                'Dropped unknown attributes "'.\implode('", "', $dropped).'" from collection "'.$collection->getId().'"'
                .($this->adapter->getTenant() === null ? '' : ' on tenant '.$this->adapter->getTenant())
            );
        }

        return $document;
    }

    /**
     * Decode Document
     *
     * @param  array<string>  $selections
     *
     * @throws DatabaseException
     */
    public function decode(Document $collection, Document $document, array $selections = []): Document
    {
        $allAttributes = Collection::fromDocument($collection)->attributes();

        $attributes = [];
        $relationships = [];
        foreach ($allAttributes as $attribute) {
            if ($attribute->relationship !== null) {
                $relationships[] = $attribute;
            } else {
                $attributes[] = $attribute;
            }
        }

        $filteredValue = [];
        $relationshipKeys = [];

        if (! empty($relationships)) {
            $documentArray = (array) $document;
            foreach ($relationships as $relationship) {
                $key = $relationship->key;
                $relationshipKeys[$key] = true;
                $filteredKey = $this->adapter->filter($key);

                if (
                    \array_key_exists($key, $documentArray)
                    || \array_key_exists($filteredKey, $documentArray)
                ) {
                    $value = $document->getAttribute($key);
                    $value ??= $document->getAttribute($filteredKey);
                    $document->removeAttribute($filteredKey);
                    $document->setAttribute($key, $value);
                }
            }
        }

        $internalKeys = [];

        foreach ($this->internalAttributes() as $attribute) {
            $attributes[] = $attribute;
            $internalKeys[$attribute->key] = true;
        }

        $hasSelections = ! empty($selections);
        $selectAll = $hasSelections && \in_array('*', $selections, true);
        $selectionsMap = ($hasSelections && ! $selectAll)
            ? \array_fill_keys($selections, true)
            : null;

        $filtering = null;
        $disabledFilters = null;

        $hasRelationshipSelections = false;
        if ($selectionsMap !== null && $relationshipKeys !== []) {
            foreach ($selections as $selection) {
                $dot = \strpos($selection, '.');
                if ($dot !== false && isset($relationshipKeys[\substr($selection, 0, $dot)])) {
                    $hasRelationshipSelections = true;
                    break;
                }
            }
        }

        foreach ($attributes as $attribute) {
            $key = $attribute->key;
            if ($key === Document::PERMISSIONS) {
                continue;
            }

            $array = $attribute->array;
            $filters = $attribute->filters;
            $value = $document->getAttribute($key);

            // filter() strips the leading "$" off an internal key, leaving a name a user
            // attribute is allowed to have ("$collection" -> "collection"). An internal value
            // never reaches the document under that name, so the alias lookup below has
            // nothing of its own to find and can only steal the user's attribute.
            if (\is_null($value) && ! isset($internalKeys[$key])) {
                $filteredKey = $this->adapter->filter($key);
                $value = $document->getAttribute($filteredKey);

                if ($filteredKey !== $key && $document->offsetExists($filteredKey)) {
                    $document->removeAttribute($filteredKey);
                }
            }

            // Skip decoding for Operator objects (shouldn't happen, but safety check)
            if ($value instanceof Operator) {
                continue;
            }

            $value = ($array) ? $value : [$value];
            $value = (is_null($value)) ? [] : $value;

            /** @var array<int|string, mixed> $value */
            $selected = ! $hasSelections
                || $selectAll
                || ($selectionsMap !== null && isset($selectionsMap[$key]));

            $filterCount = \count($filters);

            if ($filterCount > 0 && ($selected || $hasRelationshipSelections)) {
                $filtering ??= $this->filtering()->get();
                $disabledFilters ??= $this->filterExclusions()->get() ?? [];

                if ($filtering) {
                    foreach ($value as $index => $node) {
                        for ($i = $filterCount - 1; $i >= 0; $i--) {
                            if (! isset($disabledFilters[$filters[$i]])) {
                                $node = $this->decodeAttribute($filters[$i], $node, $document, $key);
                            }
                        }
                        $value[$index] = $node;
                    }
                }
            }

            $resolved = $array ? $value : $value[0];
            $filteredValue[$key] = $resolved;

            if ($selected) {
                $document->setAttribute($key, $resolved);
            }
        }

        if ($hasRelationshipSelections && $selectionsMap !== null) {
            foreach ($allAttributes as $attribute) {
                $key = $attribute->key;

                if ($attribute->relationship !== null || $key === Document::PERMISSIONS) {
                    continue;
                }

                if (! isset($selectionsMap[$key]) && isset($filteredValue[$key])) {
                    $document->setAttribute($key, $filteredValue[$key]);
                }
            }
        }

        return $document;
    }

    /**
     * Decode the values a document carries under each join alias as a direct read of the joined
     * collection would: cast to their types, then passed through every decode filter they declare,
     * with a document built from the joined row. An alias whose `$id` is null matched no row, and
     * its values stay null.
     *
     * @param  array<string, Document>  $collections  The collection each join alias reads
     *
     * @throws DatabaseException
     */
    protected function decodeJoins(Document $document, array $collections): Document
    {
        foreach ($this->joinedRows($document, $collections) as $alias => $row) {
            $collection = $collections[$alias];
            $keys = \array_map(\strval(...), \array_keys($row));

            $joined = Document::fromRow([...$row, Document::COLLECTION => $collection->getId()]);
            $joined = $this->castAfterDocument($collection, $joined);
            $joined = $this->casting($collection, $joined);
            $joined = $this->decode($collection, $joined, $keys);

            foreach ($keys as $key) {
                $document->setAttribute($alias.'.'.$key, $joined->getAttribute($key));
            }
        }

        return $document;
    }

    /**
     * Encode the values a document carries under each join alias back to how the joined collection
     * stores them. Returns a copy: the document is usually a caller's cursor, which keeps its
     * decoded values.
     *
     * @param  array<string, Document>  $collections  The collection each join alias reads
     *
     * @throws DatabaseException
     */
    protected function encodeJoins(Document $document, array $collections): Document
    {
        $rows = $this->joinedRows($document, $collections);
        if ($rows === []) {
            return $document;
        }

        $encoded = clone $document;
        foreach ($rows as $alias => $row) {
            $collection = $collections[$alias];
            $keys = \array_map(\strval(...), \array_keys($row));

            $joined = Document::fromRow([...$row, Document::COLLECTION => $collection->getId()]);
            $joined = $this->encode($collection, $joined, applyDefaults: false);
            $joined = $this->castBefore($collection, $joined);

            foreach ($keys as $key) {
                $encoded->setAttribute($alias.'.'.$key, $joined->getAttribute($key));
            }
        }

        return $encoded;
    }

    /**
     * The row each join alias carries in a document, by attribute. `$permissions` is never encoded
     * or decoded, and an alias whose `$id` is null matched no row: both are left out.
     *
     * @param  array<string, Document>  $collections
     * @return array<string, array<string, mixed>>
     */
    private function joinedRows(Document $document, array $collections): array
    {
        if ($collections === []) {
            return [];
        }

        $rows = [];
        foreach ($document as $key => $value) {
            $key = (string) $key;
            $dot = \strpos($key, '.');
            if ($dot === false) {
                continue;
            }

            $alias = \substr($key, 0, $dot);
            $attribute = \substr($key, $dot + 1);
            if (! isset($collections[$alias]) || $attribute === Document::PERMISSIONS) {
                continue;
            }

            $rows[$alias][$attribute] = $value;
        }

        return \array_filter(
            $rows,
            static fn (array $row): bool => ! \array_key_exists(Document::ID, $row) || $row[Document::ID] !== null,
        );
    }

    /**
     * Cast document attribute values to their proper PHP types based on the collection schema.
     *
     * @param Document $collection The collection definition containing attribute type information.
     * @param Document $document The document whose attributes will be cast.
     * @return Document The document with correctly typed attribute values.
     */
    public function casting(Document $collection, Document $document): Document
    {
        if ($this->adapter->hasFeature(Feature\Casting::class)) {
            return $document;
        }

        foreach (Collection::fromDocument($collection)->attributesWith($this->internalAttributes()) as $attribute) {
            $key = $attribute->key;
            $type = $attribute->type;
            $array = $attribute->array;

            $needsCast = $array || match ($type) {
                ColumnType::Id,
                ColumnType::Boolean,
                ColumnType::Integer,
                ColumnType::BigInteger,
                ColumnType::Float,
                ColumnType::Double => true,
                default => false,
            };
            if (! $needsCast || $key === Document::PERMISSIONS) {
                continue;
            }

            $value = $document->getAttribute($key);
            if (\is_null($value)) {
                continue;
            }

            if ($array) {
                $value = ! \is_string($value)
                    ? $value
                    : \json_decode($value, true);
            } else {
                $value = [$value];
            }

            /** @var array<int|string, scalar|null> $value */
            foreach ($value as $index => $node) {
                $value[$index] = match ($type) {
                    ColumnType::Id => (string) $node,
                    ColumnType::Boolean => (bool) $node,
                    ColumnType::Integer => (int) $node,
                    ColumnType::BigInteger => $this->castBigInteger($node, $attribute->signed),
                    ColumnType::Float,
                    ColumnType::Double => (float) $node,
                    default => $node,
                };
            }

            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }

        return $document;
    }

    private function castBigInteger(mixed $value, bool $signed): mixed
    {
        if (\is_string($value) && BigInt::fitsPhpInt($value, $signed)) {
            return (int) $value;
        }

        return $value;
    }

    /**
     * Set a metadata value to be printed in the query comments
     */
    public function setMetadata(string $key, mixed $value): static
    {
        $this->adapter->setMetadata($key, $value);

        return $this;
    }

    /**
     * Get metadata
     *
     * @return array<string, mixed>
     */
    public function getMetadata(): array
    {
        return $this->adapter->getMetadata();
    }

    /**
     * Clear metadata
     */
    public function resetMetadata(): void
    {
        $this->adapter->resetMetadata();
    }

    /**
     * Executes $callback with $timestamp set to $requestTimestamp.
     * Scoped to the calling coroutine and the coroutines it starts.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function withRequestTimestamp(?NativeDateTime $requestTimestamp, callable $callback): mixed
    {
        return $this->requestTimestamp()->with($requestTimestamp, $callback);
    }

    /**
     * The id of the adapter's connection, or null for an adapter without one.
     */
    public function getConnectionId(): ?string
    {
        if (! $this->adapterHasFeature(Feature\Connection::class)) {
            return null;
        }

        return $this->adapter->id();
    }

    /**
     * The host the adapter is connected to, or null for an adapter without a connection.
     */
    public function getHostname(): ?string
    {
        if (! $this->adapterHasFeature(Feature\Connection::class)) {
            return null;
        }

        return $this->adapter->hostname();
    }

    /**
     * Whether the adapter's connection answers. An adapter without a connection is always reachable.
     */
    public function ping(): bool
    {
        if (! $this->adapterHasFeature(Feature\Connection::class)) {
            return true;
        }

        return $this->adapter->ping();
    }

    /**
     * Re-establish the adapter's connection; nothing to do for an adapter without one.
     */
    public function reconnect(): void
    {
        if ($this->adapterHasFeature(Feature\Connection::class)) {
            $this->adapter->reconnect();
        }
    }

    /**
     * The attributes a collection may declare besides the internal ones; 0 when there is no limit.
     */
    public function getLimitForAttributes(): int
    {
        $limits = $this->adapter->limits();

        return $limits->attributes === 0 ? 0 : $limits->attributes - $limits->defaultAttributes;
    }

    /**
     * The indexes a collection may declare besides the internal ones.
     */
    public function getLimitForIndexes(): int
    {
        $limits = $this->adapter->limits();

        return $limits->indexes - $limits->defaultIndexes;
    }

    public function getMaxIndexLength(): int
    {
        return $this->adapter->limits()->indexLength;
    }

    public function getMaxVarcharLength(): int
    {
        return $this->adapter->limits()->varchar;
    }

    public function getMaxUidLength(): int
    {
        return $this->adapter->limits()->uidLength;
    }

    public function getMinDateTime(): NativeDateTime
    {
        return $this->adapter->limits()->minDateTime;
    }

    public function getMaxDateTime(): NativeDateTime
    {
        return $this->adapter->limits()->maxDateTime;
    }

    /**
     * Convert each filter to what its attribute stores. With the collections a query set's joins
     * read, a filter on `alias.attribute` is converted by that collection's attribute, and the
     * filters of each join's ON list and the conditions of each having() are converted too. A
     * having condition on the alias of a min or max is converted by the aggregated attribute, on
     * any other aggregate alias it is left as it is. Aggregates and selects are left as they are.
     *
     * @param  array<Query>  $queries
     * @param  array<string, Document>  $joinedCollections  The collection each join alias reads
     * @return array<Query>
     *
     * @throws QueryException
     * @throws \Utopia\Database\Exception
     */
    public function convertQueries(Document $collection, array $queries, array $joinedCollections = []): array
    {
        $attributesById = $this->buildAttributeMap($collection, $joinedCollections);
        $isNestedQueryAttributeSupported = $this->adapter->supports(Capability::Objects)
            && $this->adapter->supports(Capability::DefinedAttributes);

        $havingAttributesById = null;
        foreach ($queries as $index => $query) {
            $method = $query->getMethod();

            if ($method->isAggregate() || $method === Method::Select) {
                continue;
            }

            if ($method->isJoin()) {
                /** @var array<Query> $onQueries */
                $onQueries = $query->getJoinOnQueries();
                $this->convertQueriesWithMap($onQueries, $attributesById, $isNestedQueryAttributeSupported);

                continue;
            }

            if ($method === Method::Having) {
                $havingAttributesById ??= $this->withAggregateAliases($attributesById, $queries);
                /** @var array<Query> $conditions */
                $conditions = $query->getValues();
                $query->setValues($this->convertQueriesWithMap($conditions, $havingAttributesById, $isNestedQueryAttributeSupported));

                continue;
            }

            $queries[$index] = $this->convertQueriesWithMap([$query], $attributesById, $isNestedQueryAttributeSupported)[0];
        }

        return $queries;
    }

    /**
     * The collection's attributes and the internal ones by key, and each joined collection's under
     * `alias.key`, built once per conversion rather than per query.
     *
     * @param  array<string, Document>  $joinedCollections
     * @return array<string, Attribute>
     */
    private function buildAttributeMap(Document $collection, array $joinedCollections = []): array
    {
        $internal = $this->internalAttributes();

        $attributesById = [];
        foreach ([...Collection::fromDocument($collection)->attributes(), ...$internal] as $attribute) {
            $attributesById[$attribute->key] = $attribute;
        }

        foreach ($joinedCollections as $alias => $joined) {
            foreach ([...Collection::fromDocument($joined)->attributes(), ...$internal] as $attribute) {
                $attributesById[$alias.'.'.$attribute->key] ??= $attribute;
            }
        }

        return $attributesById;
    }

    /**
     * The attribute map a having condition is converted by: an aggregate alias names the result of
     * its aggregate, which for min and max has the type of the aggregated attribute.
     *
     * @param  array<string, Attribute>  $attributesById
     * @param  array<Query>  $queries
     * @return array<string, Attribute>
     */
    private function withAggregateAliases(array $attributesById, array $queries): array
    {
        foreach ($queries as $query) {
            $method = $query->getMethod();
            $alias = $query->getValue('');
            if (! $method->isAggregate() || ! \is_string($alias) || $alias === '') {
                continue;
            }

            $aggregated = \in_array($method, [Method::Min, Method::Max], true)
                ? $attributesById[$query->getAttribute()] ?? null
                : null;

            if ($aggregated === null) {
                unset($attributesById[$alias]);
            } else {
                $attributesById[$alias] = $aggregated;
            }
        }

        return $attributesById;
    }

    /**
     * @param array<Query> $queries
     * @param array<string, Attribute> $attributesById
     * @return array<Query>
     * @throws QueryException
     * @throws \Utopia\Database\Exception
     */
    private function convertQueriesWithMap(array $queries, array $attributesById, bool $isNestedQueryAttributeSupported): array
    {
        foreach ($queries as $index => $query) {
            if ($query->isNested()) {
                /** @var array<Query> $nestedQueries */
                $nestedQueries = $query->getValues();
                $values = $this->convertQueriesWithMap($nestedQueries, $attributesById, $isNestedQueryAttributeSupported);
                $query->setValues($values);
            }

            $query = $this->convertQueryWithMap($query, $attributesById, $isNestedQueryAttributeSupported);

            $queries[$index] = $query;
        }

        return $queries;
    }

    /**
     * @param  Document  $collection
     * @param  Query  $query
     * @return Query
     *
     * @throws QueryException
     * @throws \Utopia\Database\Exception
     */
    public function convertQuery(Document $collection, Query $query): Query
    {
        $attributesById = $this->buildAttributeMap($collection);
        $isNestedQueryAttributeSupported = $this->adapter->supports(Capability::Objects)
            && $this->adapter->supports(Capability::DefinedAttributes);

        return $this->convertQueryWithMap($query, $attributesById, $isNestedQueryAttributeSupported);
    }

    /**
     * @param array<string, Attribute> $attributesById
     * @return Query
     * @throws QueryException
     * @throws \Utopia\Database\Exception
     */
    private function convertQueryWithMap(Query $query, array $attributesById, bool $isNestedQueryAttributeSupported): Query
    {
        $queryAttribute = $query->getAttribute();
        $isNestedQueryAttribute = $isNestedQueryAttributeSupported && \str_contains($queryAttribute, '.');

        $attribute = $attributesById[$queryAttribute] ?? null;

        if ($attribute === null && $isNestedQueryAttribute) {
            $baseAttribute = \explode('.', $queryAttribute, 2)[0];
            $base = $attributesById[$baseAttribute] ?? null;
            if ($base !== null && $base->type === ColumnType::Object) {
                $query->setAttributeType(ColumnType::Object->value);
            }
        }

        if ($attribute !== null) {
            $query->setOnArray($attribute->array);
            $query->setAttributeType($attribute->type->value);

            if ($attribute->type === ColumnType::Datetime) {
                $values = $query->getValues();
                foreach ($values as $valueIndex => $value) {
                    try {
                        /** @var string $value */
                        $values[$valueIndex] = $this->adapterHasFeature(Feature\Casting::class)
                            ? $this->adapter->castDatetime($value)
                            : DateTime::setTimezone($value);
                    } catch (Throwable $e) {
                        throw new QueryException($e->getMessage(), $e->getCode(), $e);
                    }
                }
                $query->setValues($values);
            }
        } elseif (! $this->adapter->supports(Capability::DefinedAttributes)) {
            $values = $query->getValues();
            // setting attribute type to properly apply filters in the adapter level
            if ($this->adapter->supports(Capability::Objects) && $this->isCompatibleObjectValue($values)) {
                $query->setAttributeType(ColumnType::Object->value);
            }
        }

        return $query;
    }

    /**
     * The definition of the metadata collection, which stores every other collection's definition.
     */
    public static function collectionDefinition(): Collection
    {
        return clone (self::$definition ??= Collection::create(
            id: self::METADATA,
            name: 'collections',
            attributes: [
                Attribute::string(self::COLLECTION_NAME, 256, required: true),
                Attribute::string(self::COLLECTION_ATTRIBUTES, 1_000_000, filters: [Filter::Json]),
                Attribute::string(self::COLLECTION_INDEXES, 1_000_000, filters: [Filter::Json]),
                Attribute::boolean(self::COLLECTION_DOCUMENT_SECURITY, required: true),
            ],
            documentSecurity: false,
            metadata: [Document::COLLECTION => self::METADATA],
        ));
    }

    /**
     * The attributes every document carries; `$tenant` only under shared tables.
     *
     * @return list<Attribute>
     */
    public function internalAttributes(): array
    {
        return self::internalAttributesFor($this->adapter->getSharedTables());
    }

    /**
     * @internal for library code without a Database instance; use internalAttributes()
     *
     * @return list<Attribute>
     */
    public static function internalAttributesFor(bool $sharedTables): array
    {
        return self::$internalAttributes[(int) $sharedTables] ??= [
            Attribute::string(Document::ID, required: true),
            Attribute::id(Document::SEQUENCE, required: true),
            Attribute::string(Document::COLLECTION, required: true),
            ...($sharedTables ? [Attribute::id(Document::TENANT)] : []),
            Attribute::datetime(Document::CREATED_AT),
            Attribute::datetime(Document::UPDATED_AT),
            Attribute::string(Document::PERMISSIONS, 1_000_000, default: [], filters: [Filter::Json]),
        ];
    }

    /**
     * The columns the engine holds for a collection, read back from its catalog; empty where the adapter does not
     * support Capability::SchemaIntrospection.
     *
     * @return list<Schema\Column>
     *
     * @throws DatabaseException
     */
    public function getSchemaAttributes(string $collection): array
    {
        if (! $this->adapter->supports(Capability::SchemaIntrospection)) {
            return [];
        }

        return $this->adapter->getSchemaAttributes($collection);
    }

    /**
     * The indexes the engine holds for a collection, read back from its catalog; empty where the adapter does not
     * support Capability::SchemaIntrospection.
     *
     * @return list<Schema\Index>
     *
     * @throws DatabaseException
     */
    public function getSchemaIndexes(string $collection): array
    {
        if (! $this->adapter->supports(Capability::SchemaIntrospection)) {
            return [];
        }

        return $this->adapter->getSchemaIndexes($collection);
    }

    /**
     * @return array{0: string, 1: string}
     */
    public function getCacheBaseKeys(string $collectionId, ?string $documentId = null): array
    {
        $hostname = $this->getHostname();

        $tenantSegment = $this->adapter->getTenant();

        if (
            $collectionId === self::METADATA &&
            $this->adapter->getSharedTables() &&
            $documentId !== null &&
            isset($this->globalCollections[$documentId])
        ) {
            $tenantSegment = null;
        }

        $collectionKey = \sprintf(
            '%s-cache-%s:%s:%s:%s:collection:%s',
            $this->cacheName,
            $hostname ?? '',
            $this->adapter->getDatabase(),
            $this->getNamespace(),
            $tenantSegment,
            $collectionId
        );

        return [$collectionKey, $documentId ? "{$collectionKey}:{$documentId}" : ''];
    }

    /**
     * @param  array<string>  $selects
     * @return array{0: string, 1: string, 2: string}
     */
    public function getCacheKeys(string $collectionId, ?string $documentId = null, array $selects = []): array
    {
        [$collectionKey, $documentKey] = $this->getCacheBaseKeys($collectionId, $documentId);

        if ($documentId) {
            $sortedSelects = $selects;
            \sort($sortedSelects);

            $payload = \json_encode([
                'selects' => $sortedSelects,
                'relationships' => $collectionId !== self::METADATA && ($this->relationshipHook?->isEnabled() ?? false),
                'filters' => $this->getActiveFilterSignatures(),
            ]) ?: '';
            $documentHashKey = $documentKey . ':' . \md5($payload);
        }

        return [
            $collectionKey,
            $documentKey,
            $documentHashKey ?? '',
        ];
    }

    /**
     * Key of a collection's caller-owned withCache() region. find() caches its results in
     * the query cache instead; purgeCachedQueries() clears both.
     */
    public function getQueryCacheKey(string $collectionId, ?string $namespace = null): string
    {
        return \sprintf(
            '%s-cache-%s:%s:%s:%s:collection:%s:query',
            $this->cacheName,
            $this->getHostname() ?? '',
            $this->adapter->getDatabase(),
            $namespace ?? $this->getNamespace(),
            $this->adapter->getTenant(),
            $collectionId,
        );
    }

    protected function getQueryCacheScope(?string $namespace = null): Scope
    {
        return new Scope(
            hostname: $this->getHostname() ?? '',
            database: $this->adapter->getDatabase(),
            namespace: $namespace ?? $this->adapter->getNamespace(),
            tenant: $this->adapter->getTenant(),
        );
    }

    /**
     * Stable cache field for cached query entries on a collection.
     *
     * @param  array<Query>  $queries
     */
    public function getQueryCacheField(
        ?Document $collection = null,
        array $queries = [],
        string $field = 'documents',
        PermissionType $forPermission = PermissionType::Read,
    ): ?string {
        $this->checkQueryTypes($queries);

        if ($forPermission !== PermissionType::Read || $this->adapter->inTransaction()) {
            return null;
        }

        foreach ($queries as $query) {
            if ($query->getMethod() === Method::OrderRandom) {
                return null;
            }
        }

        $authorizationRoles = \array_values(\array_unique($this->authorization->getRoles()));
        \sort($authorizationRoles);

        $queryPayload = [
            'version' => 1,
            'authorization' => [
                'enabled' => $this->authorization->getStatus(),
                'roles' => $authorizationRoles,
            ],
            'database' => $this->getDatabase(),
            'queries' => \array_map(
                fn (Query $query): array => $this->serializeQueryCacheQuery($query),
                $queries,
            ),
            'relationships' => $this->relationshipHook?->isEnabled() ?? false,
            'filters' => $this->getActiveFilterSignatures(),
        ];

        return \sprintf(
            '%s:%s:%s',
            $collection === null ? '' : Collection::fromDocument($collection)->fingerprint(),
            \md5(\json_encode($queryPayload) ?: ''),
            $field,
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function serializeQueryCacheQuery(Query $query): array
    {
        $serialized = [
            'method' => $query->getMethod()->value,
        ];

        if ($query->getAttribute() !== '') {
            $serialized['attribute'] = $query->getAttribute();
        }

        $values = [];
        foreach ($query->getValues() as $value) {
            if ($value instanceof Query) {
                $values[] = $this->serializeQueryCacheQuery($value);
                continue;
            }

            $values[] = $this->normalizeQueryCacheQueryValue($value);
        }

        $serialized['values'] = $values;

        return $serialized;
    }

    private function normalizeQueryCacheQueryValue(mixed $value): mixed
    {
        if ($value instanceof Document) {
            $value = $value->getArrayCopy();
        }

        if (! \is_array($value)) {
            return $value;
        }

        foreach ($value as $key => $item) {
            $value[$key] = $this->normalizeQueryCacheQueryValue($item);
        }

        return $value;
    }

    protected function getCollectionMetadataCacheKey(string $collection): string
    {
        $tenant = $this->adapter->getTenant();
        $tenantKey = match (true) {
            $tenant === null => 'null',
            \is_int($tenant) => 'integer:'.$tenant,
            default => 'string:'.\strlen($tenant).':'.$tenant,
        };

        return $this->adapter->getDatabase().'::'.$this->adapter->getNamespace()
            .'::'.$tenantKey.'::'.$collection;
    }

    /**
     * @return array<string, string>
     */
    private function getActiveFilterSignatures(): array
    {
        if (! $this->filtering()->get()) {
            return [];
        }

        $signatures = [];

        foreach (self::$filters as $name => $callbacks) {
            $signatures[$name] = $callbacks['signature'];
        }

        foreach ($this->typeRegistry?->all() ?? [] as $name => $type) {
            $signatures[$name] = $type::class;
        }

        foreach ($this->instanceFilters as $name => $callbacks) {
            $signatures[$name] = $callbacks['signature'];
        }

        $signatures = \array_diff_key($signatures, $this->filterExclusions()->get() ?? []);
        \ksort($signatures);

        return $signatures;
    }

    /**
     * Fire an event to mandatory cache invalidation and registered lifecycle hooks.
     * Mandatory invalidation is never silenced and failures are propagated.
     */
    protected function trigger(Event $event, mixed $data = null): void
    {
        $this->invalidate($event, $data);
        $this->triggerHooks($event, $data);
    }

    /**
     * Run mandatory cache invalidation for a lifecycle event.
     */
    protected function invalidate(Event $event, mixed $data = null): void
    {
        $invalidator = $this->invalidator;
        if ($invalidator === null || ! $invalidator->isMutation($event)) {
            return;
        }

        $tokens = $this->getInvalidationTokens($event, $data);
        $invalidator->block($tokens);
        $invalidator->activate($tokens);
    }

    /**
     * @return array<string, string>
     */
    protected function getInvalidationTokens(Event $event, mixed $data = null): array
    {
        return $this->invalidator?->tokens(
            $event,
            $data,
            $this->getQueryCacheScope(),
            $this->adapter->getSharedTables() && $this->adapter->getTenantPerDocument(),
        ) ?? [];
    }

    /**
     * @param  array<string, string>  $tokens
     */
    protected function blockInvalidation(array $tokens): void
    {
        $this->invalidator?->block($tokens);
    }

    /**
     * @param  array<string, string>  $tokens
     */
    protected function activateInvalidation(array $tokens): void
    {
        $this->invalidator?->activate($tokens);
    }

    /**
     * Fire suppressible user lifecycle hooks after mandatory invalidation succeeds.
     *
     * Whether a hook's exception reaches the caller depends on the event
     * ({@see propagatesHookFailures()}); an \Error always does.
     */
    protected function triggerHooks(Event $event, mixed $data = null): void
    {
        $propagates = $this->propagatesHookFailures($event);

        foreach ($this->getActiveLifecycleHooks($event) as $hook) {
            try {
                $hook->handle($event, $data);
            } catch (Exception $exception) {
                if ($propagates) {
                    throw $exception;
                }
            }
        }
    }

    /**
     * Fire suppressible user lifecycle hooks and let the first hook exception reach the
     * caller whatever the event's default. Document writes and purgeCachedDocument() fire
     * Event::DocumentPurge through it; the schema changes that purge a collection fire it
     * through triggerHooks(), isolated.
     */
    protected function triggerPropagatingHooks(Event $event, mixed $data = null): void
    {
        foreach ($this->getActiveLifecycleHooks($event) as $hook) {
            $hook->handle($event, $data);
        }
    }

    /**
     * Keeps 7.x behaviour: the events it dispatched unguarded let a listener failure fail
     * the call; the ones it wrapped in try/catch isolate every hook from the others.
     */
    private function propagatesHookFailures(Event $event): bool
    {
        return match ($event) {
            Event::IndexCreate,
            Event::DocumentRead,
            Event::DocumentCreate,
            Event::DocumentsCreate,
            Event::DocumentUpdate,
            Event::DocumentsUpdate,
            Event::DocumentsUpsert,
            Event::DocumentIncrease,
            Event::DocumentDecrease,
            Event::DocumentDelete,
            Event::DocumentsDelete,
            Event::DocumentFind,
            Event::DocumentCount,
            Event::DocumentSum => true,
            Event::All,
            Event::DatabaseList,
            Event::DatabaseCreate,
            Event::DatabaseDelete,
            Event::CollectionList,
            Event::CollectionCreate,
            Event::CollectionUpdate,
            Event::CollectionRead,
            Event::CollectionDelete,
            Event::DocumentPurge,
            Event::PermissionsCreate,
            Event::PermissionsRead,
            Event::PermissionsDelete,
            Event::AttributeCreate,
            Event::AttributesCreate,
            Event::AttributeUpdate,
            Event::AttributeDelete,
            Event::IndexRename,
            Event::IndexDelete => false,
        };
    }

    /**
     * @return array<Lifecycle>
     */
    private function getActiveLifecycleHooks(Event $event): array
    {
        if ($this->lifecycleHooks === [] || $this->areEventsSilenced()) {
            return [];
        }

        $silenced = $this->silencedListeners()->get();
        $active = [];
        foreach ($this->lifecycleHooks as $hook) {
            if ($hook instanceof Named && isset($silenced[$hook->getName()])) {
                continue;
            }
            if ($hook instanceof Selective && ! $hook->handles($event)) {
                continue;
            }
            $active[] = $hook;
        }

        return $active;
    }

    /**
     * Create a document instance of the appropriate type from data read back from storage or the
     * cache. Non-string permissions are dropped, as Document::fromStorage() does, instead of failing
     * the read; a mapped type is kept.
     *
     * @param  string  $collection  Collection ID
     * @param  array<string, mixed>  $data  Document data
     */
    protected function createDocumentInstance(string $collection, array $data): Document
    {
        $className = $this->documentTypes[$collection] ?? null;
        if ($className === null) {
            return Document::fromStorage($data);
        }

        try {
            return $className::fromArray($data);
        } catch (StructureException) {
            return $className::fromArray(self::withStringPermissions($data));
        }
    }

    /**
     * The data with the non-string permissions of the document, and of the documents nested in it
     * the way the Document constructor nests them, dropped.
     *
     * @param  array<string, mixed>  $data
     * @return array<string, mixed>
     */
    private static function withStringPermissions(array $data): array
    {
        $permissions = $data[Document::PERMISSIONS] ?? null;
        if (\is_array($permissions)) {
            $data[Document::PERMISSIONS] = \array_values(\array_filter($permissions, \is_string(...)));
        }

        foreach ($data as $key => $value) {
            if (! \is_array($value)) {
                continue;
            }

            if (isset($value[Document::ID]) || isset($value[Document::COLLECTION])) {
                /** @var array<string, mixed> $value */
                $data[$key] = self::withStringPermissions($value);

                continue;
            }

            foreach ($value as $childKey => $child) {
                if (\is_array($child) && (isset($child[Document::ID]) || isset($child[Document::COLLECTION]))) {
                    /** @var array<string, mixed> $child */
                    $value[$childKey] = self::withStringPermissions($child);
                }
            }
            $data[$key] = $value;
        }

        return $data;
    }

    /**
     * Encode Attribute
     *
     * Passes the attribute $value, and $document context to a predefined filter
     * that allow you to manipulate the input format of the given attribute.
     *
     *
     * @throws DatabaseException
     */
    protected function encodeAttribute(string $name, mixed $value, Document $document): mixed
    {
        try {
            if (\array_key_exists($name, $this->instanceFilters)) {
                return $this->instanceFilters[$name]['encode']($value, $document, $this);
            }

            $type = $this->typeRegistry?->get($name);
            if ($type !== null) {
                return $type->encode($value);
            }

            if (\array_key_exists($name, self::$filters)) {
                return self::$filters[$name]['encode']($value, $document, $this);
            }
        } catch (Throwable $th) {
            throw new DatabaseException($th->getMessage(), $th->getCode(), $th);
        }

        throw new NotFoundException("Filter: {$name} not found");
    }

    /**
     * Decode Attribute
     *
     * Passes the attribute $value, and $document context to a predefined filter
     *  that allow you to manipulate the output format of the given attribute.
     *
     * @throws NotFoundException
     */
    protected function decodeAttribute(string $filter, mixed $value, Document $document, string $attribute): mixed
    {
        if (\array_key_exists($filter, $this->instanceFilters)) {
            return $this->instanceFilters[$filter]['decode']($value, $document, $this, $attribute);
        }

        $type = $this->typeRegistry?->get($filter);
        if ($type !== null) {
            return $type->decode($value);
        }

        if (\array_key_exists($filter, self::$filters)) {
            return self::$filters[$filter]['decode']($value, $document, $this, $attribute);
        }

        throw new NotFoundException("Filter \"{$filter}\" not found for attribute \"{$attribute}\"");
    }

    /**
     * Check if values are compatible with object attribute type (hashmap/multi-dimensional array)
     *
     * @param  array<mixed>  $values
     */
    private function isCompatibleObjectValue(array $values): bool
    {
        if (empty($values)) {
            return false;
        }

        foreach ($values as $value) {
            if (! \is_array($value)) {
                return false;
            }

            // Check associative array (hashmap) or nested structure
            if (empty($value)) {
                continue;
            }

            // simple indexed array => not an object
            if (\array_keys($value) === \range(0, \count($value) - 1)) {
                return false;
            }

            foreach ($value as $nestedValue) {
                if (\is_array($nestedValue)) {
                    continue;
                }
            }
        }

        return true;
    }

    /**
     * Retry a callable with exponential backoff
     *
     * @param  callable  $operation  The operation to retry
     * @param  int  $maxAttempts  Maximum number of retry attempts
     * @param  int  $initialDelayMs  Initial delay in milliseconds
     * @param  float  $multiplier  Backoff multiplier
     * @return void The result of the operation
     *
     * @throws Throwable The last exception if all retries fail
     */
    private function withRetries(
        callable $operation,
        int $maxAttempts = 3,
        int $initialDelayMs = 100,
        float $multiplier = 2.0
    ): void {
        $attempt = 0;
        $delayMs = $initialDelayMs;
        $lastException = new DatabaseException('All retry attempts failed');

        while ($attempt < $maxAttempts) {
            try {
                $operation();

                return;
            } catch (Throwable $e) {
                if (! $this->isRetryable($e)) {
                    throw $e;
                }

                $lastException = $e;
                $attempt++;

                if ($attempt >= $maxAttempts) {
                    break;
                }

                if (\extension_loaded('swoole') && Coroutine::getCid() > 0) {
                    Coroutine::sleep($delayMs / 1000);
                } else {
                    \usleep($delayMs * 1000);
                }

                $delayMs = (int) ($delayMs * $multiplier);
            }
        }

        throw $lastException;
    }

    private function isRetryable(Throwable $error): bool
    {
        if ($this->mayHaveCommitted($error) || $this->retriedByTransaction($error)) {
            return false;
        }

        foreach (self::DETERMINISTIC_FAILURES as $deterministic) {
            if ($error instanceof $deterministic) {
                return false;
            }
        }

        return true;
    }

    /**
     * Generic cleanup operation with retry logic
     *
     * @param  callable  $operation  The cleanup operation to execute
     * @param  string  $resourceType  Type of resource being cleaned up (e.g., 'attribute', 'index')
     * @param  string  $resourceId  ID of the resource being cleaned up
     * @param  int  $maxAttempts  Maximum retry attempts
     *
     * @throws DatabaseException If cleanup fails after all retries
     */
    private function cleanup(
        callable $operation,
        string $resourceType,
        string $resourceId,
        int $maxAttempts = 3
    ): void {
        try {
            $this->withRetries($operation, maxAttempts: $maxAttempts);
        } catch (Throwable $e) {
            Console::error("Failed to cleanup {$resourceType} '{$resourceId}' after {$maxAttempts} attempts: ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * Persist metadata with automatic rollback on failure
     *
     * Centralizes the common pattern of:
     * 1. Attempting to persist metadata with retry
     * 2. Rolling back database operations if metadata persistence fails
     * 3. Providing detailed error messages for both success and failure scenarios
     *
     * A failure raised after the metadata write committed (its cache invalidation or events), or a commit that
     * could not be confirmed, is rethrown unchanged and rolls nothing back: the definition it reports on may be stored.
     *
     * @param  Document  $collection  The collection document to persist
     * @param  callable|null  $rollbackOperation  Cleanup operation to run if persistence fails (null if no cleanup needed)
     * @param  bool  $shouldRollback  Whether rollback should be attempted (e.g., false for duplicates in shared tables)
     * @param  string  $operationDescription  Description of the operation for error messages
     * @param  bool  $rollbackReturnsErrors  Whether rollback operation returns error array (true) or throws (false)
     * @param  bool  $silentRollback  Whether a failed rollback is reported after the persistence error (true) or fails the call as a cleanup failure (false)
     *
     * @throws DatabaseException If metadata persistence fails after all retries
     */
    private function updateMetadata(
        Document $collection,
        ?callable $rollbackOperation,
        bool $shouldRollback,
        string $operationDescription = 'operation',
        bool $rollbackReturnsErrors = false,
        bool $silentRollback = false
    ): void {
        try {
            if ($collection->getId() !== self::METADATA) {
                $this->withRetries(
                    fn () => $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection))
                );
            }
        } catch (Throwable $e) {
            if ($this->mayHaveCommitted($e)) {
                throw $e;
            }

            $cleanupFailure = '';
            if ($shouldRollback && $rollbackOperation !== null) {
                if ($rollbackReturnsErrors) {
                    /** @var array<string> $cleanupErrors */
                    $cleanupErrors = $rollbackOperation();
                    if (! empty($cleanupErrors)) {
                        throw new DatabaseException(
                            "Failed to persist metadata after retries and cleanup encountered errors for {$operationDescription}: ".$e->getMessage().' | Cleanup errors: '.implode(', ', $cleanupErrors),
                            previous: $e
                        );
                    }
                } elseif ($silentRollback) {
                    try {
                        $rollbackOperation();
                    } catch (Throwable $cleanupError) {
                        $cleanupFailure = ' | Cleanup error: '.$cleanupError->getMessage();
                    }
                } else {
                    try {
                        $rollbackOperation();
                    } catch (Throwable $cleanupError) {
                        throw new DatabaseException(
                            "Failed to persist metadata after retries and cleanup failed for {$operationDescription}: ".$e->getMessage().' | Cleanup error: '.$cleanupError->getMessage(),
                            previous: $e
                        );
                    }
                }
            }

            throw new DatabaseException(
                "Failed to persist metadata after retries for {$operationDescription}: ".$e->getMessage().$cleanupFailure,
                previous: $e
            );
        }
    }
}

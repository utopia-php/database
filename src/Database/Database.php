<?php

namespace Utopia\Database;

use DateTime as NativeDateTime;
use DateTimeZone;
use Exception;
use Swoole\Coroutine;
use Throwable;
use Utopia\Cache\Cache;
use Utopia\CLI\Console;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\QueryTransform;
use Utopia\Database\Hook\Relationship;
use Utopia\Database\Profiler\QueryProfiler;
use Utopia\Database\Type\TypeRegistry;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Authorization\Input;
use Utopia\Database\Validator\Spatial as SpatialValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;

/**
 * High-level database interface providing CRUD operations for documents, collections, attributes, indexes, and relationships with built-in caching, filtering, validation, and authorization.
 */
class Database
{
    use Traits\Async;
    use Traits\Attributes;
    use Traits\Collections;
    use Traits\Databases;
    use Traits\Documents;
    use Traits\Entities;
    use Traits\Indexes;
    use Traits\Relationships;
    use Traits\Transactions;

    // Max limits
    public const MAX_INT = 2147483647;

    public const MAX_BIG_INT = PHP_INT_MAX;

    public const MAX_DOUBLE = PHP_FLOAT_MAX;

    public const MAX_VECTOR_DIMENSIONS = 16000;

    public const MAX_ARRAY_INDEX_LENGTH = 255;

    public const MAX_UID_DEFAULT_LENGTH = 36;

    // Min limits
    public const MIN_INT = -2147483648;

    // Global SRID for geographic coordinates (WGS84)
    public const DEFAULT_SRID = 4326;

    public const EARTH_RADIUS = 6371000;

    public const RELATION_MAX_DEPTH = 3;

    public const RELATION_QUERY_CHUNK_SIZE = 5000;

    public const METADATA = '_metadata';

    // Lengths
    public const LENGTH_KEY = 255;

    // Cache
    public const TTL = 60 * 60 * 24; // 24 hours

    public const INSERT_BATCH_SIZE = 1_000;

    public const DELETE_BATCH_SIZE = 1_000;

    /**
     * List of Internal attributes
     *
     * @var array<array<string, mixed>>
     */
    public const INTERNAL_ATTRIBUTES = [
        [
            '$id' => '$id',
            'type' => 'string',
            'size' => Database::LENGTH_KEY,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$sequence',
            'type' => 'id',
            'size' => 0,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$collection',
            'type' => 'string',
            'size' => Database::LENGTH_KEY,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$tenant',
            'type' => 'integer',
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$createdAt',
            'type' => 'datetime',
            'format' => '',
            'size' => 0,
            'signed' => false,
            'required' => false,
            'default' => null,
            'array' => false,
            'filters' => ['datetime'],
        ],
        [
            '$id' => '$updatedAt',
            'type' => 'datetime',
            'format' => '',
            'size' => 0,
            'signed' => false,
            'required' => false,
            'default' => null,
            'array' => false,
            'filters' => ['datetime'],
        ],
        [
            '$id' => '$permissions',
            'type' => 'string',
            'size' => 1_000_000,
            'signed' => true,
            'required' => false,
            'default' => [],
            'array' => false,
            'filters' => ['json'],
        ],
        [
            '$id' => '$version',
            'type' => 'integer',
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ],
    ];

    public const INTERNAL_ATTRIBUTE_KEYS = [
        '_uid',
        '_createdAt',
        '_updatedAt',
        '_permissions',
        '_version',
    ];

    public const INTERNAL_INDEXES = [
        '_id',
        '_uid',
        '_createdAt',
        '_updatedAt',
        '_permissions_id',
        '_permissions',
    ];

    /**
     * Parent Collection
     * Defines the structure for both system and custom collections
     *
     * @var array<string, mixed>
     */
    protected const COLLECTION = [
        '$id' => self::METADATA,
        '$collection' => self::METADATA,
        'name' => 'collections',
        'attributes' => [
            [
                '$id' => 'name',
                'key' => 'name',
                'type' => 'string',
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'attributes',
                'key' => 'attributes',
                'type' => 'string',
                'size' => 1000000,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => ['json'],
            ],
            [
                '$id' => 'indexes',
                'key' => 'indexes',
                'type' => 'string',
                'size' => 1000000,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => ['json'],
            ],
            [
                '$id' => 'documentSecurity',
                'key' => 'documentSecurity',
                'type' => 'boolean',
                'size' => 0,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
        ],
        'indexes' => [],
    ];

    protected Adapter $adapter;

    protected Cache $cache;

    protected string $cacheName = 'default';

    /**
     * @var array<string, array{encode: callable, decode: callable}>
     */
    protected static array $filters = [];

    /**
     * @var array<string, array{encode: callable, decode: callable}>
     */
    protected array $instanceFilters = [];

    /**
     * @var array<Lifecycle>
     */
    protected array $lifecycleHooks = [];

    /**
     * When true, lifecycle hooks are not fired.
     */
    protected bool $eventsSilenced = false;

    protected ?NativeDateTime $timestamp = null;

    protected ?Relationship $relationshipHook = null;

    protected bool $filter = true;

    /**
     * @var array<string, bool>|null
     */
    protected ?array $disabledFilters = [];

    protected bool $validate = true;

    protected bool $preserveDates = false;

    protected bool $preserveSequence = false;

    protected int $maxQueryValues = 5000;

    protected bool $migrating = false;

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
        $this->instanceFilters = $filters;

        $this->setAuthorization(new Authorization());

        self::addFilter(
            'json',
            /**
             * @return mixed
             */
            function (mixed $value) {
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
            function (mixed $value) {
                if (! is_string($value)) {
                    return $value;
                }

                $decoded = json_decode($value, true) ?? [];
                if (! is_array($decoded)) {
                    return $decoded;
                }

                /** @var array<string, mixed> $decoded */
                if (array_key_exists('$id', $decoded)) {
                    return new Document($decoded);
                } else {
                    $decoded = array_map(function ($item) {
                        if (is_array($item) && array_key_exists('$id', $item)) { // if `$id` exists, create a Document instance
                            /** @var array<string, mixed> $item */
                            return new Document($item);
                        }

                        return $item;
                    }, $decoded);
                }

                return $decoded;
            }
        );

        self::addFilter(
            'datetime',
            /**
             * @return mixed
             */
            function (mixed $value) {
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
            function (?string $value) {
                return DateTime::formatTz($value);
            }
        );

        self::addFilter(
            ColumnType::Point->value,
            /**
             * @return mixed
             */
            function (mixed $value) {
                if (! is_array($value)) {
                    return $value;
                }
                try {
                    return self::encodeSpatialData($value, ColumnType::Point->value);
                } catch (Throwable) {
                    return $value;
                }
            },
            /**
             * @return array|null
             */
            function (?string $value) {
                if ($value === null) {
                    return null;
                }
                if ($this->adapter->supports(Capability::Spatial)) {
                    return $this->adapter->decodePoint($value);
                }

                return null;
            }
        );

        self::addFilter(
            ColumnType::Linestring->value,
            /**
             * @return mixed
             */
            function (mixed $value) {
                if (! is_array($value)) {
                    return $value;
                }
                try {
                    return self::encodeSpatialData($value, ColumnType::Linestring->value);
                } catch (Throwable) {
                    return $value;
                }
            },
            /**
             * @return array|null
             */
            function (?string $value) {
                if (is_null($value)) {
                    return null;
                }
                if ($this->adapter->supports(Capability::Spatial)) {
                    return $this->adapter->decodeLinestring($value);
                }

                return null;
            }
        );

        self::addFilter(
            ColumnType::Polygon->value,
            /**
             * @return mixed
             */
            function (mixed $value) {
                if (! is_array($value)) {
                    return $value;
                }
                try {
                    return self::encodeSpatialData($value, ColumnType::Polygon->value);
                } catch (Throwable) {
                    return $value;
                }
            },
            /**
             * @return array|null
             */
            function (?string $value) {
                if (is_null($value)) {
                    return null;
                }
                if ($this->adapter->supports(Capability::Spatial)) {
                    return $this->adapter->decodePolygon($value);
                }

                return null;
            }
        );

        self::addFilter(
            ColumnType::Vector->value,
            /**
             * @return mixed
             */
            function (mixed $value) {
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
            function (?string $value) {
                if (is_null($value)) {
                    return null;
                }
                $decoded = json_decode($value, true);

                return is_array($decoded) ? $decoded : $value;
            }
        );

        self::addFilter(
            ColumnType::Object->value,
            /**
             * @return mixed
             */
            function (mixed $value) {
                if (! \is_array($value)) {
                    return $value;
                }

                return \json_encode($value);
            },
            /**
             * @return array|null
             */
            function (mixed $value) {
                if (is_null($value)) {
                    return;
                }
                // can be non string in case of mongodb as it stores the value as object
                if (! is_string($value)) {
                    return $value;
                }
                $decoded = json_decode($value, true);

                return is_array($decoded) ? $decoded : $value;
            }
        );
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
     * Get Database Adapter
     */
    public function getAdapter(): Adapter
    {
        return $this->adapter;
    }

    public function query(string $collection): QueryBuilder
    {
        return new QueryBuilder($this, $collection);
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
        $this->queryCache = $queryCache;

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
     * Get list of keywords that cannot be used
     *
     * @return string[]
     */
    public function getKeywords(): array
    {
        return $this->adapter->getKeywords();
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
    public function setTenant(?int $tenant): static
    {
        $this->adapter->setTenant($tenant);

        return $this;
    }

    /**
     * Get Tenant
     *
     * Get tenant to use if tables are shared
     */
    public function getTenant(): ?int
    {
        return $this->adapter->getTenant();
    }

    /**
     * With Tenant
     *
     * Execute a callback with a specific tenant
     */
    public function withTenant(?int $tenant, callable $callback): mixed
    {
        $previous = $this->adapter->getTenant();
        $this->adapter->setTenant($tenant);

        try {
            return $callback();
        } finally {
            $this->adapter->setTenant($previous);
        }
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
        $this->adapter->setTimeout($milliseconds, $event);

        return $this;
    }

    /**
     * Clear maximum query execution time
     */
    public function clearTimeout(Event $event = Event::All): void
    {
        $this->adapter->clearTimeout($event);
    }

    /**
     * Set the relationship hook used to resolve related documents during reads and writes.
     *
     * @param Relationship|null $hook The relationship hook, or null to disable.
     * @return $this
     */
    public function setRelationshipHook(?Relationship $hook): self
    {
        $this->relationshipHook = $hook;

        return $this;
    }

    /**
     * Get the current relationship hook.
     *
     * @return Relationship|null The relationship hook, or null if not set.
     */
    public function getRelationshipHook(): ?Relationship
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
        $this->preserveDates = $preserve;

        return $this;
    }

    /**
     * Get whether date preservation is enabled.
     *
     * @return bool True if dates are being preserved.
     */
    public function getPreserveDates(): bool
    {
        return $this->preserveDates;
    }

    /**
     * Execute a callback with date preservation enabled, restoring the previous state afterward.
     *
     * @param callable $callback The callback to execute.
     * @return mixed The callback's return value.
     */
    public function withPreserveDates(callable $callback): mixed
    {
        $previous = $this->preserveDates;
        $this->preserveDates = true;

        try {
            return $callback();
        } finally {
            $this->preserveDates = $previous;
        }
    }

    /**
     * Set whether to preserve original sequence values instead of auto-generating them.
     *
     * @param bool $preserve True to preserve sequence values on write operations.
     * @return $this
     */
    public function setPreserveSequence(bool $preserve): static
    {
        $this->preserveSequence = $preserve;

        return $this;
    }

    /**
     * Get whether sequence preservation is enabled.
     *
     * @return bool True if sequence values are being preserved.
     */
    public function getPreserveSequence(): bool
    {
        return $this->preserveSequence;
    }

    /**
     * Execute a callback with sequence preservation enabled, restoring the previous state afterward.
     *
     * @param callable $callback The callback to execute.
     * @return mixed The callback's return value.
     */
    public function withPreserveSequence(callable $callback): mixed
    {
        $previous = $this->preserveSequence;
        $this->preserveSequence = true;

        try {
            return $callback();
        } finally {
            $this->preserveSequence = $previous;
        }
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
        $this->documentTypes = [];

        return $this;
    }

    /**
     * Enable or disable LOCK=SHARED during ALTER TABLE operation
     *
     * Set lock mode when altering tables
     */
    public function enableLocks(bool $enabled): static
    {
        if ($this->adapter->supports(Capability::AlterLock)) {
            $this->adapter->enableAlterLocks($enabled);
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
        $this->validate = true;

        return $this;
    }

    /**
     * Disable validation
     *
     * @return $this
     */
    public function disableValidation(): static
    {
        $this->validate = false;

        return $this;
    }

    /**
     * Skip Validation
     *
     * Execute a callback without validation
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function skipValidation(callable $callback): mixed
    {
        $initial = $this->validate;
        $this->disableValidation();

        try {
            return $callback();
        } finally {
            $this->validate = $initial;
        }
    }

    /**
     * Register a lifecycle hook to receive database events.
     */
    public function addLifecycleHook(Lifecycle $hook): static
    {
        $this->lifecycleHooks[] = $hook;

        return $this;
    }

    /**
     * Register a query transform hook on the adapter.
     */
    public function addQueryTransform(string $name, QueryTransform $transform): static
    {
        $this->adapter->addQueryTransform($name, $transform);

        return $this;
    }

    /**
     * Remove a query transform hook from the adapter.
     */
    public function removeQueryTransform(string $name): static
    {
        $this->adapter->removeQueryTransform($name);

        return $this;
    }

    /**
     * Silence lifecycle hooks for calls inside the callback.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function silent(callable $callback): mixed
    {
        $previous = $this->eventsSilenced;
        $this->eventsSilenced = true;

        try {
            return $callback();
        } finally {
            $this->eventsSilenced = $previous;
        }
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
        self::$filters[$name] = [
            'encode' => $encode,
            'decode' => $decode,
        ];
    }

    /**
     * Enable filters
     *
     * @return $this
     */
    public function enableFilters(): static
    {
        $this->filter = true;

        return $this;
    }

    /**
     * Disable filters
     *
     * @return $this
     */
    public function disableFilters(): static
    {
        $this->filter = false;

        return $this;
    }

    /**
     * Skip filters
     *
     * Execute a callback without filters
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
            $initial = $this->filter;
            $this->disableFilters();

            try {
                return $callback();
            } finally {
                $this->filter = $initial;
            }
        }

        $previous = $this->filter;
        $previousDisabled = $this->disabledFilters;
        $disabled = [];
        foreach ($filters as $name) {
            $disabled[$name] = true;
        }
        $this->disabledFilters = $disabled;

        try {
            return $callback();
        } finally {
            $this->filter = $previous;
            $this->disabledFilters = $previousDisabled;
        }
    }

    /**
     * Get instance filters
     *
     * @return array<string, array{encode: callable, decode: callable}>
     */
    public function getInstanceFilters(): array
    {
        return $this->instanceFilters;
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
        /** @var array<array<string, mixed>> $attributes */
        $attributes = $collection->getAttribute('attributes', []);
        $internalDateAttributes = ['$createdAt', '$updatedAt'];
        foreach ($this->getInternalAttributes() as $attribute) {
            $attributes[] = $attribute;
        }

        foreach ($attributes as $attribute) {
            /** @var string $key */
            $key = $attribute['$id'] ?? '';
            $array = $attribute['array'] ?? false;
            $default = $attribute['default'] ?? null;
            /** @var array<string> $filters */
            $filters = $attribute['filters'] ?? [];
            $value = $document->getAttribute($key);

            if (in_array($key, $internalDateAttributes) && is_string($value) && empty($value)) {
                $document->setAttribute($key, null);

                continue;
            }

            if ($key === '$permissions') {
                continue;
            }

            // Continue on optional param with no default
            if (is_null($value) && is_null($default)) {
                continue;
            }

            // Skip encoding for Operator objects
            if ($value instanceof Operator) {
                continue;
            }

            // Assign default only if no value provided
            if (is_null($value) && ! is_null($default)) {
                // Skip applying defaults during updates to avoid resetting unspecified attributes
                if (! $applyDefaults) {
                    continue;
                }
                $value = ($array) ? $default : [$default];
            } else {
                $value = ($array) ? $value : [$value];
            }

            /** @var array<int|string, mixed> $value */
            foreach ($value as $index => $node) {
                if ($node !== null) {
                    foreach ($filters as $filter) {
                        $node = $this->encodeAttribute($filter, $node, $document);
                    }
                    $value[$index] = $node;
                }
            }

            if (! $array) {
                $value = $value[0];
            }
            $document->setAttribute($key, $value);
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
        /** @var array<array<string, mixed>|Document> $allAttributes */
        $allAttributes = $collection->getAttribute('attributes', []);
        $attributes = \array_filter(
            $allAttributes,
            fn (array|Document $attribute) => $attribute['type'] !== ColumnType::Relationship->value
        );

        $relationships = \array_filter(
            $allAttributes,
            fn (array|Document $attribute) => $attribute['type'] === ColumnType::Relationship->value
        );

        $filteredValue = [];

        foreach ($relationships as $relationship) {
            /** @var string $key */
            $key = $relationship['$id'] ?? '';

            if (
                \array_key_exists($key, (array) $document)
                || \array_key_exists($this->adapter->filter($key), (array) $document)
            ) {
                $value = $document->getAttribute($key);
                $value ??= $document->getAttribute($this->adapter->filter($key));
                $document->removeAttribute($this->adapter->filter($key));
                $document->setAttribute($key, $value);
            }
        }

        foreach ($this->getInternalAttributes() as $attribute) {
            $attributes[] = $attribute;
        }

        foreach ($attributes as $attribute) {
            /** @var string $key */
            $key = $attribute['$id'] ?? '';
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;
            /** @var array<string> $filters */
            $filters = $attribute['filters'] ?? [];
            $value = $document->getAttribute($key);

            if ($key === '$permissions') {
                continue;
            }

            if (\is_null($value)) {
                $value = $document->getAttribute($this->adapter->filter($key));

                if (! \is_null($value)) {
                    $document->removeAttribute($this->adapter->filter($key));
                }
            }

            // Skip decoding for Operator objects (shouldn't happen, but safety check)
            if ($value instanceof Operator) {
                continue;
            }

            $value = ($array) ? $value : [$value];
            $value = (is_null($value)) ? [] : $value;

            /** @var array<int|string, mixed> $value */
            foreach ($value as $index => $node) {
                foreach (\array_reverse($filters) as $filter) {
                    $node = $this->decodeAttribute($filter, $node, $document, $key);
                }
                $value[$index] = $node;
            }

            $filteredValue[$key] = ($array) ? $value : $value[0];

            if (
                empty($selections)
                || \in_array($key, $selections)
                || \in_array('*', $selections)
            ) {
                $document->setAttribute($key, ($array) ? $value : $value[0]);
            }
        }

        $hasRelationshipSelections = false;
        if (! empty($selections)) {
            foreach ($selections as $selection) {
                if (\str_contains($selection, '.')) {
                    $hasRelationshipSelections = true;
                    break;
                }
            }
        }

        if ($hasRelationshipSelections && ! empty($selections) && ! \in_array('*', $selections)) {
            foreach ($allAttributes as $attribute) {
                /** @var string $key */
                $key = $attribute['$id'] ?? '';

                if ($attribute['type'] === ColumnType::Relationship->value || $key === '$permissions') {
                    continue;
                }

                if (! in_array($key, $selections) && isset($filteredValue[$key])) {
                    $document->setAttribute($key, $filteredValue[$key]);
                }
            }
        }

        return $document;
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
        if (! $this->adapter->supports(Capability::Casting)) {
            return $document;
        }

        /** @var array<array<string, mixed>> $attributes */
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($this->getInternalAttributes() as $attribute) {
            $attributes[] = $attribute;
        }

        foreach ($attributes as $attribute) {
            /** @var string $key */
            $key = $attribute['$id'] ?? '';
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;
            $value = $document->getAttribute($key, null);
            if (is_null($value)) {
                continue;
            }

            if ($key === '$permissions') {
                continue;
            }

            if ($array) {
                $value = ! is_string($value)
                    ? $value
                    : json_decode($value, true);
            } else {
                $value = [$value];
            }

            /** @var array<int|string, scalar|null> $value */
            foreach ($value as $index => $node) {
                $node = match ($type) {
                    ColumnType::Id->value => (string) $node,
                    ColumnType::Boolean->value => (bool) $node,
                    ColumnType::Integer->value => (int) $node,
                    ColumnType::Double->value => (float) $node,
                    default => $node,
                };

                $value[$index] = $node;
            }

            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }

        return $document;
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
     * Executes $callback with $timestamp set to $requestTimestamp
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function withRequestTimestamp(?NativeDateTime $requestTimestamp, callable $callback): mixed
    {
        $previous = $this->timestamp;
        $this->timestamp = $requestTimestamp;
        try {
            $result = $callback();
        } finally {
            $this->timestamp = $previous;
        }

        return $result;
    }

    /**
     * Get getConnection Id
     *
     * @throws Exception
     */
    public function getConnectionId(): string
    {
        return $this->adapter->getConnectionId();
    }

    /**
     * Ping Database
     */
    public function ping(): bool
    {
        return $this->adapter->ping();
    }

    /**
     * Reconnect to the database, re-establishing any dropped connections.
     */
    public function reconnect(): void
    {
        $this->adapter->reconnect();
    }

    /**
     * Get adapter attribute limit, accounting for internal metadata
     * Returns 0 to indicate no limit
     */
    public function getLimitForAttributes(): int
    {
        if ($this->adapter->getLimitForAttributes() === 0) {
            return 0;
        }

        return $this->adapter->getLimitForAttributes() - $this->adapter->getCountOfDefaultAttributes();
    }

    /**
     * Get adapter index limit
     */
    public function getLimitForIndexes(): int
    {
        return $this->adapter->getLimitForIndexes() - $this->adapter->getCountOfDefaultIndexes();
    }

    /**
     * @param  array<Query>  $queries
     * @return array<Query>
     *
     * @throws QueryException
     * @throws \Utopia\Database\Exception
     */
    public function convertQueries(Document $collection, array $queries): array
    {
        foreach ($queries as $index => $query) {
            if ($query->isNested()) {
                /** @var array<Query> $nestedQueries */
                $nestedQueries = $query->getValues();
                $values = $this->convertQueries($collection, $nestedQueries);
                $query->setValues($values);
            }

            $query = $this->convertQuery($collection, $query);

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
        /**
         * @var array<Document> $attributes
         */
        $attributes = $collection->getAttribute('attributes', []);

        foreach (Database::INTERNAL_ATTRIBUTES as $attribute) {
            $attributes[] = new Document($attribute);
        }

        $queryAttribute = $query->getAttribute();
        $isNestedQueryAttribute = $this->getAdapter()->supports(Capability::DefinedAttributes) && $this->adapter->supports(Capability::Objects) && \str_contains($queryAttribute, '.');

        $attribute = new Document();

        foreach ($attributes as $attr) {
            if ($attr->getId() === $query->getAttribute()) {
                $attribute = $attr;
            } elseif ($isNestedQueryAttribute) {
                // nested object query
                $baseAttribute = \explode('.', $queryAttribute, 2)[0];
                if ($baseAttribute === $attr->getId() && $attr->getAttribute('type') === ColumnType::Object->value) {
                    $query->setAttributeType(ColumnType::Object->value);
                }
            }
        }

        if (! $attribute->isEmpty()) {
            /** @var bool $isArray */
            $isArray = $attribute->getAttribute('array', false);
            /** @var string $attrType */
            $attrType = $attribute->getAttribute('type');
            $query->setOnArray($isArray);
            $query->setAttributeType($attrType);

            if ($attrType == ColumnType::Datetime->value) {
                $values = $query->getValues();
                foreach ($values as $valueIndex => $value) {
                    try {
                        /** @var string $value */
                        $values[$valueIndex] = $this->adapter->supports(Capability::UTCCasting)
                            ? $this->adapter->setUTCDatetime($value)
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
     * @return array<array<string, mixed>>
     */
    /**
     * @return array<string, mixed>
     */
    protected static function collectionMeta(): array
    {
        $collection = self::COLLECTION;
        $collection['attributes'] = \array_map(
            fn (array $attr) => new Document($attr),
            $collection['attributes']
        );

        return $collection;
    }

    /**
     * Get the list of internal attribute definitions (e.g., $id, $createdAt, $permissions) as typed Attribute objects.
     *
     * @return array<Attribute>
     */
    public static function internalAttributes(): array
    {
        return \array_map(
            fn (array $attr): Attribute => Attribute::fromDocument(new Document($attr)),
            self::INTERNAL_ATTRIBUTES
        );
    }

    /**
     * Get the internal attribute definitions for the current adapter, excluding tenant if shared tables are disabled.
     *
     * @return array<array<string, mixed>> The internal attribute configurations.
     */
    public function getInternalAttributes(): array
    {
        $attributes = self::INTERNAL_ATTRIBUTES;

        if (! $this->adapter->getSharedTables()) {
            $attributes = \array_filter(Database::INTERNAL_ATTRIBUTES, function ($attribute) {
                return $attribute['$id'] !== '$tenant';
            });
        }

        return $attributes;
    }

    /**
     * Get Schema Attributes
     *
     * @return array<Document>
     *
     * @throws DatabaseException
     */
    public function getSchemaAttributes(string $collection): array
    {
        return $this->adapter->getSchemaAttributes($collection);
    }

    /**
     * @param  array<string>  $selects
     * @return array{0: string, 1: string, 2: string}
     */
    public function getCacheKeys(string $collectionId, ?string $documentId = null, array $selects = []): array
    {
        if ($this->adapter->supports(Capability::Hostname)) {
            $hostname = $this->adapter->getHostname();
        }

        $tenantSegment = $this->adapter->getTenant();

        if ($collectionId === self::METADATA && isset($this->globalCollections[$documentId])) {
            $tenantSegment = null;
        }

        $collectionKey = \sprintf(
            '%s-cache-%s:%s:%s:collection:%s',
            $this->cacheName,
            $hostname ?? '',
            $this->getNamespace(),
            $tenantSegment,
            $collectionId
        );

        if ($documentId) {
            $documentKey = $documentHashKey = "{$collectionKey}:{$documentId}";

            if (! empty($selects)) {
                $documentHashKey = $documentKey.':'.\md5(\implode($selects));
            }
        }

        return [
            $collectionKey,
            $documentKey ?? '',
            $documentHashKey ?? '',
        ];
    }

    /**
     * Fire an event to all registered lifecycle hooks.
     * Exceptions from hooks are silently caught.
     */
    protected function trigger(Event $event, mixed $data = null): void
    {
        if ($this->eventsSilenced) {
            return;
        }

        foreach ($this->lifecycleHooks as $hook) {
            try {
                $hook->handle($event, $data);
            } catch (Throwable) {
                // Lifecycle hooks must not break business logic
            }
        }
    }

    /**
     * Create a document instance of the appropriate type
     *
     * @param  string  $collection  Collection ID
     * @param  array<string, mixed>  $data  Document data
     */
    protected function createDocumentInstance(string $collection, array $data): Document
    {
        $className = $this->documentTypes[$collection] ?? Document::class;

        return new $className($data);
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
        if (! array_key_exists($name, self::$filters) && ! array_key_exists($name, $this->instanceFilters)) {
            throw new NotFoundException("Filter: {$name} not found");
        }

        try {
            if (\array_key_exists($name, $this->instanceFilters)) {
                $value = $this->instanceFilters[$name]['encode']($value, $document, $this);
            } else {
                $value = self::$filters[$name]['encode']($value, $document, $this);
            }
        } catch (Throwable $th) {
            throw new DatabaseException($th->getMessage(), $th->getCode(), $th);
        }

        return $value;
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
        if (! $this->filter) {
            return $value;
        }

        if (! \is_null($this->disabledFilters) && isset($this->disabledFilters[$filter])) {
            return $value;
        }

        if (! array_key_exists($filter, self::$filters) && ! array_key_exists($filter, $this->instanceFilters)) {
            throw new NotFoundException("Filter \"{$filter}\" not found for attribute \"{$attribute}\"");
        }

        if (array_key_exists($filter, $this->instanceFilters)) {
            $value = $this->instanceFilters[$filter]['decode']($value, $document, $this);
        } else {
            $value = self::$filters[$filter]['decode']($value, $document, $this);
        }

        return $value;
    }

    /**
     * Encode spatial data from array format to WKT (Well-Known Text) format
     *
     * @throws DatabaseException
     */
    protected function encodeSpatialData(mixed $value, string $type): string
    {
        $validator = new SpatialValidator($type);
        if (! $validator->isValid($value)) {
            throw new StructureException($validator->getDescription());
        }

        /** @var array<int, array<int, float|int>|array<int, array<int, float|int>>> $value */
        switch ($type) {
            case ColumnType::Point->value:
                /** @var array{0: float|int, 1: float|int} $value */
                return "POINT({$value[0]} {$value[1]})";

            case ColumnType::Linestring->value:
                $points = [];
                /** @var array<int, array{0: float|int, 1: float|int}> $value */
                foreach ($value as $point) {
                    $points[] = "{$point[0]} {$point[1]}";
                }

                return 'LINESTRING('.implode(', ', $points).')';

            case ColumnType::Polygon->value:
                /** @var array<int, mixed> $value */
                // Check if this is a single ring (flat array of points) or multiple rings
                $isSingleRing = count($value) > 0 && is_array($value[0]) &&
                    count($value[0]) === 2 && is_numeric($value[0][0]) && is_numeric($value[0][1]);

                if ($isSingleRing) {
                    // Convert single ring format [[x1,y1], [x2,y2], ...] to multi-ring format
                    $value = [$value];
                }

                $rings = [];
                /** @var array<int, array<int, array{0: float|int, 1: float|int}>> $value */
                foreach ($value as $ring) {
                    $points = [];
                    foreach ($ring as $point) {
                        $points[] = "{$point[0]} {$point[1]}";
                    }
                    $rings[] = '('.implode(', ', $points).')';
                }

                return 'POLYGON('.implode(', ', $rings).')';

            default:
                throw new DatabaseException('Unknown spatial type: '.$type);
        }
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
     * @param  Document  $collection  The collection document to persist
     * @param  callable|null  $rollbackOperation  Cleanup operation to run if persistence fails (null if no cleanup needed)
     * @param  bool  $shouldRollback  Whether rollback should be attempted (e.g., false for duplicates in shared tables)
     * @param  string  $operationDescription  Description of the operation for error messages
     * @param  bool  $rollbackReturnsErrors  Whether rollback operation returns error array (true) or throws (false)
     * @param  bool  $silentRollback  Whether rollback errors should be silently caught (true) or thrown (false)
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
            // Attempt rollback only if conditions are met
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
                    // Silent mode: swallow rollback errors
                    try {
                        $rollbackOperation();
                    } catch (Throwable $e) {
                        // Silent rollback - errors are swallowed
                    }
                } else {
                    // Regular mode: rollback throws on failure
                    try {
                        $rollbackOperation();
                    } catch (Throwable $ex) {
                        throw new DatabaseException(
                            "Failed to persist metadata after retries and cleanup failed for {$operationDescription}: ".$ex->getMessage().' | Cleanup error: '.$e->getMessage(),
                            previous: $e
                        );
                    }
                }
            }

            throw new DatabaseException(
                "Failed to persist metadata after retries for {$operationDescription}: ".$e->getMessage(),
                previous: $e
            );
        }
    }
}

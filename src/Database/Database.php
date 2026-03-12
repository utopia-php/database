<?php

namespace Utopia\Database;

use Exception;
use Swoole\Coroutine;
use Throwable;
use Utopia\Cache\Cache;
use Utopia\CLI\Console;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Attribute as AttributeValidator;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Authorization\Input;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Database\Validator\IndexDependency as IndexDependencyValidator;
use Utopia\Database\Validator\PartialStructure;
use Utopia\Database\Validator\Permissions;
use Utopia\Database\Validator\Queries\Document as DocumentValidator;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;
use Utopia\Database\Capability;
use Utopia\Database\CursorDirection;
use Utopia\Database\OrderDirection;
use Utopia\Database\PermissionType;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Spatial as SpatialValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Database\Hook\Relationship;
use Utopia\Database\Traits;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

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

    // Events
    public const EVENT_ALL = '*';

    public const EVENT_DATABASE_LIST = 'database_list';
    public const EVENT_DATABASE_CREATE = 'database_create';
    public const EVENT_DATABASE_DELETE = 'database_delete';

    public const EVENT_COLLECTION_LIST = 'collection_list';
    public const EVENT_COLLECTION_CREATE = 'collection_create';
    public const EVENT_COLLECTION_UPDATE = 'collection_update';
    public const EVENT_COLLECTION_READ = 'collection_read';
    public const EVENT_COLLECTION_DELETE = 'collection_delete';

    public const EVENT_DOCUMENT_FIND = 'document_find';
    public const EVENT_DOCUMENT_PURGE = 'document_purge';
    public const EVENT_DOCUMENT_CREATE = 'document_create';
    public const EVENT_DOCUMENTS_CREATE = 'documents_create';
    public const EVENT_DOCUMENT_READ = 'document_read';
    public const EVENT_DOCUMENT_UPDATE = 'document_update';
    public const EVENT_DOCUMENTS_UPDATE = 'documents_update';
    public const EVENT_DOCUMENTS_UPSERT = 'documents_upsert';
    public const EVENT_DOCUMENT_DELETE = 'document_delete';
    public const EVENT_DOCUMENTS_DELETE = 'documents_delete';
    public const EVENT_DOCUMENT_COUNT = 'document_count';
    public const EVENT_DOCUMENT_SUM = 'document_sum';
    public const EVENT_DOCUMENT_INCREASE = 'document_increase';
    public const EVENT_DOCUMENT_DECREASE = 'document_decrease';

    public const EVENT_PERMISSIONS_CREATE = 'permissions_create';
    public const EVENT_PERMISSIONS_READ = 'permissions_read';
    public const EVENT_PERMISSIONS_DELETE = 'permissions_delete';

    public const EVENT_ATTRIBUTE_CREATE = 'attribute_create';
    public const EVENT_ATTRIBUTES_CREATE = 'attributes_create';
    public const EVENT_ATTRIBUTE_UPDATE = 'attribute_update';
    public const EVENT_ATTRIBUTE_DELETE = 'attribute_delete';

    public const EVENT_INDEX_RENAME = 'index_rename';
    public const EVENT_INDEX_CREATE = 'index_create';
    public const EVENT_INDEX_DELETE = 'index_delete';

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
            'filters' => ['datetime']
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
            'filters' => ['datetime']
        ],
        [
            '$id' => '$permissions',
            'type' => 'string',
            'size' => 1_000_000,
            'signed' => true,
            'required' => false,
            'default' => [],
            'array' => false,
            'filters' => ['json']
        ],
    ];

    public const INTERNAL_ATTRIBUTE_KEYS = [
        '_uid',
        '_createdAt',
        '_updatedAt',
        '_permissions',
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
                'filters' => []
            ]
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
     * @var array<string, array<string, callable>>
     */
    protected array $listeners = [
        '*' => [],
    ];

    /**
     * Array in which the keys are the names of database listeners that
     * should be skipped when dispatching events. null $silentListeners
     * will skip all listeners.
     *
     * @var ?array<string, bool>
     */
    protected ?array $silentListeners = [];

    protected ?\DateTime $timestamp = null;

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
     * @var array<string, class-string<Document>>
     */
    protected array $documentTypes = [];

    /**
     * @var Authorization
     */
    private Authorization $authorization;

    /**
     * @param Adapter $adapter
     * @param Cache $cache
     * @param array<string, array{encode: callable, decode: callable}> $filters
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
             * @param mixed $value
             * @return mixed
             */
            function (mixed $value) {
                $value = ($value instanceof Document) ? $value->getArrayCopy() : $value;

                if (!is_array($value) && !$value instanceof \stdClass) {
                    return $value;
                }

                return json_encode($value);
            },
            /**
             * @param mixed $value
             * @return mixed
             * @throws Exception
             */
            function (mixed $value) {
                if (!is_string($value)) {
                    return $value;
                }

                $value = json_decode($value, true) ?? [];

                if (array_key_exists('$id', $value)) {
                    return new Document($value);
                } else {
                    $value = array_map(function ($item) {
                        if (is_array($item) && array_key_exists('$id', $item)) { // if `$id` exists, create a Document instance
                            return new Document($item);
                        }
                        return $item;
                    }, $value);
                }

                return $value;
            }
        );

        self::addFilter(
            'datetime',
            /**
             * @param mixed $value
             * @return mixed
             */
            function (mixed $value) {
                if (is_null($value)) {
                    return;
                }
                try {
                    $value = new \DateTime($value);
                    $value->setTimezone(new \DateTimeZone(date_default_timezone_get()));
                    return DateTime::format($value);
                } catch (\Throwable) {
                    return $value;
                }
            },
            /**
             * @param string|null $value
             * @return string|null
             */
            function (?string $value) {
                return DateTime::formatTz($value);
            }
        );

        self::addFilter(
            ColumnType::Point->value,
            /**
             * @param mixed $value
             * @return mixed
             */
            function (mixed $value) {
                if (!is_array($value)) {
                    return $value;
                }
                try {
                    return self::encodeSpatialData($value, ColumnType::Point->value);
                } catch (\Throwable) {
                    return $value;
                }
            },
            /**
             * @param string|null $value
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
             * @param mixed $value
             * @return mixed
             */
            function (mixed $value) {
                if (!is_array($value)) {
                    return $value;
                }
                try {
                    return self::encodeSpatialData($value, ColumnType::Linestring->value);
                } catch (\Throwable) {
                    return $value;
                }
            },
            /**
             * @param string|null $value
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
             * @param mixed $value
             * @return mixed
             */
            function (mixed $value) {
                if (!is_array($value)) {
                    return $value;
                }
                try {
                    return self::encodeSpatialData($value, ColumnType::Polygon->value);
                } catch (\Throwable) {
                    return $value;
                }
            },
            /**
             * @param string|null $value
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
             * @param mixed $value
             * @return mixed
             */
            function (mixed $value) {
                if (!\is_array($value)) {
                    return $value;
                }
                if (!\array_is_list($value)) {
                    return $value;
                }
                foreach ($value as $item) {
                    if (!\is_int($item) && !\is_float($item)) {
                        return $value;
                    }
                }

                return \json_encode(\array_map(\floatval(...), $value));
            },
            /**
             * @param string|null $value
             * @return array|null
             */
            function (?string $value) {
                if (is_null($value)) {
                    return null;
                }
                if (!is_string($value)) {
                    return $value;
                }
                $decoded = json_decode($value, true);
                return is_array($decoded) ? $decoded : $value;
            }
        );

        self::addFilter(
            ColumnType::Object->value,
            /**
             * @param mixed $value
             * @return mixed
             */
            function (mixed $value) {
                if (!\is_array($value)) {
                    return $value;
                }

                return \json_encode($value);
            },
            /**
             * @param mixed $value
             * @return array|null
             */
            function (mixed $value) {
                if (is_null($value)) {
                    return;
                }
                // can be non string in case of mongodb as it stores the value as object
                if (!is_string($value)) {
                    return $value;
                }
                $decoded = json_decode($value, true);
                return is_array($decoded) ? $decoded : $value;
            }
        );
    }

    /**
     * Add listener to events
     * Passing a null $callback will remove the listener
     *
     * @param string $event
     * @param string $name
     * @param ?callable $callback
     * @return static
     */
    public function on(string $event, string $name, ?callable $callback): static
    {
        if (empty($callback)) {
            unset($this->listeners[$event][$name]);
            return $this;
        }

        if (!isset($this->listeners[$event])) {
            $this->listeners[$event] = [];
        }
        $this->listeners[$event][$name] = $callback;

        return $this;
    }

    /**
     * Add a transformation to be applied to a query string before an event occurs
     *
     * @param string $event
     * @param string $name
     * @param callable $callback
     * @return $this
     */
    public function before(string $event, string $name, callable $callback): static
    {
        $this->adapter->before($event, $name, $callback);

        return $this;
    }

    /**
     * Silent event generation for calls inside the callback
     *
     * @template T
     * @param callable(): T $callback
     * @param array<string>|null $listeners List of listeners to silence; if null, all listeners will be silenced
     * @return T
     */
    public function silent(callable $callback, ?array $listeners = null): mixed
    {
        $previous = $this->silentListeners;

        if (is_null($listeners)) {
            $this->silentListeners = null;
        } else {
            $silentListeners = [];
            foreach ($listeners as $listener) {
                $silentListeners[$listener] = true;
            }
            $this->silentListeners = $silentListeners;
        }

        try {
            return $callback();
        } finally {
            $this->silentListeners = $previous;
        }
    }

    /**
     * Get getConnection Id
     *
     * @return string
     * @throws Exception
     */
    public function getConnectionId(): string
    {
        return $this->adapter->getConnectionId();
    }
    /**
     * Trigger callback for events
     *
     * @param string $event
     * @param mixed $args
     * @return void
     */
    protected function trigger(string $event, mixed $args = null): void
    {
        if (\is_null($this->silentListeners)) {
            return;
        }
        foreach ($this->listeners[self::EVENT_ALL] as $name => $callback) {
            if (isset($this->silentListeners[$name])) {
                continue;
            }
            $callback($event, $args);
        }

        foreach (($this->listeners[$event] ?? []) as $name => $callback) {
            if (isset($this->silentListeners[$name])) {
                continue;
            }
            $callback($event, $args);
        }
    }

    /**
     * Executes $callback with $timestamp set to $requestTimestamp
     *
     * @template T
     * @param ?\DateTime $requestTimestamp
     * @param callable(): T $callback
     * @return T
     */
    public function withRequestTimestamp(?\DateTime $requestTimestamp, callable $callback): mixed
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
     * Set Namespace.
     *
     * Set namespace to divide different scope of data sets
     *
     * @param string $namespace
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
     *
     * @return string
     */
    public function getNamespace(): string
    {
        return $this->adapter->getNamespace();
    }

    /**
     * Set database to use for current scope
     *
     * @param string $name
     *
     * @return static
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
     * @return string
     * @throws DatabaseException
     */
    public function getDatabase(): string
    {
        return $this->adapter->getDatabase();
    }

    /**
     * Set the cache instance
     *
     * @param Cache $cache
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
     *
     * @return Cache
     */
    public function getCache(): Cache
    {
        return $this->cache;
    }

    /**
     * Set the name to use for cache
     *
     * @param string $name
     * @return $this
     */
    public function setCacheName(string $name): static
    {
        $this->cacheName = $name;

        return $this;
    }

    /**
     * Get the cache name
     *
     * @return string
     */
    public function getCacheName(): string
    {
        return $this->cacheName;
    }

    /**
     * Set a metadata value to be printed in the query comments
     *
     * @param string $key
     * @param mixed $value
     * @return static
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
     * Sets instance of authorization for permission checks
     *
     * @param Authorization $authorization
     * @return self
     */
    public function setAuthorization(Authorization $authorization): self
    {
        $this->adapter->setAuthorization($authorization);
        $this->authorization = $authorization;
        return $this;
    }

    /**
     * Get Authorization
     *
     * @return Authorization
     */
    public function getAuthorization(): Authorization
    {
        return $this->authorization;
    }

    public function setRelationshipHook(?Relationship $hook): self
    {
        $this->relationshipHook = $hook;
        return $this;
    }

    public function getRelationshipHook(): ?Relationship
    {
        return $this->relationshipHook;
    }

    /**
     * Clear metadata
     *
     * @return void
     */
    public function resetMetadata(): void
    {
        $this->adapter->resetMetadata();
    }

    /**
     * Set maximum query execution time
     *
     * @param int $milliseconds
     * @param string $event
     * @return static
     * @throws Exception
     */
    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): static
    {
        $this->adapter->setTimeout($milliseconds, $event);

        return $this;
    }

    /**
     * Clear maximum query execution time
     *
     * @param string $event
     * @return void
     */
    public function clearTimeout(string $event = Database::EVENT_ALL): void
    {
        $this->adapter->clearTimeout($event);
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
     * @param callable(): T $callback
     * @param array<string>|null $filters
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
     * @param callable(): T $callback
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
     * Get shared tables
     *
     * Get whether to share tables between tenants
     * @return bool
     */
    public function getSharedTables(): bool
    {
        return $this->adapter->getSharedTables();
    }

    /**
     * Set shard tables
     *
     * Set whether to share tables between tenants
     *
     * @param bool $sharedTables
     * @return static
     */
    public function setSharedTables(bool $sharedTables): static
    {
        $this->adapter->setSharedTables($sharedTables);

        return $this;
    }

    /**
     * Set Tenant
     *
     * Set tenant to use if tables are shared
     *
     * @param ?int $tenant
     * @return static
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
     *
     * @return ?int
     */
    public function getTenant(): ?int
    {
        return $this->adapter->getTenant();
    }

    /**
     * With Tenant
     *
     * Execute a callback with a specific tenant
     *
     * @param int|null $tenant
     * @param callable $callback
     * @return mixed
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
     *
     * @param bool $enabled
     * @return static
     */
    public function setTenantPerDocument(bool $enabled): static
    {
        $this->adapter->setTenantPerDocument($enabled);

        return $this;
    }

    /**
     * Get whether to allow creating documents with tenant set per document.
     *
     * @return bool
     */
    public function getTenantPerDocument(): bool
    {
        return $this->adapter->getTenantPerDocument();
    }

    /**
     * Enable or disable LOCK=SHARED during ALTER TABLE operation
     *
     * Set lock mode when altering tables
     *
     * @param bool $enabled
     * @return static
     */
    public function enableLocks(bool $enabled): static
    {
        if ($this->adapter->supports(Capability::AlterLock)) {
            $this->adapter->enableAlterLocks($enabled);
        }

        return $this;
    }

    /**
     * Set custom document class for a collection
     *
     * @param string $collection Collection ID
     * @param class-string<Document> $className Fully qualified class name that extends Document
     * @return static
     * @throws DatabaseException
     */
    public function setDocumentType(string $collection, string $className): static
    {
        if (!\class_exists($className)) {
            throw new DatabaseException("Class {$className} does not exist");
        }

        if (!\is_subclass_of($className, Document::class)) {
            throw new DatabaseException("Class {$className} must extend " . Document::class);
        }

        $this->documentTypes[$collection] = $className;

        return $this;
    }

    /**
     * Get custom document class for a collection
     *
     * @param string $collection Collection ID
     * @return class-string<Document>|null
     */
    public function getDocumentType(string $collection): ?string
    {
        return $this->documentTypes[$collection] ?? null;
    }

    /**
     * Clear document type mapping for a collection
     *
     * @param string $collection Collection ID
     * @return static
     */
    public function clearDocumentType(string $collection): static
    {
        unset($this->documentTypes[$collection]);

        return $this;
    }

    /**
     * Clear all document type mappings
     *
     * @return static
     */
    public function clearAllDocumentTypes(): static
    {
        $this->documentTypes = [];

        return $this;
    }

    /**
     * Create a document instance of the appropriate type
     *
     * @param string $collection Collection ID
     * @param array<string, mixed> $data Document data
     * @return Document
     */
    protected function createDocumentInstance(string $collection, array $data): Document
    {
        $className = $this->documentTypes[$collection] ?? Document::class;

        return new $className($data);
    }

    public function getPreserveDates(): bool
    {
        return $this->preserveDates;
    }

    public function setPreserveDates(bool $preserve): static
    {
        $this->preserveDates = $preserve;

        return $this;
    }

    public function setMigrating(bool $migrating): self
    {
        $this->migrating = $migrating;

        return $this;
    }

    public function isMigrating(): bool
    {
        return $this->migrating;
    }

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

    public function getPreserveSequence(): bool
    {
        return $this->preserveSequence;
    }

    public function setPreserveSequence(bool $preserve): static
    {
        $this->preserveSequence = $preserve;

        return $this;
    }

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

    public function setMaxQueryValues(int $max): self
    {
        $this->maxQueryValues = $max;

        return $this;
    }

    public function getMaxQueryValues(): int
    {
        return $this->maxQueryValues;
    }

    /**
     * Set list of collections which are globally accessible
     *
     * @param array<string> $collections
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
     *
     * @return void
     */
    public function resetGlobalCollections(): void
    {
        $this->globalCollections = [];
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
     * Get Database Adapter
     *
     * @return Adapter
     */
    public function getAdapter(): Adapter
    {
        return $this->adapter;
    }
    /**
     * Ping Database
     *
     * @return bool
     */
    public function ping(): bool
    {
        return $this->adapter->ping();
    }

    public function reconnect(): void
    {
        $this->adapter->reconnect();
    }
    /**
     * Add Attribute Filter
     *
     * @param string $name
     * @param callable $encode
     * @param callable $decode
     *
     * @return void
     */
    public static function addFilter(string $name, callable $encode, callable $decode): void
    {
        self::$filters[$name] = [
            'encode' => $encode,
            'decode' => $decode,
        ];
    }

    /**
     * Encode Document
     *
     * @param Document $collection
     * @param Document $document
     * @param bool $applyDefaults Whether to apply default values to null attributes
     *
     * @return Document
     * @throws DatabaseException
     */
    public function encode(Document $collection, Document $document, bool $applyDefaults = true): Document
    {
        $attributes = $collection->getAttribute('attributes', []);
        $internalDateAttributes = ['$createdAt', '$updatedAt'];
        foreach ($this->getInternalAttributes() as $attribute) {
            $attributes[] = $attribute;
        }

        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? '';
            $array = $attribute['array'] ?? false;
            $default = $attribute['default'] ?? null;
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
            // False positive "Call to function is_null() with mixed will always evaluate to false"
            // @phpstan-ignore-next-line
            if (is_null($value) && !is_null($default)) {
                // Skip applying defaults during updates to avoid resetting unspecified attributes
                if (!$applyDefaults) {
                    continue;
                }
                $value = ($array) ? $default : [$default];
            } else {
                $value = ($array) ? $value : [$value];
            }

            foreach ($value as $index => $node) {
                if ($node !== null) {
                    foreach ($filters as $filter) {
                        $node = $this->encodeAttribute($filter, $node, $document);
                    }
                    $value[$index] = $node;
                }
            }

            if (!$array) {
                $value = $value[0];
            }
            $document->setAttribute($key, $value);
        }

        return $document;
    }

    /**
     * Decode Document
     *
     * @param Document $collection
     * @param Document $document
     * @param array<string> $selections
     * @return Document
     * @throws DatabaseException
     */
    public function decode(Document $collection, Document $document, array $selections = []): Document
    {
        $attributes = \array_filter(
            $collection->getAttribute('attributes', []),
            fn ($attribute) => $attribute['type'] !== ColumnType::Relationship->value
        );

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn ($attribute) => $attribute['type'] === ColumnType::Relationship->value
        );

        $filteredValue = [];

        foreach ($relationships as $relationship) {
            $key = $relationship['$id'] ?? '';

            if (
                \array_key_exists($key, (array)$document)
                || \array_key_exists($this->adapter->filter($key), (array)$document)
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
            $key = $attribute['$id'] ?? '';
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;
            $filters = $attribute['filters'] ?? [];
            $value = $document->getAttribute($key);

            if ($key === '$permissions') {
                continue;
            }

            if (\is_null($value)) {
                $value = $document->getAttribute($this->adapter->filter($key));

                if (!\is_null($value)) {
                    $document->removeAttribute($this->adapter->filter($key));
                }
            }

            // Skip decoding for Operator objects (shouldn't happen, but safety check)
            if ($value instanceof Operator) {
                continue;
            }

            $value = ($array) ? $value : [$value];
            $value = (is_null($value)) ? [] : $value;

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
        if (!empty($selections)) {
            foreach ($selections as $selection) {
                if (\str_contains($selection, '.')) {
                    $hasRelationshipSelections = true;
                    break;
                }
            }
        }

        if ($hasRelationshipSelections && !empty($selections) && !\in_array('*', $selections)) {
            foreach ($collection->getAttribute('attributes', []) as $attribute) {
                $key = $attribute['$id'] ?? '';

                if ($attribute['type'] === ColumnType::Relationship->value || $key === '$permissions') {
                    continue;
                }

                if (!in_array($key, $selections) && isset($filteredValue[$key])) {
                    $document->setAttribute($key, $filteredValue[$key]);
                }
            }
        }
        return $document;
    }

    /**
     * Casting
     *
     * @param Document $collection
     * @param Document $document
     *
     * @return Document
     */
    public function casting(Document $collection, Document $document): Document
    {
        if (!$this->adapter->supports(Capability::Casting)) {
            return $document;
        }

        $attributes = $collection->getAttribute('attributes', []);

        foreach ($this->getInternalAttributes() as $attribute) {
            $attributes[] = $attribute;
        }

        foreach ($attributes as $attribute) {
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
                $value = !is_string($value)
                    ? $value
                    : json_decode($value, true);
            } else {
                $value = [$value];
            }

            foreach ($value as $index => $node) {
                $node = match ($type) {
                    ColumnType::Id->value => (string)$node,
                    ColumnType::Boolean->value => (bool)$node,
                    ColumnType::Integer->value => (int)$node,
                    ColumnType::Double->value => (float)$node,
                    default => $node,
                };

                $value[$index] = $node;
            }

            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }

        return $document;
    }

    /**
     * Encode Attribute
     *
     * Passes the attribute $value, and $document context to a predefined filter
     * that allow you to manipulate the input format of the given attribute.
     *
     * @param string $name
     * @param mixed $value
     * @param Document $document
     *
     * @return mixed
     * @throws DatabaseException
     */
    protected function encodeAttribute(string $name, mixed $value, Document $document): mixed
    {
        if (!array_key_exists($name, self::$filters) && !array_key_exists($name, $this->instanceFilters)) {
            throw new NotFoundException("Filter: {$name} not found");
        }

        try {
            if (\array_key_exists($name, $this->instanceFilters)) {
                $value = $this->instanceFilters[$name]['encode']($value, $document, $this);
            } else {
                $value = self::$filters[$name]['encode']($value, $document, $this);
            }
        } catch (\Throwable $th) {
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
     * @param string $filter
     * @param mixed $value
     * @param Document $document
     * @param string $attribute
     * @return mixed
     * @throws NotFoundException
     */
    protected function decodeAttribute(string $filter, mixed $value, Document $document, string $attribute): mixed
    {
        if (!$this->filter) {
            return $value;
        }

        if (!\is_null($this->disabledFilters) && isset($this->disabledFilters[$filter])) {
            return $value;
        }

        if (!array_key_exists($filter, self::$filters) && !array_key_exists($filter, $this->instanceFilters)) {
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
     * Get adapter attribute limit, accounting for internal metadata
     * Returns 0 to indicate no limit
     *
     * @return int
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
     *
     * @return int
     */
    public function getLimitForIndexes(): int
    {
        return $this->adapter->getLimitForIndexes() - $this->adapter->getCountOfDefaultIndexes();
    }

    /**
     * @param Document $collection
     * @param array<Query> $queries
     * @return array<Query>
     * @throws QueryException
     * @throws \Utopia\Database\Exception
     */
    public function convertQueries(Document $collection, array $queries): array
    {
        foreach ($queries as $index => $query) {
            if ($query->isNested()) {
                $values = $this->convertQueries($collection, $query->getValues());
                $query->setValues($values);
            }

            $query = $this->convertQuery($collection, $query);

            $queries[$index] = $query;
        }

        return $queries;
    }

    /**
     * @param Document $collection
     * @param Query $query
     * @return Query
     * @throws QueryException
     * @throws \Utopia\Database\Exception
     */
    /**
     * Check if values are compatible with object attribute type (hashmap/multi-dimensional array)
     *
     * @param array<mixed> $values
     * @return bool
     */
    private function isCompatibleObjectValue(array $values): bool
    {
        if (empty($values)) {
            return false;
        }

        foreach ($values as $value) {
            if (!\is_array($value)) {
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

        if (!$attribute->isEmpty()) {
            $query->setOnArray($attribute->getAttribute('array', false));
            $query->setAttributeType($attribute->getAttribute('type'));

            if ($attribute->getAttribute('type') == ColumnType::Datetime->value) {
                $values = $query->getValues();
                foreach ($values as $valueIndex => $value) {
                    try {
                        $values[$valueIndex] = $this->adapter->supports(Capability::UTCCasting)
                            ? $this->adapter->setUTCDatetime($value)
                            : DateTime::setTimezone($value);
                    } catch (\Throwable $e) {
                        throw new QueryException($e->getMessage(), $e->getCode(), $e);
                    }
                }
                $query->setValues($values);
            }
        } elseif (!$this->adapter->supports(Capability::DefinedAttributes)) {
            $values = $query->getValues();
            // setting attribute type to properly apply filters in the adapter level
            if ($this->adapter->supports(Capability::Objects) && $this->isCompatibleObjectValue($values)) {
                $query->setAttributeType(ColumnType::Object->value);
            }
        }

        return $query;
    }

    /**
     * @return  array<array<string, mixed>>
     */
    public function getInternalAttributes(): array
    {
        $attributes = self::INTERNAL_ATTRIBUTES;

        if (!$this->adapter->getSharedTables()) {
            $attributes = \array_filter(Database::INTERNAL_ATTRIBUTES, function ($attribute) {
                return $attribute['$id'] !== '$tenant';
            });
        }

        return $attributes;
    }

    /**
     * Get Schema Attributes
     *
     * @param string $collection
     * @return array<Document>
     * @throws DatabaseException
     */
    public function getSchemaAttributes(string $collection): array
    {
        return $this->adapter->getSchemaAttributes($collection);
    }

    /**
     * @param string $collectionId
     * @param string|null $documentId
     * @param array<string> $selects
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

            if (!empty($selects)) {
                $documentHashKey = $documentKey . ':' . \md5(\implode($selects));
            }
        }

        return [
            $collectionKey,
            $documentKey ?? '',
            $documentHashKey ?? ''
        ];
    }
    /**
     * Encode spatial data from array format to WKT (Well-Known Text) format
     *
     * @param mixed $value
     * @param string $type
     * @return string
     * @throws DatabaseException
     */
    protected function encodeSpatialData(mixed $value, string $type): string
    {
        $validator = new SpatialValidator($type);
        if (!$validator->isValid($value)) {
            throw new StructureException($validator->getDescription());
        }

        switch ($type) {
            case ColumnType::Point->value:
                return "POINT({$value[0]} {$value[1]})";

            case ColumnType::Linestring->value:
                $points = [];
                foreach ($value as $point) {
                    $points[] = "{$point[0]} {$point[1]}";
                }
                return 'LINESTRING(' . implode(', ', $points) . ')';

            case ColumnType::Polygon->value:
                // Check if this is a single ring (flat array of points) or multiple rings
                $isSingleRing = count($value) > 0 && is_array($value[0]) &&
                    count($value[0]) === 2 && is_numeric($value[0][0]) && is_numeric($value[0][1]);

                if ($isSingleRing) {
                    // Convert single ring format [[x1,y1], [x2,y2], ...] to multi-ring format
                    $value = [$value];
                }

                $rings = [];
                foreach ($value as $ring) {
                    $points = [];
                    foreach ($ring as $point) {
                        $points[] = "{$point[0]} {$point[1]}";
                    }
                    $rings[] = '(' . implode(', ', $points) . ')';
                }
                return 'POLYGON(' . implode(', ', $rings) . ')';

            default:
                throw new DatabaseException('Unknown spatial type: ' . $type);
        }
    }

    /**
     * Retry a callable with exponential backoff
     *
     * @param callable $operation The operation to retry
     * @param int $maxAttempts Maximum number of retry attempts
     * @param int $initialDelayMs Initial delay in milliseconds
     * @param float $multiplier Backoff multiplier
     * @return void The result of the operation
     * @throws \Throwable The last exception if all retries fail
     */
    private function withRetries(
        callable $operation,
        int $maxAttempts = 3,
        int $initialDelayMs = 100,
        float $multiplier = 2.0
    ): void {
        $attempt = 0;
        $delayMs = $initialDelayMs;
        $lastException = null;

        while ($attempt < $maxAttempts) {
            try {
                $operation();
                return;
            } catch (\Throwable $e) {
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

                $delayMs = (int)($delayMs * $multiplier);
            }
        }

        throw $lastException;
    }

    /**
     * Generic cleanup operation with retry logic
     *
     * @param callable $operation The cleanup operation to execute
     * @param string $resourceType Type of resource being cleaned up (e.g., 'attribute', 'index')
     * @param string $resourceId ID of the resource being cleaned up
     * @param int $maxAttempts Maximum retry attempts
     * @return void
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
        } catch (\Throwable $e) {
            Console::error("Failed to cleanup {$resourceType} '{$resourceId}' after {$maxAttempts} attempts: " . $e->getMessage());
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
     * @param Document $collection The collection document to persist
     * @param callable|null $rollbackOperation Cleanup operation to run if persistence fails (null if no cleanup needed)
     * @param bool $shouldRollback Whether rollback should be attempted (e.g., false for duplicates in shared tables)
     * @param string $operationDescription Description of the operation for error messages
     * @param bool $rollbackReturnsErrors Whether rollback operation returns error array (true) or throws (false)
     * @param bool $silentRollback Whether rollback errors should be silently caught (true) or thrown (false)
     * @return void
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
        } catch (\Throwable $e) {
            // Attempt rollback only if conditions are met
            if ($shouldRollback && $rollbackOperation !== null) {
                if ($rollbackReturnsErrors) {
                    // Batch mode: rollback returns array of errors
                    $cleanupErrors = $rollbackOperation();
                    if (!empty($cleanupErrors)) {
                        throw new DatabaseException(
                            "Failed to persist metadata after retries and cleanup encountered errors for {$operationDescription}: " . $e->getMessage() . ' | Cleanup errors: ' . implode(', ', $cleanupErrors),
                            previous: $e
                        );
                    }
                } elseif ($silentRollback) {
                    // Silent mode: swallow rollback errors
                    try {
                        $rollbackOperation();
                    } catch (\Throwable $e) {
                        // Silent rollback - errors are swallowed
                    }
                } else {
                    // Regular mode: rollback throws on failure
                    try {
                        $rollbackOperation();
                    } catch (\Throwable $ex) {
                        throw new DatabaseException(
                            "Failed to persist metadata after retries and cleanup failed for {$operationDescription}: " . $ex->getMessage() . ' | Cleanup error: ' . $e->getMessage(),
                            previous: $e
                        );
                    }
                }
            }

            throw new DatabaseException(
                "Failed to persist metadata after retries for {$operationDescription}: " . $e->getMessage(),
                previous: $e
            );
        }
    }
}

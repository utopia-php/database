<?php

namespace Utopia\Database;

use Exception;
use Utopia\Cache\Cache;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Queries\Document as DocumentValidator;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;
use Utopia\Database\Validator\Index as IndexValidator;
use Utopia\Database\Validator\Permissions;
use Utopia\Database\Validator\Structure;

class Database
{
    public const VAR_STRING = 'string';
    // Simple Types
    public const VAR_INTEGER = 'integer';
    public const VAR_FLOAT = 'double';
    public const VAR_BOOLEAN = 'boolean';
    public const VAR_DATETIME = 'datetime';

    // Relationship Types
    public const VAR_RELATIONSHIP = 'relationship';

    // Index Types
    public const INDEX_KEY = 'key';
    public const INDEX_FULLTEXT = 'fulltext';
    public const INDEX_UNIQUE = 'unique';
    public const INDEX_SPATIAL = 'spatial';
    public const ARRAY_INDEX_LENGTH = 255;

    // Relation Types
    public const RELATION_ONE_TO_ONE = 'oneToOne';
    public const RELATION_ONE_TO_MANY = 'oneToMany';
    public const RELATION_MANY_TO_ONE = 'manyToOne';
    public const RELATION_MANY_TO_MANY = 'manyToMany';

    // Relation Actions
    public const RELATION_MUTATE_CASCADE = 'cascade';
    public const RELATION_MUTATE_RESTRICT = 'restrict';
    public const RELATION_MUTATE_SET_NULL = 'setNull';

    // Relation Sides
    public const RELATION_SIDE_PARENT = 'parent';
    public const RELATION_SIDE_CHILD = 'child';

    public const RELATION_MAX_DEPTH = 3;

    // Orders
    public const ORDER_ASC = 'ASC';
    public const ORDER_DESC = 'DESC';

    // Permissions
    public const PERMISSION_CREATE = 'create';
    public const PERMISSION_READ = 'read';
    public const PERMISSION_UPDATE = 'update';
    public const PERMISSION_DELETE = 'delete';

    // Aggregate permissions
    public const PERMISSION_WRITE = 'write';

    public const PERMISSIONS = [
        self::PERMISSION_CREATE,
        self::PERMISSION_READ,
        self::PERMISSION_UPDATE,
        self::PERMISSION_DELETE,
    ];

    // Collections
    public const METADATA = '_metadata';

    // Cursor
    public const CURSOR_BEFORE = 'before';
    public const CURSOR_AFTER = 'after';

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
    public const EVENT_DOCUMENT_CREATE = 'document_create';
    public const EVENT_DOCUMENTS_CREATE = 'documents_create';
    public const EVENT_DOCUMENT_READ = 'document_read';
    public const EVENT_DOCUMENT_UPDATE = 'document_update';
    public const EVENT_DOCUMENTS_UPDATE = 'documents_update';
    public const EVENT_DOCUMENT_DELETE = 'document_delete';
    public const EVENT_DOCUMENT_COUNT = 'document_count';
    public const EVENT_DOCUMENT_SUM = 'document_sum';
    public const EVENT_DOCUMENT_INCREASE = 'document_increase';
    public const EVENT_DOCUMENT_DECREASE = 'document_decrease';

    public const EVENT_PERMISSIONS_CREATE = 'permissions_create';
    public const EVENT_PERMISSIONS_READ = 'permissions_read';
    public const EVENT_PERMISSIONS_DELETE = 'permissions_delete';

    public const EVENT_ATTRIBUTE_CREATE = 'attribute_create';
    public const EVENT_ATTRIBUTE_UPDATE = 'attribute_update';
    public const EVENT_ATTRIBUTE_DELETE = 'attribute_delete';

    public const EVENT_INDEX_RENAME = 'index_rename';
    public const EVENT_INDEX_CREATE = 'index_create';
    public const EVENT_INDEX_DELETE = 'index_delete';

    public const INSERT_BATCH_SIZE = 100;

    protected Adapter $adapter;

    protected Cache $cache;

    protected string $cacheName = 'default';

    /**
     * @var array<bool|string>
     */
    protected array $map = [];

    /**
     * List of Internal attributes
     *
     * @var array<array<string, mixed>>
     */
    public const INTERNAL_ATTRIBUTES = [
        [
            '$id' => '$id',
            'type' => self::VAR_STRING,
            'size' => Database::LENGTH_KEY,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$internalId',
            'type' => self::VAR_STRING,
            'size' => Database::LENGTH_KEY,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$collection',
            'type' => self::VAR_STRING,
            'size' => Database::LENGTH_KEY,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$tenant',
            'type' => self::VAR_STRING,
            'size' => 36,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$createdAt',
            'type' => Database::VAR_DATETIME,
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
            'type' => Database::VAR_DATETIME,
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
            'type' => Database::VAR_STRING,
            'size' => 1000000,
            'signed' => true,
            'required' => false,
            'default' => [],
            'array' => false,
            'filters' => ['json']
        ],
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
    public const COLLECTION = [
        '$id' => self::METADATA,
        '$collection' => self::METADATA,
        'name' => 'collections',
        'attributes' => [
            [
                '$id' => 'name',
                'key' => 'name',
                'type' => self::VAR_STRING,
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'attributes',
                'key' => 'attributes',
                'type' => self::VAR_STRING,
                'size' => 1000000,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => ['json'],
            ],
            [
                '$id' => 'indexes',
                'key' => 'indexes',
                'type' => self::VAR_STRING,
                'size' => 1000000,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => ['json'],
            ],
            [
                '$id' => 'documentSecurity',
                'key' => 'documentSecurity',
                'type' => self::VAR_BOOLEAN,
                'size' => 0,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => []
            ]
        ],
        'indexes' => [],
    ];

    /**
     * @var array<string, array{encode: callable, decode: callable}>
     */
    protected static array $filters = [];

    /**
     * @var array<string, array{encode: callable, decode: callable}>
     */
    protected array $instanceFilters = [];

    /**
     * @var array<string, mixed>
     */
    protected array $listeners = [
        '*' => [],
    ];

    /**
     * Array in which the keys are the names of databse listeners that
     * should be skipped when dispatching events. null $silentListeners
     * will skip all listeners.
     *
     * @var ?array<string, bool>
     */
    protected ?array $silentListeners = [];

    protected ?\DateTime $timestamp = null;

    protected bool $resolveRelationships = true;

    protected int $relationshipFetchDepth = 1;

    protected bool $filter = true;

    protected bool $validate = true;

    protected bool $preserveDates = false;

    /**
     * Stack of collection IDs when creating or updating related documents
     * @var array<string>
     */
    protected array $relationshipWriteStack = [];

    /**
     * @var array<Document>
     */
    protected array $relationshipFetchStack = [];

    /**
     * @var array<Document>
     */
    protected array $relationshipDeleteStack = [];

    /**
     * @param Adapter $adapter
     * @param Cache $cache
     * @param array<string, array{encode: callable, decode: callable}> $filters
     */
    public function __construct(Adapter $adapter, Cache $cache, array $filters = [])
    {
        $this->adapter = $adapter;
        $this->cache = $cache;
        $this->instanceFilters = $filters;

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
             * @param string|null $value
             * @return string|null
             * @throws Exception
             */
            function (?string $value) {
                if (is_null($value)) {
                    return null;
                }
                try {
                    $value = new \DateTime($value);
                    $value->setTimezone(new \DateTimeZone(date_default_timezone_get()));
                    return DateTime::format($value);
                } catch (\Throwable $th) {
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
    }

    /**
     * Add listener to events
     *
     * @param string $event
     * @param string $name
     * @param callable $callback
     * @return self
     */
    public function on(string $event, string $name, callable $callback): self
    {
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
    public function before(string $event, string $name, callable $callback): self
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
    public function silent(callable $callback, array $listeners = null): mixed
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
     * Skip relationships for all the calls inside the callback
     *
     * @template T
     * @param callable(): T $callback
     * @return T
     */
    public function skipRelationships(callable $callback): mixed
    {
        $previous = $this->resolveRelationships;
        $this->resolveRelationships = false;

        try {
            return $callback();
        } finally {
            $this->resolveRelationships = $previous;
        }
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
    public function setNamespace(string $namespace): self
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
     * @return self
     * @throws DatabaseException
     */
    public function setDatabase(string $name): self
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
    public function setCache(Cache $cache): self
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
    public function setCacheName(string $name): self
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
     * @return self
     */
    public function setMetadata(string $key, mixed $value): self
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
     * @return self
     * @throws Exception
     */
    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): self
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
    public function enableFilters(): self
    {
        $this->filter = true;

        return $this;
    }

    /**
     * Disable filters
     *
     * @return $this
     */
    public function disableFilters(): self
    {
        $this->filter = false;

        return $this;
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
    public function enableValidation(): self
    {
        $this->validate = true;

        return $this;
    }

    /**
     * Disable validation
     *
     * @return $this
     */
    public function disableValidation(): self
    {
        $this->validate = false;

        return $this;
    }

    /**
     * Set Share Tables
     *
     * Set whether to share tables between tenants
     *
     * @param bool $share
     * @return self
     */
    public function setShareTables(bool $share): self
    {
        $this->adapter->setShareTables($share);

        return $this;
    }

    /**
     * Set Tenant
     *
     * Set tenant to use if tables are shared
     *
     * @param ?int $tenant
     * @return self
     */
    public function setTenant(?int $tenant): self
    {
        $this->adapter->setTenant($tenant);

        return $this;
    }

    public function setPreserveDates(bool $preserve): self
    {
        $this->preserveDates = $preserve;

        return $this;
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

    /**
     * Create the database
     *
     * @throws DatabaseException
     *
     * @return bool
     */
    public function create(?string $database = null): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $database = $database ?? $this->adapter->getDatabase();
        $this->adapter->create($database);

        /**
         * Create array of attribute documents
         * @var array<Document> $attributes
         */
        $attributes = array_map(function ($attribute) {
            return new Document($attribute);
        }, self::COLLECTION['attributes']);

        $this->silent(fn () => $this->createCollection(self::METADATA, $attributes));

        $this->trigger(self::EVENT_DATABASE_CREATE, $database);

        return true;
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string|null $database (optional) database name
     * @param string|null $collection (optional) collection name
     *
     * @return bool
     */
    public function exists(?string $database = null, ?string $collection = null): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $database = $database ?? $this->adapter->getDatabase();

        return $this->adapter->exists($database, $collection);
    }

    /**
     * List Databases
     *
     * @return array<Document>
     */
    public function list(): array
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $databases = $this->adapter->list();

        $this->trigger(self::EVENT_DATABASE_LIST, $databases);

        return $databases;
    }

    /**
     * Delete Database
     *
     * @param string|null $database
     * @return bool
     */
    public function delete(?string $database = null): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $database = $database ?? $this->adapter->getDatabase();

        $deleted = $this->adapter->delete($database);

        $this->trigger(self::EVENT_DATABASE_DELETE, [
            'name' => $database,
            'deleted' => $deleted
        ]);

        return $deleted;
    }

    /**
     * Create Collection
     *
     * @param string $id
     * @param array<Document> $attributes
     * @param array<Document> $indexes
     * @param array<string> $permissions
     * @param bool $documentSecurity
     * @return Document
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     */
    public function createCollection(string $id, array $attributes = [], array $indexes = [], array $permissions = null, bool $documentSecurity = true): Document
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $permissions ??= [
            Permission::create(Role::any()),
        ];

        if ($this->validate) {
            $validator = new Permissions();
            if (!$validator->isValid($permissions)) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        $collection = $this->silent(fn () => $this->getCollection($id));

        if (!$collection->isEmpty() && $id !== self::METADATA) {
            throw new DuplicateException('Collection ' . $id . ' already exists');
        }

        $collection = new Document([
            '$id' => ID::custom($id),
            '$permissions' => $permissions,
            'name' => $id,
            'attributes' => $attributes,
            'indexes' => $indexes,
            'documentSecurity' => $documentSecurity
        ]);

        if ($this->validate) {
            $validator = new IndexValidator(
                $attributes,
                $this->adapter->getMaxIndexLength()
            );
            foreach ($indexes as $index) {
                if (!$validator->isValid($index)) {
                    throw new DatabaseException($validator->getDescription());
                }
            }
        }

        $this->adapter->createCollection($id, $attributes, $indexes);

        if ($id === self::METADATA) {
            return new Document(self::COLLECTION);
        }

        // Check index limits, if given
        if ($indexes && $this->adapter->getCountOfIndexes($collection) > $this->adapter->getLimitForIndexes()) {
            throw new LimitException('Index limit of ' . $this->adapter->getLimitForIndexes() . ' exceeded. Cannot create collection.');
        }

        // Check attribute limits, if given
        if ($attributes) {
            if (
                $this->adapter->getLimitForAttributes() > 0 &&
                $this->adapter->getCountOfAttributes($collection) > $this->adapter->getLimitForAttributes()
            ) {
                throw new LimitException('Column limit of ' . $this->adapter->getLimitForAttributes() . ' exceeded. Cannot create collection.');
            }

            if (
                $this->adapter->getDocumentSizeLimit() > 0 &&
                $this->adapter->getAttributeWidth($collection) > $this->adapter->getDocumentSizeLimit()
            ) {
                throw new LimitException('Row width limit of ' . $this->adapter->getDocumentSizeLimit() . ' exceeded. Cannot create collection.');
            }
        }

        $createdCollection = $this->silent(fn () => $this->createDocument(self::METADATA, $collection));

        $this->trigger(self::EVENT_COLLECTION_CREATE, $createdCollection);

        return $createdCollection;
    }

    /**
     * Update Collections Permissions.
     *
     * @param string $id
     * @param array<string> $permissions
     * @param bool $documentSecurity
     *
     * @return Document
     * @throws ConflictException
     * @throws DatabaseException
     */
    public function updateCollection(string $id, array $permissions, bool $documentSecurity): Document
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if ($this->validate) {
            $validator = new Permissions();
            if (!$validator->isValid($permissions)) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        $collection = $this->silent(fn () => $this->getCollection($id));

        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        if ($this->adapter->getShareTables()
            && $collection->getAttribute('$tenant') != $this->adapter->getTenant()) {
            throw new DatabaseException('Collection not found');
        }

        $collection
            ->setAttribute('$permissions', $permissions)
            ->setAttribute('documentSecurity', $documentSecurity);

        $collection = $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));

        $this->trigger(self::EVENT_COLLECTION_UPDATE, $collection);

        return $collection;
    }

    /**
     * Get Collection
     *
     * @param string $id
     *
     * @return Document
     * @throws DatabaseException
     */
    public function getCollection(string $id): Document
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getDocument(self::METADATA, $id));

        if ($id !== self::METADATA
            && $this->adapter->getShareTables()
            && $collection->getAttribute('$tenant') != $this->adapter->getTenant()) {
            return new Document();
        }

        $this->trigger(self::EVENT_COLLECTION_READ, $collection);

        return $collection;
    }

    /**
     * List Collections
     *
     * @param int $offset
     * @param int $limit
     *
     * @return array<Document>
     * @throws Exception
     */
    public function listCollections(int $limit = 25, int $offset = 0): array
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $result = $this->silent(fn () => $this->find(self::METADATA, [
            Query::limit($limit),
            Query::offset($offset)
        ]));

        if ($this->adapter->getShareTables()) {
            $result = \array_filter($result, function ($collection) {
                return $collection->getAttribute('$tenant') === $this->adapter->getTenant();
            });
        }

        $this->trigger(self::EVENT_COLLECTION_LIST, $result);

        return $result;
    }

    /**
     * Get Collection Size
     *
     * @param string $collection
     *
     * @return int
     */
    public function getSizeOfCollection(string $collection): int
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        if ($this->adapter->getShareTables() && $collection->getAttribute('$tenant') != $this->adapter->getTenant()) {
            throw new DatabaseException('Collection not found');
        }

        return $this->adapter->getSizeOfCollection($collection->getId());
    }

    /**
     * Delete Collection
     *
     * @param string $id
     *
     * @return bool
     */
    public function deleteCollection(string $id): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getDocument(self::METADATA, $id));

        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        if ($this->adapter->getShareTables() && $collection->getAttribute('$tenant') != $this->adapter->getTenant()) {
            throw new DatabaseException('Collection not found');
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes'),
            fn ($attribute) =>
            $attribute->getAttribute('type') === Database::VAR_RELATIONSHIP
        );

        foreach ($relationships as $relationship) {
            $this->deleteRelationship($collection->getId(), $relationship->getId());
        }

        $this->adapter->deleteCollection($id);

        $deleted = $this->silent(fn () => $this->deleteDocument(self::METADATA, $id));

        $this->trigger(self::EVENT_COLLECTION_DELETE, $collection);

        return $deleted;
    }

    /**
     * Create Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size utf8mb4 chars length
     * @param bool $required
     * @param mixed $default
     * @param bool $signed
     * @param bool $array
     * @param string|null $format optional validation format of attribute
     * @param array<string, mixed> $formatOptions assoc array with custom options that can be passed for the format validation
     * @param array<string> $filters
     *
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $required, mixed $default = null, bool $signed = true, bool $array = false, string $format = null, array $formatOptions = [], array $filters = []): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        if ($this->adapter->getShareTables() && $collection->getAttribute('$tenant') != $this->adapter->getTenant()) {
            throw new DatabaseException('Collection not found');
        }

        // attribute IDs are case insensitive
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $attributes */
        foreach ($attributes as $attribute) {
            if (\strtolower($attribute->getId()) === \strtolower($id)) {
                throw new DuplicateException('Attribute already exists');
            }
        }

        /** Ensure required filters for the attribute are passed */
        $requiredFilters = $this->getRequiredFilters($type);
        if (!empty(array_diff($requiredFilters, $filters))) {
            throw new DatabaseException("Attribute of type: $type requires the following filters: " . implode(",", $requiredFilters));
        }

        if (
            $this->adapter->getLimitForAttributes() > 0 &&
            $this->adapter->getCountOfAttributes($collection) >= $this->adapter->getLimitForAttributes()
        ) {
            throw new LimitException('Column limit reached. Cannot create new attribute.');
        }

        if ($format) {
            if (!Structure::hasFormat($format, $type)) {
                throw new DatabaseException('Format ("' . $format . '") not available for this attribute type ("' . $type . '")');
            }
        }

        $attribute = new Document([
            '$id' => ID::custom($id),
            'key' => $id,
            'type' => $type,
            'size' => $size,
            'required' => $required,
            'default' => $default,
            'signed' => $signed,
            'array' => $array,
            'format' => $format,
            'formatOptions' => $formatOptions,
            'filters' => $filters,
        ]);

        $collection->setAttribute('attributes', $attribute, Document::SET_TYPE_APPEND);

        if (
            $this->adapter->getDocumentSizeLimit() > 0 &&
            $this->adapter->getAttributeWidth($collection) >= $this->adapter->getDocumentSizeLimit()
        ) {
            throw new LimitException('Row width limit reached. Cannot create new attribute.');
        }

        switch ($type) {
            case self::VAR_STRING:
                if ($size > $this->adapter->getLimitForString()) {
                    throw new DatabaseException('Max size allowed for string is: ' . number_format($this->adapter->getLimitForString()));
                }
                break;

            case self::VAR_INTEGER:
                $limit = ($signed) ? $this->adapter->getLimitForInt() / 2 : $this->adapter->getLimitForInt();
                if ($size > $limit) {
                    throw new DatabaseException('Max size allowed for int is: ' . number_format($limit));
                }
                break;
            case self::VAR_FLOAT:
            case self::VAR_BOOLEAN:
            case self::VAR_DATETIME:
            case self::VAR_RELATIONSHIP:
                break;
            default:
                throw new DatabaseException('Unknown attribute type: ' . $type . '. Must be one of ' . self::VAR_STRING . ', ' . self::VAR_INTEGER .  ', ' . self::VAR_FLOAT . ', ' . self::VAR_BOOLEAN . ', ' . self::VAR_DATETIME . ', ' . self::VAR_RELATIONSHIP);
        }

        // only execute when $default is given
        if (!\is_null($default)) {
            if ($required === true) {
                throw new DatabaseException('Cannot set a default value on a required attribute');
            }

            $this->validateDefaultTypes($type, $default);
        }

        $created = $this->adapter->createAttribute($collection->getId(), $id, $type, $size, $signed, $array);

        if (!$created) {
            throw new DatabaseException('Failed to create attribute');
        }

        if ($collection->getId() !== self::METADATA) {
            $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));
        }

        $this->trigger(self::EVENT_ATTRIBUTE_CREATE, $attribute);

        return true;
    }

    /**
     * Get the list of required filters for each data type
     *
     * @param string|null $type Type of the attribute
     *
     * @return array<string>
     */
    protected function getRequiredFilters(?string $type): array
    {
        return match ($type) {
            self::VAR_DATETIME => ['datetime'],
            default => [],
        };
    }

    /**
     * Function to validate if the default value of an attribute matches its attribute type
     *
     * @param string $type Type of the attribute
     * @param mixed $default Default value of the attribute
     *
     * @throws Exception
     * @return void
     */
    protected function validateDefaultTypes(string $type, mixed $default): void
    {
        $defaultType = \gettype($default);

        if ($defaultType === 'NULL') {
            // Disable null. No validation required
            return;
        }

        if ($defaultType === 'array') {
            foreach ($default as $value) {
                $this->validateDefaultTypes($type, $value);
            }
            return;
        }

        switch ($type) {
            case self::VAR_STRING:
            case self::VAR_INTEGER:
            case self::VAR_FLOAT:
            case self::VAR_BOOLEAN:
                if ($type !== $defaultType) {
                    throw new DatabaseException('Default value ' . $default . ' does not match given type ' . $type);
                }
                break;
            case self::VAR_DATETIME:
                if ($defaultType !== self::VAR_STRING) {
                    throw new DatabaseException('Default value ' . $default . ' does not match given type ' . $type);
                }
                break;
            default:
                throw new DatabaseException('Unknown attribute type: ' . $type . '. Must be one of ' . self::VAR_STRING . ', ' . self::VAR_INTEGER .  ', ' . self::VAR_FLOAT . ', ' . self::VAR_BOOLEAN . ', ' . self::VAR_DATETIME . ', ' . self::VAR_RELATIONSHIP);
        }
    }

    /**
     * Update attribute metadata. Utility method for update attribute methods.
     *
     * @param string $collection
     * @param string $id
     * @param callable $updateCallback method that receives document, and returns it with changes applied
     *
     * @return Document
     * @throws ConflictException
     * @throws DatabaseException
     */
    protected function updateIndexMeta(string $collection, string $id, callable $updateCallback): Document
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() === self::METADATA) {
            throw new DatabaseException('Cannot update metadata indexes');
        }

        $indexes = $collection->getAttribute('indexes', []);
        $index = \array_search($id, \array_map(fn ($index) => $index['$id'], $indexes));

        if ($index === false) {
            throw new DatabaseException('Index not found');
        }

        // Execute update from callback
        $updateCallback($indexes[$index], $collection, $index);

        // Save
        $collection->setAttribute('indexes', $indexes);

        $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));

        $this->trigger(self::EVENT_ATTRIBUTE_UPDATE, $indexes[$index]);

        return $indexes[$index];
    }

    /**
     * Update attribute metadata. Utility method for update attribute methods.
     *
     * @param string $collection
     * @param string $id
     * @param callable $updateCallback method that receives document, and returns it with changes applied
     *
     * @return Document
     * @throws ConflictException
     * @throws DatabaseException
     */
    protected function updateAttributeMeta(string $collection, string $id, callable $updateCallback): Document
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() === self::METADATA) {
            throw new DatabaseException('Cannot update metadata attributes');
        }

        $attributes = $collection->getAttribute('attributes', []);
        $index = \array_search($id, \array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($index === false) {
            throw new DatabaseException('Attribute not found');
        }

        // Execute update from callback
        $updateCallback($attributes[$index], $collection, $index);

        // Save
        $collection->setAttribute('attributes', $attributes, Document::SET_TYPE_ASSIGN);

        $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));

        $this->trigger(self::EVENT_ATTRIBUTE_UPDATE, $attributes[$index]);

        return $attributes[$index];
    }

    /**
     * Update required status of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param bool $required
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeRequired(string $collection, string $id, bool $required): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($required) {
            $attribute->setAttribute('required', $required);
        });
    }

    /**
     * Update format of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param string $format validation format of attribute
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeFormat(string $collection, string $id, string $format): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($format) {
            if (!Structure::hasFormat($format, $attribute->getAttribute('type'))) {
                throw new DatabaseException('Format "' . $format . '" not available for attribute type "' . $attribute->getAttribute('type') . '"');
            }

            $attribute->setAttribute('format', $format);
        });
    }

    /**
     * Update format options of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param array<string, mixed> $formatOptions assoc array with custom options that can be passed for the format validation
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeFormatOptions(string $collection, string $id, array $formatOptions): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($formatOptions) {
            $attribute->setAttribute('formatOptions', $formatOptions);
        });
    }

    /**
     * Update filters of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param array<string> $filters
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeFilters(string $collection, string $id, array $filters): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($filters) {
            $attribute->setAttribute('filters', $filters);
        });
    }

    /**
     * Update default value of attribute
     *
     * @param string $collection
     * @param string $id
     * @param mixed $default
     *
     * @return Document
     * @throws Exception
     */
    public function updateAttributeDefault(string $collection, string $id, mixed $default = null): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute) use ($default) {
            if ($attribute->getAttribute('required') === true) {
                throw new DatabaseException('Cannot set a default value on a required attribute');
            }

            $this->validateDefaultTypes($attribute->getAttribute('type'), $default);

            $attribute->setAttribute('default', $default);
        });
    }

    /**
     * Update Attribute. This method is for updating data that causes underlying structure to change. Check out other updateAttribute methods if you are looking for metadata adjustments.
     * To update attribute key (ID), use renameAttribute instead.
     * @param string $collection
     * @param string $id
     * @param string|null $type
     * @param int|null $size utf8mb4 chars length
     * @param bool|null $required
     * @param mixed $default
     * @param bool $signed
     * @param bool $array
     * @param string|null $format
     * @param array<string, mixed>|null $formatOptions
     * @param array<string>|null $filters
     * @return Document
     * @throws Exception
     */
    public function updateAttribute(string $collection, string $id, string $type = null, int $size = null, bool $required = null, mixed $default = null, bool $signed = null, bool $array = null, string $format = null, ?array $formatOptions = null, ?array $filters = null): Document
    {
        return $this->updateAttributeMeta($collection, $id, function ($attribute, $collectionDoc, $attributeIndex) use ($collection, $id, $type, $size, $required, $default, $signed, $array, $format, $formatOptions, $filters) {
            $altering = !\is_null($type)
                || !\is_null($size)
                || !\is_null($signed)
                || !\is_null($array);
            $type ??= $attribute->getAttribute('type');
            $size ??= $attribute->getAttribute('size');
            $signed ??= $attribute->getAttribute('signed');
            $required ??= $attribute->getAttribute('required');
            $default ??= $attribute->getAttribute('default');
            $array ??= $attribute->getAttribute('array');
            $format ??= $attribute->getAttribute('format');
            $formatOptions ??= $attribute->getAttribute('formatOptions');
            $filters ??= $attribute->getAttribute('filters');

            if ($required === true && !\is_null($default)) {
                $default = null;
            }

            switch ($type) {
                case self::VAR_STRING:
                    if (empty($size)) {
                        throw new DatabaseException('Size length is required');
                    }

                    if ($size > $this->adapter->getLimitForString()) {
                        throw new DatabaseException('Max size allowed for string is: ' . number_format($this->adapter->getLimitForString()));
                    }
                    break;

                case self::VAR_INTEGER:
                    $limit = ($signed) ? $this->adapter->getLimitForInt() / 2 : $this->adapter->getLimitForInt();
                    if ($size > $limit) {
                        throw new DatabaseException('Max size allowed for int is: ' . number_format($limit));
                    }
                    break;
                case self::VAR_FLOAT:
                case self::VAR_BOOLEAN:
                case self::VAR_DATETIME:
                    if (!empty($size)) {
                        throw new DatabaseException('Size must be empty');
                    }
                    break;
                default:
                    throw new DatabaseException('Unknown attribute type: ' . $type . '. Must be one of ' . self::VAR_STRING . ', ' . self::VAR_INTEGER .  ', ' . self::VAR_FLOAT . ', ' . self::VAR_BOOLEAN . ', ' . self::VAR_DATETIME . ', ' . self::VAR_RELATIONSHIP);
            }

            /** Ensure required filters for the attribute are passed */
            $requiredFilters = $this->getRequiredFilters($type);
            if (!empty(array_diff($requiredFilters, $filters))) {
                throw new DatabaseException("Attribute of type: $type requires the following filters: " . implode(",", $requiredFilters));
            }

            if ($format) {
                if (!Structure::hasFormat($format, $type)) {
                    throw new DatabaseException('Format ("' . $format . '") not available for this attribute type ("' . $type . '")');
                }
            }

            if (!\is_null($default)) {
                if ($required) {
                    throw new DatabaseException('Cannot set a default value on a required attribute');
                }

                $this->validateDefaultTypes($type, $default);
            }

            $attribute
                ->setAttribute('type', $type)
                ->setAttribute('size', $size)
                ->setAttribute('signed', $signed)
                ->setAttribute('array', $array)
                ->setAttribute('format', $format)
                ->setAttribute('formatOptions', $formatOptions)
                ->setAttribute('filters', $filters)
                ->setAttribute('required', $required)
                ->setAttribute('default', $default);

            $attributes = $collectionDoc->getAttribute('attributes');
            $attributes[$attributeIndex] = $attribute;
            $collectionDoc->setAttribute('attributes', $attributes, Document::SET_TYPE_ASSIGN);

            if (
                $this->adapter->getDocumentSizeLimit() > 0 &&
                $this->adapter->getAttributeWidth($collectionDoc) >= $this->adapter->getDocumentSizeLimit()
            ) {
                throw new LimitException('Row width limit reached. Cannot create new attribute.');
            }

            if ($altering) {
                $updated = $this->adapter->updateAttribute($collection, $id, $type, $size, $signed, $array);

                if (!$updated) {
                    throw new DatabaseException('Failed to update attribute');
                }

                $this->purgeCachedCollection($collection);
            }

            $this->purgeCachedDocument(self::METADATA, $collection);
        });
    }

    /**
     * Checks if attribute can be added to collection.
     * Used to check attribute limits without asking the database
     * Returns true if attribute can be added to collection, throws exception otherwise
     *
     * @param Document $collection
     * @param Document $attribute
     *
     * @throws LimitException
     * @return bool
     */
    public function checkAttribute(Document $collection, Document $attribute): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = clone $collection;

        $collection->setAttribute('attributes', $attribute, Document::SET_TYPE_APPEND);

        if (
            $this->adapter->getLimitForAttributes() > 0 &&
            $this->adapter->getCountOfAttributes($collection) > $this->adapter->getLimitForAttributes()
        ) {
            throw new LimitException('Column limit reached. Cannot create new attribute.');
        }

        if (
            $this->adapter->getDocumentSizeLimit() > 0 &&
            $this->adapter->getAttributeWidth($collection) >= $this->adapter->getDocumentSizeLimit()
        ) {
            throw new LimitException('Row width limit reached. Cannot create new attribute.');
        }

        return true;
    }

    /**
     * Delete Attribute
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     * @throws ConflictException
     * @throws DatabaseException
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $attribute = null;

        foreach ($attributes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $id) {
                $attribute = $value;
                unset($attributes[$key]);
                break;
            }
        }

        if (\is_null($attribute)) {
            throw new DatabaseException('Attribute not found');
        }

        if ($attribute['type'] === self::VAR_RELATIONSHIP) {
            throw new DatabaseException('Cannot delete relationship as an attribute');
        }

        foreach ($indexes as $indexKey => $index) {
            $indexAttributes = $index->getAttribute('attributes', []);

            $indexAttributes = \array_filter($indexAttributes, fn ($attribute) => $attribute !== $id);

            if (empty($indexAttributes)) {
                unset($indexes[$indexKey]);
            } else {
                $index->setAttribute('attributes', \array_values($indexAttributes));
            }
        }

        $deleted = $this->adapter->deleteAttribute($collection->getId(), $id);

        if (!$deleted) {
            throw new DatabaseException('Failed to delete attribute');
        }

        $collection->setAttribute('attributes', \array_values($attributes));
        $collection->setAttribute('indexes', \array_values($indexes));

        if ($collection->getId() !== self::METADATA) {
            $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));
        }

        $this->trigger(self::EVENT_ATTRIBUTE_DELETE, $attribute);

        return true;
    }

    /**
     * Rename Attribute
     *
     * @param string $collection
     * @param string $old Current attribute ID
     * @param string $new
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $attribute = \in_array($old, \array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($attribute === false) {
            throw new DatabaseException('Attribute not found');
        }

        $attributeNew = \in_array($new, \array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($attributeNew !== false) {
            throw new DuplicateException('Attribute name already used');
        }

        foreach ($attributes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $old) {
                $attributes[$key]['key'] = $new;
                $attributes[$key]['$id'] = $new;
                $attributeNew = $attributes[$key];
                break;
            }
        }

        foreach ($indexes as $index) {
            $indexAttributes = $index->getAttribute('attributes', []);

            $indexAttributes = \array_map(fn ($attribute) => ($attribute === $old) ? $new : $attribute, $indexAttributes);

            $index->setAttribute('attributes', $indexAttributes);
        }

        $collection->setAttribute('attributes', $attributes);
        $collection->setAttribute('indexes', $indexes);

        if ($collection->getId() !== self::METADATA) {
            $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));
        }

        $renamed = $this->adapter->renameAttribute($collection->getId(), $old, $new);

        $this->trigger(self::EVENT_ATTRIBUTE_UPDATE, $attributeNew);

        return $renamed;
    }

    /**
     * Create a relationship attribute
     *
     * @param string $collection
     * @param string $relatedCollection
     * @param string $type
     * @param bool $twoWay
     * @param string|null $id
     * @param string|null $twoWayKey
     * @param string $onDelete
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     */
    public function createRelationship(
        string $collection,
        string $relatedCollection,
        string $type,
        bool $twoWay = false,
        ?string $id = null,
        ?string $twoWayKey = null,
        string $onDelete = Database::RELATION_MUTATE_RESTRICT
    ): bool {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        $relatedCollection = $this->silent(fn () => $this->getCollection($relatedCollection));

        if ($relatedCollection->isEmpty()) {
            throw new DatabaseException('Related collection not found');
        }

        $id ??= $relatedCollection->getId();

        $twoWayKey ??= $collection->getId();

        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<Document> $attributes */
        foreach ($attributes as $attribute) {
            if (\strtolower($attribute->getId()) === \strtolower($id)) {
                throw new DuplicateException('Attribute already exists');
            }

            if (
                $attribute->getAttribute('type') === self::VAR_RELATIONSHIP
                && \strtolower($attribute->getAttribute('options')['twoWayKey']) === \strtolower($twoWayKey)
                && $attribute->getAttribute('options')['relatedCollection'] === $relatedCollection->getId()
            ) {
                throw new DuplicateException('Related attribute already exists');
            }
        }

        if (
            $this->adapter->getLimitForAttributes() > 0 &&
            ($this->adapter->getCountOfAttributes($collection) >= $this->adapter->getLimitForAttributes()
                || $this->adapter->getCountOfAttributes($relatedCollection) >= $this->adapter->getLimitForAttributes())
        ) {
            throw new LimitException('Column limit reached. Cannot create new attribute.');
        }

        if (
            $this->adapter->getDocumentSizeLimit() > 0 &&
            ($this->adapter->getAttributeWidth($collection) >= $this->adapter->getDocumentSizeLimit()
                || $this->adapter->getAttributeWidth($relatedCollection) >= $this->adapter->getDocumentSizeLimit())
        ) {
            throw new LimitException('Row width limit reached. Cannot create new attribute.');
        }

        $relationship = new Document([
            '$id' => ID::custom($id),
            'key' => $id,
            'type' => Database::VAR_RELATIONSHIP,
            'required' => false,
            'default' => null,
            'options' => [
                'relatedCollection' => $relatedCollection->getId(),
                'relationType' => $type,
                'twoWay' => $twoWay,
                'twoWayKey' => $twoWayKey,
                'onDelete' => $onDelete,
                'side' => Database::RELATION_SIDE_PARENT,
            ],
        ]);

        $twoWayRelationship = new Document([
            '$id' => ID::custom($twoWayKey),
            'key' => $twoWayKey,
            'type' => Database::VAR_RELATIONSHIP,
            'required' => false,
            'default' => null,
            'options' => [
                'relatedCollection' => $collection->getId(),
                'relationType' => $type,
                'twoWay' => $twoWay,
                'twoWayKey' => $id,
                'onDelete' => $onDelete,
                'side' => Database::RELATION_SIDE_CHILD,
            ],
        ]);

        $collection->setAttribute('attributes', $relationship, Document::SET_TYPE_APPEND);
        $relatedCollection->setAttribute('attributes', $twoWayRelationship, Document::SET_TYPE_APPEND);

        if ($type === self::RELATION_MANY_TO_MANY) {
            $this->silent(fn () => $this->createCollection('_' . $collection->getInternalId() . '_' . $relatedCollection->getInternalId(), [
                new Document([
                    '$id' => $id,
                    'key' => $id,
                    'type' => self::VAR_STRING,
                    'size' => Database::LENGTH_KEY,
                    'required' => true,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
                new Document([
                    '$id' => $twoWayKey,
                    'key' => $twoWayKey,
                    'type' => self::VAR_STRING,
                    'size' => Database::LENGTH_KEY,
                    'required' => true,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
            ], [
                new Document([
                    '$id' => '_index_' . $id,
                    'key' => 'index_' . $id,
                    'type' => self::INDEX_KEY,
                    'attributes' => [$id],
                ]),
                new Document([
                    '$id' => '_index_' . $twoWayKey,
                    'key' => '_index_' . $twoWayKey,
                    'type' => self::INDEX_KEY,
                    'attributes' => [$twoWayKey],
                ]),
            ]));
        }

        $created = $this->adapter->createRelationship(
            $collection->getId(),
            $relatedCollection->getId(),
            $type,
            $twoWay,
            $id,
            $twoWayKey
        );

        if (!$created) {
            throw new DatabaseException('Failed to create relationship');
        }

        $this->silent(function () use ($collection, $relatedCollection, $type, $twoWay, $id, $twoWayKey) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
            $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);

            $indexKey = '_index_' . $id;
            $twoWayIndexKey = '_index_' . $twoWayKey;

            switch ($type) {
                case self::RELATION_ONE_TO_ONE:
                    $this->createIndex($collection->getId(), $indexKey, self::INDEX_UNIQUE, [$id]);
                    if ($twoWay) {
                        $this->createIndex($relatedCollection->getId(), $twoWayIndexKey, self::INDEX_UNIQUE, [$twoWayKey]);
                    }
                    break;
                case self::RELATION_ONE_TO_MANY:
                    $this->createIndex($relatedCollection->getId(), $twoWayIndexKey, self::INDEX_KEY, [$twoWayKey]);
                    break;
                case self::RELATION_MANY_TO_ONE:
                    $this->createIndex($collection->getId(), $indexKey, self::INDEX_KEY, [$id]);
                    break;
                case self::RELATION_MANY_TO_MANY:
                    // Indexes created on junction collection creation
                    break;
                default:
                    throw new DatabaseException('Invalid relationship type.');
            }
        });

        $this->trigger(self::EVENT_ATTRIBUTE_CREATE, $relationship);

        return true;
    }

    /**
     * Update a relationship attribute
     *
     * @param string $collection
     * @param string $id
     * @param string|null $newKey
     * @param string|null $newTwoWayKey
     * @param bool|null $twoWay
     * @param string|null $onDelete
     * @return bool
     * @throws ConflictException
     * @throws DatabaseException
     */
    public function updateRelationship(
        string  $collection,
        string  $id,
        ?string $newKey = null,
        ?string $newTwoWayKey = null,
        ?bool $twoWay = null,
        ?string $onDelete = null
    ): bool {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (
            \is_null($newKey)
            && \is_null($newTwoWayKey)
            && \is_null($twoWay)
            && \is_null($onDelete)
        ) {
            return true;
        }

        $collection = $this->getCollection($collection);
        $attributes = $collection->getAttribute('attributes', []);

        if (
            !\is_null($newKey)
            && \in_array($newKey, \array_map(fn ($attribute) => $attribute['key'], $attributes))
        ) {
            throw new DuplicateException('Attribute already exists');
        }

        $this->updateAttributeMeta($collection->getId(), $id, function ($attribute) use ($collection, $id, $newKey, $newTwoWayKey, $twoWay, $onDelete) {
            $altering = (!\is_null($newKey) && $newKey !== $id)
                || (!\is_null($newTwoWayKey) && $newTwoWayKey !== $attribute['options']['twoWayKey']);

            $relatedCollectionId = $attribute['options']['relatedCollection'];
            $relatedCollection = $this->getCollection($relatedCollectionId);
            $relatedAttributes = $relatedCollection->getAttribute('attributes', []);

            if (
                !\is_null($newTwoWayKey)
                && \in_array($newTwoWayKey, \array_map(fn ($attribute) => $attribute['key'], $relatedAttributes))
            ) {
                throw new DuplicateException('Related attribute already exists');
            }

            $type = $attribute['options']['relationType'];
            $side = $attribute['options']['side'];

            $newKey ??= $attribute['key'];
            $twoWayKey = $attribute['options']['twoWayKey'];
            $newTwoWayKey ??= $attribute['options']['twoWayKey'];
            $twoWay ??= $attribute['options']['twoWay'];
            $onDelete ??= $attribute['options']['onDelete'];

            $attribute->setAttribute('$id', $newKey);
            $attribute->setAttribute('key', $newKey);
            $attribute->setAttribute('options', [
                'relatedCollection' => $relatedCollection->getId(),
                'relationType' => $type,
                'twoWay' => $twoWay,
                'twoWayKey' => $newTwoWayKey,
                'onDelete' => $onDelete,
                'side' => $side,
            ]);


            $this->updateAttributeMeta($relatedCollection->getId(), $twoWayKey, function ($twoWayAttribute) use ($newKey, $newTwoWayKey, $twoWay, $onDelete) {
                $options = $twoWayAttribute->getAttribute('options', []);
                $options['twoWayKey'] = $newKey;
                $options['twoWay'] = $twoWay;
                $options['onDelete'] = $onDelete;

                $twoWayAttribute->setAttribute('$id', $newTwoWayKey);
                $twoWayAttribute->setAttribute('key', $newTwoWayKey);
                $twoWayAttribute->setAttribute('options', $options);
            });

            if ($type === self::RELATION_MANY_TO_MANY) {
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $this->updateAttributeMeta($junction, $id, function ($junctionAttribute) use ($newKey) {
                    $junctionAttribute->setAttribute('$id', $newKey);
                    $junctionAttribute->setAttribute('key', $newKey);
                });
                $this->updateAttributeMeta($junction, $twoWayKey, function ($junctionAttribute) use ($newTwoWayKey) {
                    $junctionAttribute->setAttribute('$id', $newTwoWayKey);
                    $junctionAttribute->setAttribute('key', $newTwoWayKey);
                });

                $this->purgeCachedCollection($junction);
            }

            if ($altering) {
                $updated = $this->adapter->updateRelationship(
                    $collection->getId(),
                    $relatedCollection->getId(),
                    $type,
                    $twoWay,
                    $id,
                    $twoWayKey,
                    $newKey,
                    $newTwoWayKey
                );

                if (!$updated) {
                    throw new DatabaseException('Failed to update relationship');
                }
            }

            $this->purgeCachedCollection($collection->getId());
            $this->purgeCachedCollection($relatedCollection->getId());

            $renameIndex = function (string $collection, string $key, string $newKey) {
                $this->updateIndexMeta(
                    $collection,
                    '_index_' . $key,
                    fn ($index) =>
                    $index->setAttribute('attributes', [$newKey])
                );
                $this->silent(
                    fn () =>
                    $this->renameIndex($collection, '_index_' . $key, '_index_' . $newKey)
                );
            };

            switch ($type) {
                case self::RELATION_ONE_TO_ONE:
                    if ($id !== $newKey) {
                        $renameIndex($collection->getId(), $id, $newKey);
                    }
                    if ($twoWay && $twoWayKey !== $newTwoWayKey) {
                        $renameIndex($relatedCollection->getId(), $twoWayKey, $newTwoWayKey);
                    }
                    break;
                case self::RELATION_ONE_TO_MANY:
                    if ($twoWayKey !== $newTwoWayKey) {
                        $renameIndex($relatedCollection->getId(), $twoWayKey, $newTwoWayKey);
                    }
                    break;
                case self::RELATION_MANY_TO_ONE:
                    if ($id !== $newKey) {
                        $renameIndex($collection->getId(), $id, $newKey);
                    }
                    break;
                case self::RELATION_MANY_TO_MANY:
                    $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                    if ($id !== $newKey) {
                        $renameIndex($junction, $id, $newKey);
                    }
                    if ($twoWayKey !== $newTwoWayKey) {
                        $renameIndex($junction, $twoWayKey, $newTwoWayKey);
                    }
                    break;
                default:
                    throw new DatabaseException('Invalid relationship type.');
            }
        });

        return true;
    }

    /**
     * Delete a relationship attribute
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function deleteRelationship(string $collection, string $id): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $relationship = null;

        foreach ($attributes as $name => $attribute) {
            if ($attribute['$id'] === $id) {
                $relationship = $attribute;
                unset($attributes[$name]);
                break;
            }
        }

        if (\is_null($relationship)) {
            throw new DatabaseException('Attribute not found');
        }

        $collection->setAttribute('attributes', \array_values($attributes));

        $relatedCollection = $relationship['options']['relatedCollection'];
        $type = $relationship['options']['relationType'];
        $twoWay = $relationship['options']['twoWay'];
        $twoWayKey = $relationship['options']['twoWayKey'];
        $side = $relationship['options']['side'];

        $relatedCollection = $this->silent(fn () => $this->getCollection($relatedCollection));
        $relatedAttributes = $relatedCollection->getAttribute('attributes', []);

        foreach ($relatedAttributes as $name => $attribute) {
            if ($attribute['$id'] === $twoWayKey) {
                unset($relatedAttributes[$name]);
                break;
            }
        }

        $relatedCollection->setAttribute('attributes', \array_values($relatedAttributes));

        $this->silent(function () use ($collection, $relatedCollection, $type, $twoWay, $id, $twoWayKey, $side) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
            $this->updateDocument(self::METADATA, $relatedCollection->getId(), $relatedCollection);

            $indexKey = '_index_' . $id;
            $twoWayIndexKey = '_index_' . $twoWayKey;

            switch ($type) {
                case self::RELATION_ONE_TO_ONE:
                    if ($side === Database::RELATION_SIDE_PARENT) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                        if ($twoWay) {
                            $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        }
                    }
                    if ($side === Database::RELATION_SIDE_CHILD) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                        if ($twoWay) {
                            $this->deleteIndex($collection->getId(), $indexKey);
                        }
                    }
                    break;
                case self::RELATION_ONE_TO_MANY:
                    if ($side === Database::RELATION_SIDE_PARENT) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                    } elseif ($twoWay) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                    }
                    break;
                case self::RELATION_MANY_TO_ONE:
                    if ($side === Database::RELATION_SIDE_CHILD) {
                        $this->deleteIndex($relatedCollection->getId(), $twoWayIndexKey);
                    } elseif ($twoWay) {
                        $this->deleteIndex($collection->getId(), $indexKey);
                    }
                    break;
                case self::RELATION_MANY_TO_MANY:
                    $junction = $this->getJunctionCollection(
                        $collection,
                        $relatedCollection,
                        $side
                    );

                    $this->deleteDocument(self::METADATA, $junction);
                    break;
                default:
                    throw new DatabaseException('Invalid relationship type.');
            }
        });

        $deleted = $this->adapter->deleteRelationship(
            $collection->getId(),
            $relatedCollection->getId(),
            $type,
            $twoWay,
            $id,
            $twoWayKey,
            $side
        );

        if (!$deleted) {
            throw new DatabaseException('Failed to delete relationship');
        }

        $this->purgeCachedCollection($collection->getId());
        $this->purgeCachedCollection($relatedCollection->getId());

        $this->trigger(self::EVENT_ATTRIBUTE_DELETE, $relationship);

        return true;
    }

    /**
     * Rename Index
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     *
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        $indexes = $collection->getAttribute('indexes', []);

        $index = \in_array($old, \array_map(fn ($index) => $index['$id'], $indexes));

        if ($index === false) {
            throw new DatabaseException('Index not found');
        }

        $indexNew = \in_array($new, \array_map(fn ($index) => $index['$id'], $indexes));

        if ($indexNew !== false) {
            throw new DuplicateException('Index name already used');
        }

        foreach ($indexes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $old) {
                $indexes[$key]['key'] = $new;
                $indexes[$key]['$id'] = $new;
                $indexNew = $indexes[$key];
                break;
            }
        }

        $collection->setAttribute('indexes', $indexes);

        $this->adapter->renameIndex($collection->getId(), $old, $new);

        if ($collection->getId() !== self::METADATA) {
            $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));
        }

        $this->trigger(self::EVENT_INDEX_RENAME, $indexNew);

        return true;
    }

    /**
     * Create Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array<string> $attributes
     * @param array<int> $lengths
     * @param array<string> $orders
     *
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     * @throws Exception
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths = [], array $orders = []): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (empty($attributes)) {
            throw new DatabaseException('Missing attributes');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        // index IDs are case-insensitive
        $indexes = $collection->getAttribute('indexes', []);

        /** @var array<Document> $indexes */
        foreach ($indexes as $index) {
            if (\strtolower($index->getId()) === \strtolower($id)) {
                throw new DuplicateException('Index already exists');
            }
        }

        if ($this->adapter->getCountOfIndexes($collection) >= $this->adapter->getLimitForIndexes()) {
            throw new LimitException('Index limit reached. Cannot create new index.');
        }

        switch ($type) {
            case self::INDEX_KEY:
                if (!$this->adapter->getSupportForIndex()) {
                    throw new DatabaseException('Key index is not supported');
                }
                break;

            case self::INDEX_UNIQUE:
                if (!$this->adapter->getSupportForUniqueIndex()) {
                    throw new DatabaseException('Unique index is not supported');
                }
                break;

            case self::INDEX_FULLTEXT:
                if (!$this->adapter->getSupportForFulltextIndex()) {
                    throw new DatabaseException('Fulltext index is not supported');
                }
                break;

            default:
                throw new DatabaseException('Unknown index type: ' . $type . '. Must be one of ' . Database::INDEX_KEY . ', ' . Database::INDEX_UNIQUE . ', ' . Database::INDEX_FULLTEXT);
        }

        /** @var array<Document> $collectionAttributes */
        $collectionAttributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $i => $attr) {
            foreach ($collectionAttributes as $collectionAttribute) {
                if($collectionAttribute->getAttribute('key') === $attr) {
                    $isArray = $collectionAttribute->getAttribute('array', false);
                    if($isArray) {
                        if($this->adapter->getMaxIndexLength() > 0) {
                            $lengths[$i] = self::ARRAY_INDEX_LENGTH;
                        }
                        $orders[$i] = null;
                    }
                    break;
                }
            }
        }

        $index = new Document([
            '$id' => ID::custom($id),
            'key' => $id,
            'type' => $type,
            'attributes' => $attributes,
            'lengths' => $lengths,
            'orders' => $orders,
        ]);

        $collection->setAttribute('indexes', $index, Document::SET_TYPE_APPEND);

        if ($this->validate) {
            $validator = new IndexValidator(
                $collection->getAttribute('attributes', []),
                $this->adapter->getMaxIndexLength()
            );
            if (!$validator->isValid($index)) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        $index = $this->adapter->createIndex($collection->getId(), $id, $type, $attributes, $lengths, $orders);

        if ($collection->getId() !== self::METADATA) {
            $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));
        }

        $this->trigger(self::EVENT_INDEX_CREATE, $index);

        return $index;
    }

    /**
     * Delete Index
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        $indexes = $collection->getAttribute('indexes', []);

        $indexDeleted = null;
        foreach ($indexes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $id) {
                $indexDeleted = $value;
                unset($indexes[$key]);
            }
        }

        $collection->setAttribute('indexes', \array_values($indexes));

        if ($collection->getId() !== self::METADATA) {
            $this->silent(fn () => $this->updateDocument(self::METADATA, $collection->getId(), $collection));
        }

        $deleted = $this->adapter->deleteIndex($collection->getId(), $id);

        $this->trigger(self::EVENT_INDEX_DELETE, $indexDeleted);

        return $deleted;
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     * @param Query[] $queries
     *
     * @return Document
     * @throws DatabaseException
     * @throws Exception
     */
    public function getDocument(string $collection, string $id, array $queries = []): Document
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if ($collection === self::METADATA && $id === self::METADATA) {
            return new Document(self::COLLECTION);
        }

        if (empty($collection)) {
            throw new DatabaseException('Collection not found');
        }

        if (empty($id)) {
            return new Document();
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        $attributes = $collection->getAttribute('attributes', []);

        if ($this->validate) {
            $validator = new DocumentValidator($attributes);
            if (!$validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn (Document $attribute) => $attribute->getAttribute('type') === self::VAR_RELATIONSHIP
        );

        $selects = Query::groupByType($queries)['selections'];
        $selections = $this->validateSelections($collection, $selects);
        $nestedSelections = [];

        foreach ($queries as $query) {
            if ($query->getMethod() == Query::TYPE_SELECT) {
                $values = $query->getValues();
                foreach ($values as $valueIndex => $value) {
                    if (\str_contains($value, '.')) {
                        // Shift the top level off the dot-path to pass the selection down the chain
                        // 'foo.bar.baz' becomes 'bar.baz'
                        $nestedSelections[] = Query::select([
                            \implode('.', \array_slice(\explode('.', $value), 1))
                        ]);

                        $key = \explode('.', $value)[0];

                        foreach ($relationships as $relationship) {
                            if ($relationship->getAttribute('key') === $key) {
                                switch ($relationship->getAttribute('options')['relationType']) {
                                    case Database::RELATION_MANY_TO_MANY:
                                    case Database::RELATION_ONE_TO_MANY:
                                        unset($values[$valueIndex]);
                                        break;

                                    case Database::RELATION_MANY_TO_ONE:
                                    case Database::RELATION_ONE_TO_ONE:
                                        $values[$valueIndex] = $key;
                                        break;
                                }
                            }
                        }
                    }
                }
                $query->setValues(\array_values($values));
            }
        }

        $queries = \array_values($queries);

        $validator = new Authorization(self::PERMISSION_READ);
        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $cacheKey = $this->cacheName . '-cache-' . $this->getNamespace() . ':' . $this->adapter->getTenant() . ':' . $collection->getId() . ':' . $id;

        if (!empty($selections)) {
            $cacheKey .= ':' . \md5(\implode($selections));
        } else {
            $cacheKey .= ':*';
        }

        if ($cache = $this->cache->load($cacheKey, self::TTL)) {
            $document = new Document($cache);

            if ($collection->getId() !== self::METADATA) {
                if (!$validator->isValid([
                    ...$collection->getRead(),
                    ...($documentSecurity ? $document->getRead() : [])
                ])) {
                    return new Document();
                }
            }

            $this->trigger(self::EVENT_DOCUMENT_READ, $document);

            return $document;
        }

        $document = $this->adapter->getDocument($collection->getId(), $id, $queries);

        if ($document->isEmpty()) {
            return $document;
        }

        $document->setAttribute('$collection', $collection->getId());

        if ($collection->getId() !== self::METADATA) {
            if (!$validator->isValid([
                ...$collection->getRead(),
                ...($documentSecurity ? $document->getRead() : [])
            ])) {
                return new Document();
            }
        }

        $document = $this->casting($collection, $document);
        $document = $this->decode($collection, $document, $selections);
        $this->map = [];

        if ($this->resolveRelationships && (empty($selects) || !empty($nestedSelections))) {
            $document = $this->silent(fn () => $this->populateDocumentRelationships($collection, $document, $nestedSelections));
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn ($attribute) =>
            $attribute['type'] === Database::VAR_RELATIONSHIP
        );

        $hasTwoWayRelationship = false;
        foreach ($relationships as $relationship) {
            if ($relationship['options']['twoWay']) {
                $hasTwoWayRelationship = true;
                break;
            }
        }

        /**
         * Bug with function purity in PHPStan means it thinks $this->map is always empty
         *
         * @phpstan-ignore-next-line
         */
        foreach ($this->map as $key => $value) {
            [$k, $v] = \explode('=>', $key);
            $ck = $this->cacheName . '-cache-' . $this->getNamespace() . ':' . $this->adapter->getTenant() . ':map:' . $k;
            $cache = $this->cache->load($ck, self::TTL);
            if (empty($cache)) {
                $cache = [];
            }
            if (!\in_array($v, $cache)) {
                $cache[] = $v;
                $this->cache->save($ck, $cache);
            }
        }

        // Don't save to cache if it's part of a two-way relationship or a relationship at all
        if (!$hasTwoWayRelationship && empty($relationships)) {
            $this->cache->save($cacheKey, $document->getArrayCopy());
        }

        // Remove internal attributes if not queried for select query
        // $id, $permissions and $collection are the default selected attributes for (MariaDB, MySQL, SQLite, Postgres)
        // All internal attributes are default selected attributes for (MongoDB)
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                $values = $query->getValues();
                foreach (Database::INTERNAL_ATTRIBUTES as $internalAttribute) {
                    if (!in_array($internalAttribute['$id'], $values)) {
                        $document->removeAttribute($internalAttribute['$id']);
                    }
                }
            }
        }

        $this->trigger(self::EVENT_DOCUMENT_READ, $document);

        return $document;
    }

    /**
     * @param Document $collection
     * @param Document $document
     * @param array<Query> $queries
     * @return Document
     * @throws DatabaseException
     */
    private function populateDocumentRelationships(Document $collection, Document $document, array $queries = []): Document
    {
        $attributes = $collection->getAttribute('attributes', []);

        $relationships = \array_filter($attributes, function ($attribute) {
            return $attribute['type'] === Database::VAR_RELATIONSHIP;
        });

        foreach ($relationships as $relationship) {
            $key = $relationship['key'];
            $value = $document->getAttribute($key);
            $relatedCollection = $this->getCollection($relationship['options']['relatedCollection']);
            $relationType = $relationship['options']['relationType'];
            $twoWay = $relationship['options']['twoWay'];
            $twoWayKey = $relationship['options']['twoWayKey'];
            $side = $relationship['options']['side'];

            if (!empty($value)) {
                $k = $relatedCollection->getId() . ':' . $value . '=>' . $collection->getId() . ':' . $document->getId();
                if ($relationType === Database::RELATION_ONE_TO_MANY) {
                    $k = $collection->getId() . ':' . $document->getId() . '=>' . $relatedCollection->getId() . ':' . $value;
                }
                $this->map[$k] = true;
            }

            $relationship->setAttribute('collection', $collection->getId());
            $relationship->setAttribute('document', $document->getId());

            $skipFetch = false;
            foreach ($this->relationshipFetchStack as $fetchedRelationship) {
                $existingKey = $fetchedRelationship['key'];
                $existingCollection = $fetchedRelationship['collection'];
                $existingRelatedCollection = $fetchedRelationship['options']['relatedCollection'];
                $existingTwoWayKey = $fetchedRelationship['options']['twoWayKey'];
                $existingSide = $fetchedRelationship['options']['side'];

                // If this relationship has already been fetched for this document, skip it
                $reflexive = $fetchedRelationship == $relationship;

                // If this relationship is the same as a previously fetched relationship, but on the other side, skip it
                $symmetric = $existingKey === $twoWayKey
                    && $existingTwoWayKey === $key
                    && $existingRelatedCollection === $collection->getId()
                    && $existingCollection === $relatedCollection->getId()
                    && $existingSide !== $side;

                // If this relationship is not directly related but relates across multiple collections, skip it.
                //
                // These conditions ensure that a relationship is considered transitive if it has the same
                // two-way key and related collection, but is on the opposite side of the relationship (the first and second conditions).
                //
                // They also ensure that a relationship is considered transitive if it has the same key and related
                // collection as an existing relationship, but a different two-way key (the third condition),
                // or the same two-way key as an existing relationship, but a different key (the fourth condition).
                $transitive = (($existingKey === $twoWayKey
                    && $existingCollection === $relatedCollection->getId()
                    && $existingSide !== $side)
                    || ($existingTwoWayKey === $key
                        && $existingRelatedCollection === $collection->getId()
                        && $existingSide !== $side)
                    || ($existingKey === $key
                        && $existingTwoWayKey !== $twoWayKey
                        && $existingRelatedCollection === $relatedCollection->getId()
                        && $existingSide !== $side)
                    || ($existingKey !== $key
                        && $existingTwoWayKey === $twoWayKey
                        && $existingRelatedCollection === $relatedCollection->getId()
                        && $existingSide !== $side));

                if ($reflexive || $symmetric || $transitive) {
                    $skipFetch = true;
                }
            }

            switch ($relationType) {
                case Database::RELATION_ONE_TO_ONE:
                    if ($skipFetch || $twoWay && ($this->relationshipFetchDepth === Database::RELATION_MAX_DEPTH)) {
                        $document->removeAttribute($key);
                        break;
                    }

                    if (\is_null($value)) {
                        break;
                    }

                    $this->relationshipFetchDepth++;
                    $this->relationshipFetchStack[] = $relationship;

                    $related = $this->getDocument($relatedCollection->getId(), $value, $queries);

                    $this->relationshipFetchDepth--;
                    \array_pop($this->relationshipFetchStack);

                    $document->setAttribute($key, $related);
                    break;
                case Database::RELATION_ONE_TO_MANY:
                    if ($side === Database::RELATION_SIDE_CHILD) {
                        if (!$twoWay || $this->relationshipFetchDepth === Database::RELATION_MAX_DEPTH || $skipFetch) {
                            $document->removeAttribute($key);
                            break;
                        }
                        if (!\is_null($value)) {
                            $this->relationshipFetchDepth++;
                            $this->relationshipFetchStack[] = $relationship;

                            $related = $this->getDocument($relatedCollection->getId(), $value, $queries);

                            $this->relationshipFetchDepth--;
                            \array_pop($this->relationshipFetchStack);

                            $document->setAttribute($key, $related);
                        }
                        break;
                    }

                    if ($this->relationshipFetchDepth === Database::RELATION_MAX_DEPTH || $skipFetch) {
                        break;
                    }

                    $this->relationshipFetchDepth++;
                    $this->relationshipFetchStack[] = $relationship;

                    $relatedDocuments = $this->find($relatedCollection->getId(), [
                        Query::equal($twoWayKey, [$document->getId()]),
                        Query::limit(PHP_INT_MAX),
                        ...$queries
                    ]);

                    $this->relationshipFetchDepth--;
                    \array_pop($this->relationshipFetchStack);

                    foreach ($relatedDocuments as $related) {
                        $related->removeAttribute($twoWayKey);
                    }

                    $document->setAttribute($key, $relatedDocuments);
                    break;
                case Database::RELATION_MANY_TO_ONE:
                    if ($side === Database::RELATION_SIDE_PARENT) {
                        if ($skipFetch || $this->relationshipFetchDepth === Database::RELATION_MAX_DEPTH) {
                            $document->removeAttribute($key);
                            break;
                        }

                        if (\is_null($value)) {
                            break;
                        }
                        $this->relationshipFetchDepth++;
                        $this->relationshipFetchStack[] = $relationship;

                        $related = $this->getDocument($relatedCollection->getId(), $value, $queries);

                        $this->relationshipFetchDepth--;
                        \array_pop($this->relationshipFetchStack);

                        $document->setAttribute($key, $related);
                        break;
                    }

                    if (!$twoWay) {
                        $document->removeAttribute($key);
                        break;
                    }

                    if ($this->relationshipFetchDepth === Database::RELATION_MAX_DEPTH || $skipFetch) {
                        break;
                    }

                    $this->relationshipFetchDepth++;
                    $this->relationshipFetchStack[] = $relationship;

                    $relatedDocuments = $this->find($relatedCollection->getId(), [
                        Query::equal($twoWayKey, [$document->getId()]),
                        Query::limit(PHP_INT_MAX),
                        ...$queries
                    ]);

                    $this->relationshipFetchDepth--;
                    \array_pop($this->relationshipFetchStack);


                    foreach ($relatedDocuments as $related) {
                        $related->removeAttribute($twoWayKey);
                    }

                    $document->setAttribute($key, $relatedDocuments);
                    break;
                case Database::RELATION_MANY_TO_MANY:
                    if (!$twoWay && $side === Database::RELATION_SIDE_CHILD) {
                        break;
                    }

                    if ($twoWay && ($this->relationshipFetchDepth === Database::RELATION_MAX_DEPTH || $skipFetch)) {
                        break;
                    }

                    $this->relationshipFetchDepth++;
                    $this->relationshipFetchStack[] = $relationship;

                    $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                    $junctions = $this->skipRelationships(fn () => $this->find($junction, [
                        Query::equal($twoWayKey, [$document->getId()]),
                        Query::limit(PHP_INT_MAX)
                    ]));

                    $related = [];
                    foreach ($junctions as $junction) {
                        $related[] = $this->getDocument(
                            $relatedCollection->getId(),
                            $junction->getAttribute($key),
                            $queries
                        );
                    }

                    $this->relationshipFetchDepth--;
                    \array_pop($this->relationshipFetchStack);

                    $document->setAttribute($key, $related);
                    break;
            }
        }

        return $document;
    }

    /**
     * Create Document
     *
     * @param string $collection
     * @param Document $document
     *
     * @return Document
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function createDocument(string $collection, Document $document): Document
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() !== self::METADATA) {
            $authorization = new Authorization(self::PERMISSION_CREATE);
            if (!$authorization->isValid($collection->getCreate())) {
                throw new AuthorizationException($authorization->getDescription());
            }
        }

        $time = DateTime::now();

        $createdAt = $document->getCreatedAt();
        $updatedAt = $document->getUpdatedAt();

        $document
            ->setAttribute('$id', empty($document->getId()) ? ID::unique() : $document->getId())
            ->setAttribute('$collection', $collection->getId())
            ->setAttribute('$createdAt', empty($createdAt) || !$this->preserveDates ? $time : $createdAt)
            ->setAttribute('$updatedAt', empty($updatedAt) || !$this->preserveDates ? $time : $updatedAt);

        $document = $this->encode($collection, $document);

        if ($this->validate) {
            $validator = new Permissions();
            if (!$validator->isValid($document->getPermissions())) {
                throw new DatabaseException($validator->getDescription());
            }
        }

        $structure = new Structure($collection);
        if (!$structure->isValid($document)) {
            throw new StructureException($structure->getDescription());
        }

        if ($this->resolveRelationships) {
            $document = $this->silent(fn () => $this->createDocumentRelationships($collection, $document));
        }

        $document = $this->adapter->createDocument($collection->getId(), $document);

        if ($this->resolveRelationships) {
            $document = $this->silent(fn () => $this->populateDocumentRelationships($collection, $document));
        }

        $document = $this->decode($collection, $document);

        $this->trigger(self::EVENT_DOCUMENT_CREATE, $document);

        return $document;
    }

    /**
     * Create Documents in a batch
     *
     * @param string $collection
     * @param array<Document> $documents
     * @param int $batchSize
     *
     * @return array<Document>
     *
     * @throws AuthorizationException
     * @throws StructureException
     * @throws Exception
     */
    public function createDocuments(string $collection, array $documents, int $batchSize = self::INSERT_BATCH_SIZE): array
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (empty($documents)) {
            return [];
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        $time = DateTime::now();

        foreach ($documents as $key => $document) {
            if ($this->adapter->getShareTables()) {
                if (empty($document->getAttribute('$tenant'))) {
                    throw new DatabaseException('Missing tenant. Tenant must be included when isolation mode is set to "table".');
                }
            }

            $createdAt = $document->getCreatedAt();
            $updatedAt = $document->getUpdatedAt();

            $document
                ->setAttribute('$id', empty($document->getId()) ? ID::unique() : $document->getId())
                ->setAttribute('$collection', $collection->getId())
                ->setAttribute('$createdAt', empty($createdAt) || !$this->preserveDates ? $time : $createdAt)
                ->setAttribute('$updatedAt', empty($updatedAt) || !$this->preserveDates ? $time : $updatedAt);

            $document = $this->encode($collection, $document);

            $validator = new Structure($collection);
            if (!$validator->isValid($document)) {
                throw new StructureException($validator->getDescription());
            }

            $documents[$key] = $document;
        }

        $documents = $this->adapter->createDocuments($collection->getId(), $documents, $batchSize);

        foreach ($documents as $key => $document) {
            $documents[$key] = $this->decode($collection, $document);
        }

        $this->trigger(self::EVENT_DOCUMENTS_CREATE, $documents);

        return $documents;
    }

    /**
     * @param Document $collection
     * @param Document $document
     * @return Document
     * @throws DatabaseException
     */
    private function createDocumentRelationships(Document $collection, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);

        $relationships = \array_filter(
            $attributes,
            fn ($attribute) =>
            $attribute['type'] === Database::VAR_RELATIONSHIP
        );

        $stackCount = count($this->relationshipWriteStack);

        foreach ($relationships as $index => $relationship) {
            $key = $relationship['key'];
            $value = $document->getAttribute($key);
            $relatedCollection = $this->getCollection($relationship['options']['relatedCollection']);
            $relationType = $relationship['options']['relationType'];
            $twoWay = $relationship['options']['twoWay'];
            $twoWayKey = $relationship['options']['twoWayKey'];
            $side = $relationship['options']['side'];

            if ($stackCount >= Database::RELATION_MAX_DEPTH - 1 && $this->relationshipWriteStack[$stackCount - 1] !== $relatedCollection->getId()) {
                $document->removeAttribute($key);

                continue;
            }

            $this->relationshipWriteStack[] = $collection->getId();

            try {
                switch (\gettype($value)) {
                    case 'array':
                        // List of documents or IDs
                        foreach ($value as $related) {
                            switch (\gettype($related)) {
                                case 'object':
                                    if (!$related instanceof Document) {
                                        throw new DatabaseException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                                    }
                                    $this->relateDocuments(
                                        $collection,
                                        $relatedCollection,
                                        $key,
                                        $document,
                                        $related,
                                        $relationType,
                                        $twoWay,
                                        $twoWayKey,
                                        $side,
                                    );
                                    break;
                                case 'string':
                                    $this->relateDocumentsById(
                                        $collection,
                                        $relatedCollection,
                                        $key,
                                        $document->getId(),
                                        $related,
                                        $relationType,
                                        $twoWay,
                                        $twoWayKey,
                                        $side,
                                    );
                                    break;
                                default:
                                    throw new DatabaseException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                            }
                        }
                        $document->removeAttribute($key);
                        break;
                    case 'object':
                        if (!$value instanceof Document) {
                            throw new DatabaseException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                        }
                        $relatedId = $this->relateDocuments(
                            $collection,
                            $relatedCollection,
                            $key,
                            $document,
                            $value,
                            $relationType,
                            $twoWay,
                            $twoWayKey,
                            $side,
                        );
                        $document->setAttribute($key, $relatedId);
                        break;
                    case 'string':
                        // Single document ID
                        $this->relateDocumentsById(
                            $collection,
                            $relatedCollection,
                            $key,
                            $document->getId(),
                            $value,
                            $relationType,
                            $twoWay,
                            $twoWayKey,
                            $side,
                        );
                        break;
                    case 'NULL':
                        // TODO: This might need to depend on the relation type, to be either set to null or removed?
                        $document->removeAttribute($key);
                        // No related document
                        break;
                    default:
                        throw new DatabaseException('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');
                }
            } finally {
                \array_pop($this->relationshipWriteStack);
            }
        }

        return $document;
    }

    /**
     * @param Document $collection
     * @param Document $relatedCollection
     * @param string $key
     * @param Document $document
     * @param Document $relation
     * @param string $relationType
     * @param bool $twoWay
     * @param string $twoWayKey
     * @param string $side
     * @return string related document ID
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws StructureException
     * @throws Exception
     */
    private function relateDocuments(
        Document $collection,
        Document $relatedCollection,
        string $key,
        Document $document,
        Document $relation,
        string $relationType,
        bool $twoWay,
        string $twoWayKey,
        string $side,
    ): string {
        switch ($relationType) {
            case Database::RELATION_ONE_TO_ONE:
                if ($twoWay) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    $relation->setAttribute($twoWayKey, $document->getId());
                }
                break;
        }

        // Try to get the related document
        $related = $this->getDocument($relatedCollection->getId(), $relation->getId());

        if ($related->isEmpty()) {
            // If the related document doesn't exist, create it, inheriting permissions if none are set
            if (!isset($relation['$permissions'])) {
                $relation->setAttribute('$permissions', $document->getPermissions());
            }

            $related = $this->createDocument($relatedCollection->getId(), $relation);
        } elseif ($related->getAttributes() != $relation->getAttributes()) {
            // If the related document exists and the data is not the same, update it
            foreach ($relation->getAttributes() as $attribute => $value) {
                $related->setAttribute($attribute, $value);
            }

            $related = $this->updateDocument($relatedCollection->getId(), $related->getId(), $related);
        }

        if ($relationType === Database::RELATION_MANY_TO_MANY) {
            $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

            $this->createDocument($junction, new Document([
                $key => $related->getId(),
                $twoWayKey => $document->getId(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ]
            ]));
        }

        return $related->getId();
    }

    /**
     * @param Document $collection
     * @param Document $relatedCollection
     * @param string $key
     * @param string $documentId
     * @param string $relationId
     * @param string $relationType
     * @param bool $twoWay
     * @param string $twoWayKey
     * @param string $side
     * @return void
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws StructureException
     * @throws Exception
     */
    private function relateDocumentsById(
        Document $collection,
        Document $relatedCollection,
        string $key,
        string $documentId,
        string $relationId,
        string $relationType,
        bool $twoWay,
        string $twoWayKey,
        string $side,
    ): void {
        // Get the related document, will be empty on permissions failure
        $related = $this->skipRelationships(fn () => $this->getDocument($relatedCollection->getId(), $relationId));

        if ($related->isEmpty()) {
            return;
        }

        switch ($relationType) {
            case Database::RELATION_ONE_TO_ONE:
                if ($twoWay) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->skipRelationships(fn () => $this->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->skipRelationships(fn () => $this->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    $related->setAttribute($twoWayKey, $documentId);
                    $this->skipRelationships(fn () => $this->updateDocument($relatedCollection->getId(), $relationId, $related));
                }
                break;
            case Database::RELATION_MANY_TO_MANY:
                $this->purgeCachedDocument($relatedCollection->getId(), $relationId);

                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $this->skipRelationships(fn () => $this->createDocument($junction, new Document([
                    $key => $related->getId(),
                    $twoWayKey => $documentId,
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ]
                ])));
                break;
        }
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param string $id
     * @param Document $document
     * @return Document
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws StructureException
     */
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (!$document->getId() || !$id) {
            throw new DatabaseException('Must define $id attribute');
        }

        $time = DateTime::now();
        $old = Authorization::skip(fn () => $this->silent(fn () => $this->getDocument($collection, $id))); // Skip ensures user does not need read permission for this
        $document = \array_merge($old->getArrayCopy(), $document->getArrayCopy());
        $document['$collection'] = $old->getAttribute('$collection');   // Make sure user doesn't switch collection ID
        $document['$tenant'] = $old->getAttribute('$tenant');           // Make sure user doesn't switch tenant
        $document['$createdAt'] = $old->getCreatedAt();                 // Make sure user doesn't switch createdAt
        $document = new Document($document);

        $collection = $this->silent(fn () => $this->getCollection($collection));

        $relationships = \array_filter($collection->getAttribute('attributes', []), function ($attribute) {
            return $attribute['type'] === Database::VAR_RELATIONSHIP;
        });

        $validator = new Authorization(self::PERMISSION_UPDATE);
        $shouldUpdate = false;

        if ($collection->getId() !== self::METADATA) {
            $documentSecurity = $collection->getAttribute('documentSecurity', false);

            foreach ($relationships as $relationship) {
                $relationships[$relationship->getAttribute('key')] = $relationship;
            }

            // Compare if the document has any changes
            foreach ($document as $key => $value) {
                // Skip the nested documents as they will be checked later in recursions.
                if (\array_key_exists($key, $relationships)) {
                    // No need to compare nested documents more than max depth.
                    if (count($this->relationshipWriteStack) >= Database::RELATION_MAX_DEPTH - 1) {
                        continue;
                    }
                    $relationType = (string) $relationships[$key]['options']['relationType'];
                    $side = (string) $relationships[$key]['options']['side'];

                    switch ($relationType) {
                        case Database::RELATION_ONE_TO_ONE:
                            $oldValue = $old->getAttribute($key) instanceof Document
                                ? $old->getAttribute($key)->getId()
                                : $old->getAttribute($key);

                            if ((\is_null($value) !== \is_null($oldValue))
                                || (\is_string($value) && $value !== $oldValue)
                                || ($value instanceof Document && $value->getId() !== $oldValue)
                            ) {
                                $shouldUpdate = true;
                            }
                            break;
                        case Database::RELATION_ONE_TO_MANY:
                        case Database::RELATION_MANY_TO_ONE:
                        case Database::RELATION_MANY_TO_MANY:
                            if (
                                ($relationType === Database::RELATION_MANY_TO_ONE && $side === Database::RELATION_SIDE_PARENT) ||
                                ($relationType === Database::RELATION_ONE_TO_MANY && $side === Database::RELATION_SIDE_CHILD)
                            ) {
                                $oldValue = $old->getAttribute($key) instanceof Document
                                    ? $old->getAttribute($key)->getId()
                                    : $old->getAttribute($key);

                                if ((\is_null($value) !== \is_null($oldValue))
                                    || (\is_string($value) && $value !== $oldValue)
                                    || ($value instanceof Document &&  $value->getId() !== $oldValue)
                                ) {
                                    $shouldUpdate = true;
                                }
                                break;
                            }

                            if ((\is_null($old->getAttribute($key)) !== \is_null($value))
                                || \count($old->getAttribute($key)) !== \count($value)
                            ) {
                                $shouldUpdate = true;
                                break;
                            }
                            foreach ($value as $index => $relation) {
                                $oldValue = $old->getAttribute($key)[$index] instanceof Document
                                    ? $old->getAttribute($key)[$index]->getId()
                                    : $old->getAttribute($key)[$index];

                                if ((\is_string($relation) && $relation !== $oldValue)
                                    || ($relation instanceof Document && $relation->getId() !== $oldValue)
                                ) {
                                    $shouldUpdate = true;
                                    break;
                                }
                            }
                            break;
                    }

                    if ($shouldUpdate) {
                        break;
                    }

                    continue;
                }

                $oldValue = $old->getAttribute($key);

                // If values are not equal we need to update document.
                if ($value !== $oldValue) {
                    $shouldUpdate = true;
                    break;
                }
            }

            if ($shouldUpdate && !$validator->isValid([
                ...$collection->getUpdate(),
                ...($documentSecurity ? $old->getUpdate() : [])
            ])) {
                throw new AuthorizationException($validator->getDescription());
            }
        }

        if ($shouldUpdate) {
            $updatedAt = $document->getUpdatedAt();
            $document->setAttribute('$updatedAt', empty($updatedAt) || !$this->preserveDates ? $time : $updatedAt);
        }

        // Check if document was updated after the request timestamp
        $oldUpdatedAt = new \DateTime($old->getUpdatedAt());
        if (!is_null($this->timestamp) && $oldUpdatedAt > $this->timestamp) {
            throw new ConflictException('Document was updated after the request timestamp');
        }

        $document = $this->encode($collection, $document);

        $validator = new Structure($collection);

        if (!$validator->isValid($document)) { // Make sure updated structure still apply collection rules (if any)
            throw new StructureException($validator->getDescription());
        }

        if ($this->resolveRelationships) {
            $document = $this->silent(fn () => $this->updateDocumentRelationships($collection, $old, $document));
        }

        $this->adapter->updateDocument($collection->getId(), $document);

        if ($this->resolveRelationships) {
            $document = $this->silent(fn () => $this->populateDocumentRelationships($collection, $document));
        }

        $document = $this->decode($collection, $document);

        $this->purgeRelatedDocuments($collection, $id);

        $this->purgeCachedDocument($collection->getId(), $id);

        $this->trigger(self::EVENT_DOCUMENT_UPDATE, $document);

        return $document;
    }

    /**
     * Update Documents in a batch
     *
     * @param string $collection
     * @param array<Document> $documents
     * @param int $batchSize
     *
     * @return array<Document>
     *
     * @throws AuthorizationException
     * @throws Exception
     * @throws StructureException
     */
    public function updateDocuments(string $collection, array $documents, int $batchSize = self::INSERT_BATCH_SIZE): array
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if (empty($documents)) {
            return [];
        }

        $time = DateTime::now();
        $collection = $this->silent(fn () => $this->getCollection($collection));

        foreach ($documents as $document) {
            if ($this->adapter->getShareTables() && empty($document->getAttribute('$tenant'))) {
                throw new DatabaseException('Missing tenant. Tenant must be included when isolation mode is set to "table".');
            }

            if (!$document->getId()) {
                throw new DatabaseException('Must define $id attribute for each document');
            }

            $updatedAt = $document->getUpdatedAt();
            $document->setAttribute('$updatedAt', empty($updatedAt) || !$this->preserveDates ? $time : $updatedAt);
            $document = $this->encode($collection, $document);

            $old = Authorization::skip(fn () => $this->silent(
                fn () => $this->getDocument(
                    $collection->getId(),
                    $document->getId()
                )
            ));

            $validator = new Authorization(self::PERMISSION_UPDATE);
            if (
                $collection->getId() !== self::METADATA
                && !$validator->isValid($old->getUpdate())
            ) {
                throw new AuthorizationException($validator->getDescription());
            }

            $validator = new Structure($collection);
            if (!$validator->isValid($document)) {
                throw new StructureException($validator->getDescription());
            }
        }

        $documents = $this->adapter->updateDocuments($collection->getId(), $documents, $batchSize);

        foreach ($documents as $key => $document) {
            $documents[$key] = $this->decode($collection, $document);

            $this->purgeCachedDocument($collection->getId(), $document->getId());
        }

        $this->trigger(self::EVENT_DOCUMENTS_UPDATE, $documents);

        return $documents;
    }

    /**
     * @param Document $collection
     * @param Document $old
     * @param Document $document
     *
     * @return Document
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws StructureException
     */
    private function updateDocumentRelationships(Document $collection, Document $old, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);

        $relationships = \array_filter($attributes, function ($attribute) {
            return $attribute['type'] === Database::VAR_RELATIONSHIP;
        });

        $stackCount = count($this->relationshipWriteStack);

        foreach ($relationships as $index => $relationship) {
            /** @var string $key */
            $key = $relationship['key'];
            $value = $document->getAttribute($key);
            $oldValue = $old->getAttribute($key);
            $relatedCollection = $this->getCollection($relationship['options']['relatedCollection']);
            $relationType = (string) $relationship['options']['relationType'];
            $twoWay = (bool) $relationship['options']['twoWay'];
            $twoWayKey = (string) $relationship['options']['twoWayKey'];
            $side = (string) $relationship['options']['side'];

            if ($oldValue == $value) {
                if (
                    ($relationType === Database::RELATION_ONE_TO_ONE  ||
                        ($relationType === Database::RELATION_MANY_TO_ONE && $side === Database::RELATION_SIDE_PARENT)) &&
                    $value instanceof Document
                ) {
                    $document->setAttribute($key, $value->getId());
                    continue;
                }
                $document->removeAttribute($key);
                continue;
            }

            if ($stackCount >= Database::RELATION_MAX_DEPTH - 1 && $this->relationshipWriteStack[$stackCount - 1] !== $relatedCollection->getId()) {
                $document->removeAttribute($key);
                continue;
            }

            $this->relationshipWriteStack[] = $collection->getId();

            try {
                switch ($relationType) {
                    case Database::RELATION_ONE_TO_ONE:
                        if (!$twoWay) {
                            if (\is_string($value)) {
                                $related = $this->getDocument($relatedCollection->getId(), $value);
                                if ($related->isEmpty()) {
                                    // If no such document exists in related collection
                                    // For one-one we need to update the related key to null if no relation exists
                                    $document->setAttribute($key, null);
                                }
                            } elseif ($value instanceof Document) {
                                $relationId = $this->relateDocuments(
                                    $collection,
                                    $relatedCollection,
                                    $key,
                                    $document,
                                    $value,
                                    $relationType,
                                    false,
                                    $twoWayKey,
                                    $side,
                                );
                                $document->setAttribute($key, $relationId);
                            }
                            break;
                        }

                        switch (\gettype($value)) {
                            case 'string':
                                $related = $this->skipRelationships(fn () => $this->getDocument($relatedCollection->getId(), $value));
                                if ($related->isEmpty()) {
                                    // If no such document exists in related collection
                                    // For one-one we need to update the related key to null if no relation exists
                                    $document->setAttribute($key, null);
                                    break;
                                }
                                if (
                                    $oldValue?->getId() !== $value
                                    && $this->skipRelationships(fn () => $this->findOne($relatedCollection->getId(), [
                                        Query::equal($twoWayKey, [$value]),
                                    ]))
                                ) {
                                    // Have to do this here because otherwise relations would be updated before the database can throw the unique violation
                                    throw new DuplicateException('Document already has a related document');
                                }

                                $this->skipRelationships(fn () => $this->updateDocument(
                                    $relatedCollection->getId(),
                                    $related->getId(),
                                    $related->setAttribute($twoWayKey, $document->getId())
                                ));
                                break;
                            case 'object':
                                if ($value instanceof Document) {
                                    $related = $this->skipRelationships(fn () => $this->getDocument($relatedCollection->getId(), $value->getId()));

                                    if (
                                        $oldValue?->getId() !== $value->getId()
                                        && $this->skipRelationships(fn () => $this->findOne($relatedCollection->getId(), [
                                            Query::equal($twoWayKey, [$value->getId()]),
                                        ]))
                                    ) {
                                        // Have to do this here because otherwise relations would be updated before the database can throw the unique violation
                                        throw new DuplicateException('Document already has a related document');
                                    }

                                    $this->relationshipWriteStack[] = $relatedCollection->getId();
                                    if ($related->isEmpty()) {
                                        if (!isset($value['$permissions'])) {
                                            $value->setAttribute('$permissions', $document->getAttribute('$permissions'));
                                        }
                                        $related = $this->createDocument(
                                            $relatedCollection->getId(),
                                            $value->setAttribute($twoWayKey, $document->getId())
                                        );
                                    } else {
                                        $related = $this->updateDocument(
                                            $relatedCollection->getId(),
                                            $related->getId(),
                                            $value->setAttribute($twoWayKey, $document->getId())
                                        );
                                    }
                                    \array_pop($this->relationshipWriteStack);

                                    $document->setAttribute($key, $related->getId());
                                    break;
                                }
                                // no break
                            case 'NULL':
                                if (!\is_null($oldValue?->getId())) {
                                    $oldRelated = $this->skipRelationships(
                                        fn () =>
                                        $this->getDocument($relatedCollection->getId(), $oldValue->getId())
                                    );
                                    $this->skipRelationships(fn () => $this->updateDocument(
                                        $relatedCollection->getId(),
                                        $oldRelated->getId(),
                                        $oldRelated->setAttribute($twoWayKey, null)
                                    ));
                                }
                                break;
                            default:
                                throw new DatabaseException('Invalid type for relationship. Must be either a document, document ID or null.');
                        }
                        break;
                    case Database::RELATION_ONE_TO_MANY:
                    case Database::RELATION_MANY_TO_ONE:
                        if (
                            ($relationType === Database::RELATION_ONE_TO_MANY && $side === Database::RELATION_SIDE_PARENT)
                            || ($relationType === Database::RELATION_MANY_TO_ONE && $side === Database::RELATION_SIDE_CHILD)
                        ) {
                            if (\is_null($value)) {
                                break;
                            }

                            if (!\is_array($value)) {
                                throw new DatabaseException('Invalid value for relationship');
                            }

                            $oldIds = \array_map(fn ($document) => $document->getId(), $oldValue);

                            $newIds = \array_map(function ($item) {
                                if (\is_string($item)) {
                                    return $item;
                                } elseif ($item instanceof Document) {
                                    return $item->getId();
                                } else {
                                    throw new DatabaseException('Invalid value for relationship');
                                }
                            }, $value);

                            $removedDocuments = \array_diff($oldIds, $newIds);

                            foreach ($removedDocuments as $relation) {
                                $relation = Authorization::skip(fn () => $this->skipRelationships(fn () => $this->getDocument(
                                    $relatedCollection->getId(),
                                    $relation
                                )));

                                Authorization::skip(fn () => $this->skipRelationships(fn () => $this->updateDocument(
                                    $relatedCollection->getId(),
                                    $relation->getId(),
                                    $relation->setAttribute($twoWayKey, null)
                                )));
                            }

                            foreach ($value as $relation) {
                                if (\is_string($relation)) {
                                    $related = $this->skipRelationships(
                                        fn () =>
                                        $this->getDocument($relatedCollection->getId(), $relation)
                                    );

                                    if ($related->isEmpty()) {
                                        continue;
                                    }

                                    $this->skipRelationships(fn () => $this->updateDocument(
                                        $relatedCollection->getId(),
                                        $related->getId(),
                                        $related->setAttribute($twoWayKey, $document->getId())
                                    ));
                                } elseif ($relation instanceof Document) {
                                    $related = $this->getDocument($relatedCollection->getId(), $relation->getId());

                                    if ($related->isEmpty()) {
                                        if (!isset($value['$permissions'])) {
                                            $relation->setAttribute('$permissions', $document->getAttribute('$permissions'));
                                        }
                                        $this->createDocument(
                                            $relatedCollection->getId(),
                                            $relation->setAttribute($twoWayKey, $document->getId())
                                        );
                                    } else {
                                        $this->updateDocument(
                                            $relatedCollection->getId(),
                                            $related->getId(),
                                            $relation->setAttribute($twoWayKey, $document->getId())
                                        );
                                    }
                                } else {
                                    throw new DatabaseException('Invalid value for relationship');
                                }
                            }

                            $document->removeAttribute($key);
                            break;
                        }

                        if (\is_string($value)) {
                            $related = $this->getDocument($relatedCollection->getId(), $value);
                            if ($related->isEmpty()) {
                                // If no such document exists in related collection
                                // For many-one we need to update the related key to null if no relation exists
                                $document->setAttribute($key, null);
                            }
                            $this->purgeCachedDocument($relatedCollection->getId(), $value);
                        } elseif ($value instanceof Document) {
                            $related = $this->getDocument($relatedCollection->getId(), $value->getId());

                            if ($related->isEmpty()) {
                                if (!isset($value['$permissions'])) {
                                    $value->setAttribute('$permissions', $document->getAttribute('$permissions'));
                                }
                                $this->createDocument(
                                    $relatedCollection->getId(),
                                    $value
                                );
                            } elseif ($related->getAttributes() != $value->getAttributes()) {
                                $this->updateDocument(
                                    $relatedCollection->getId(),
                                    $related->getId(),
                                    $value
                                );
                                $this->purgeCachedDocument($relatedCollection->getId(), $related->getId());
                            }

                            $document->setAttribute($key, $value->getId());
                        } elseif (\is_null($value)) {
                            break;
                        } elseif (empty($value)) {
                            throw new DatabaseException('Invalid value for relationship');

                        } else {
                            throw new DatabaseException('Invalid value for relationship');
                        }

                        break;
                    case Database::RELATION_MANY_TO_MANY:
                        if (\is_null($value)) {
                            break;
                        }
                        if (!\is_array($value)) {
                            throw new DatabaseException('Invalid value for relationship');
                        }

                        $oldIds = \array_map(fn ($document) => $document->getId(), $oldValue);

                        $newIds = \array_map(function ($item) {
                            if (\is_string($item)) {
                                return $item;
                            } elseif ($item instanceof Document) {
                                return $item->getId();
                            } else {
                                throw new DatabaseException('Invalid value for relationship');
                            }
                        }, $value);

                        $removedDocuments = \array_diff($oldIds, $newIds);

                        foreach ($removedDocuments as $relation) {
                            $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                            $junctions = $this->find($junction, [
                                Query::equal($key, [$relation]),
                                Query::equal($twoWayKey, [$document->getId()]),
                                Query::limit(PHP_INT_MAX)
                            ]);

                            foreach ($junctions as $junction) {
                                Authorization::skip(fn () => $this->deleteDocument($junction->getCollection(), $junction->getId()));
                            }
                        }

                        foreach ($value as $relation) {
                            if (\is_string($relation)) {
                                if (\in_array($relation, $oldIds) || $this->getDocument($relatedCollection->getId(), $relation)->isEmpty()) {
                                    continue;
                                }
                            } elseif ($relation instanceof Document) {
                                $related = $this->getDocument($relatedCollection->getId(), $relation->getId());

                                if ($related->isEmpty()) {
                                    if (!isset($value['$permissions'])) {
                                        $relation->setAttribute('$permissions', $document->getAttribute('$permissions'));
                                    }
                                    $related = $this->createDocument(
                                        $relatedCollection->getId(),
                                        $relation
                                    );
                                } elseif ($related->getAttributes() != $relation->getAttributes()) {
                                    $related = $this->updateDocument(
                                        $relatedCollection->getId(),
                                        $related->getId(),
                                        $relation
                                    );
                                }

                                if (\in_array($relation->getId(), $oldIds)) {
                                    continue;
                                }

                                $relation = $related->getId();
                            } else {
                                throw new DatabaseException('Invalid value for relationship');
                            }

                            $this->skipRelationships(fn () => $this->createDocument(
                                $this->getJunctionCollection($collection, $relatedCollection, $side),
                                new Document([
                                    $key => $relation,
                                    $twoWayKey => $document->getId(),
                                    '$permissions' => [
                                        Permission::read(Role::any()),
                                        Permission::update(Role::any()),
                                        Permission::delete(Role::any()),
                                    ],
                                ])
                            ));
                        }

                        $document->removeAttribute($key);
                        break;
                }
            } finally {
                \array_pop($this->relationshipWriteStack);
            }
        }

        return $document;
    }

    private function getJunctionCollection(Document $collection, Document $relatedCollection, string $side): string
    {
        return $side === Database::RELATION_SIDE_PARENT
            ? '_' . $collection->getInternalId() . '_' . $relatedCollection->getInternalId()
            : '_' . $relatedCollection->getInternalId() . '_' . $collection->getInternalId();
    }

    /**
     * Increase a document attribute by a value
     *
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param int|float|null $max
     * @return bool
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     */
    public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value = 1, int|float|null $max = null): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if ($value <= 0) { // Can be a float
            throw new DatabaseException('Value must be numeric and greater than 0');
        }

        $validator = new Authorization(self::PERMISSION_UPDATE);

        $document = Authorization::skip(fn () => $this->silent(fn () => $this->getDocument($collection, $id))); // Skip ensures user does not need read permission for this

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() !== self::METADATA) {
            $documentSecurity = $collection->getAttribute('documentSecurity', false);
            if (!$validator->isValid([
                ...$collection->getUpdate(),
                ...($documentSecurity ? $document->getUpdate() : [])
            ])) {
                throw new AuthorizationException($validator->getDescription());
            }
        }

        $attr = \array_filter($collection->getAttribute('attributes', []), function ($a) use ($attribute) {
            return $a['$id'] === $attribute;
        });

        if (empty($attr)) {
            throw new DatabaseException('Attribute not found');
        }

        $whiteList = [self::VAR_INTEGER, self::VAR_FLOAT];

        /**
         * @var Document $attr
         */
        $attr = end($attr);
        if (!in_array($attr->getAttribute('type'), $whiteList)) {
            throw new DatabaseException('Attribute type must be one of: ' . implode(',', $whiteList));
        }

        if ($max && ($document->getAttribute($attribute) + $value > $max)) {
            throw new DatabaseException('Attribute value exceeds maximum limit: ' . $max);
        }

        $max = $max ? $max - $value : null;
        $result = $this->adapter->increaseDocumentAttribute($collection->getId(), $id, $attribute, $value, null, $max);

        $this->purgeCachedDocument($collection->getId(), $id);

        $this->trigger(self::EVENT_DOCUMENT_INCREASE, $document);

        return $result;
    }


    /**
     * Decrease a document attribute by a value
     *
     * @param string $collection
     * @param string $id
     * @param string $attribute
     * @param int|float $value
     * @param int|float|null $min
     * @return bool
     *
     * @throws AuthorizationException
     * @throws DatabaseException
     */
    public function decreaseDocumentAttribute(string $collection, string $id, string $attribute, int|float $value = 1, int|float|null $min = null): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        if ($value <= 0) { // Can be a float
            throw new DatabaseException('Value must be numeric and greater than 0');
        }

        $validator = new Authorization(self::PERMISSION_UPDATE);

        $document = Authorization::skip(fn () => $this->silent(fn () => $this->getDocument($collection, $id))); // Skip ensures user does not need read permission for this

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->getId() !== self::METADATA) {
            $documentSecurity = $collection->getAttribute('documentSecurity', false);
            if (!$validator->isValid([
                ...$collection->getUpdate(),
                ...($documentSecurity ? $document->getUpdate() : [])
            ])) {
                throw new AuthorizationException($validator->getDescription());
            }
        }

        $attr = \array_filter($collection->getAttribute('attributes', []), function ($a) use ($attribute) {
            return $a['$id'] === $attribute;
        });

        if (empty($attr)) {
            throw new DatabaseException('Attribute not found');
        }

        $whiteList = [self::VAR_INTEGER, self::VAR_FLOAT];

        /**
         * @var Document $attr
         */
        $attr = end($attr);
        if (!in_array($attr->getAttribute('type'), $whiteList)) {
            throw new DatabaseException('Attribute type must be one of: ' . implode(',', $whiteList));
        }

        if ($min && ($document->getAttribute($attribute) - $value < $min)) {
            throw new DatabaseException('Attribute value Exceeds minimum limit ' . $min);
        }

        $min = $min ? $min + $value : null;

        $result = $this->adapter->increaseDocumentAttribute($collection->getId(), $id, $attribute, $value * -1, $min);

        $this->purgeCachedDocument($collection->getId(), $id);

        $this->trigger(self::EVENT_DOCUMENT_DECREASE, $document);

        return $result;
    }

    /**
     * Delete Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     *
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     * @throws StructureException
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $document = Authorization::skip(fn () => $this->silent(fn () => $this->getDocument($collection, $id)));

        $collection = $this->silent(fn () => $this->getCollection($collection));

        $validator = new Authorization(self::PERMISSION_DELETE);

        if ($collection->getId() !== self::METADATA) {
            $documentSecurity = $collection->getAttribute('documentSecurity', false);
            if (!$validator->isValid([
                ...$collection->getDelete(),
                ...($documentSecurity ? $document->getDelete() : [])
            ])) {
                throw new AuthorizationException($validator->getDescription());
            }
        }

        // Check if document was updated after the request timestamp
        try {
            $oldUpdatedAt = new \DateTime($document->getUpdatedAt());
        } catch (Exception $e) {
            throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
        }

        if (!is_null($this->timestamp) && $oldUpdatedAt > $this->timestamp) {
            throw new ConflictException('Document was updated after the request timestamp');
        }

        if ($this->resolveRelationships) {
            $document = $this->silent(fn () => $this->deleteDocumentRelationships($collection, $document));
        }

        $deleted = $this->adapter->deleteDocument($collection->getId(), $id);

        $this->purgeRelatedDocuments($collection, $id);
        $this->purgeCachedDocument($collection->getId(), $id);

        $this->trigger(self::EVENT_DOCUMENT_DELETE, $document);

        return $deleted;
    }

    /**
     * @param Document $collection
     * @param Document $document
     * @return Document
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     * @throws StructureException
     */
    private function deleteDocumentRelationships(Document $collection, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);

        $relationships = \array_filter($attributes, function ($attribute) {
            return $attribute['type'] === Database::VAR_RELATIONSHIP;
        });

        foreach ($relationships as $relationship) {
            $key = $relationship['key'];
            $value = $document->getAttribute($key);
            $relatedCollection = $this->getCollection($relationship['options']['relatedCollection']);
            $relationType = $relationship['options']['relationType'];
            $twoWay = $relationship['options']['twoWay'];
            $twoWayKey = $relationship['options']['twoWayKey'];
            $onDelete = $relationship['options']['onDelete'];
            $side = $relationship['options']['side'];

            $relationship->setAttribute('collection', $collection->getId());
            $relationship->setAttribute('document', $document->getId());

            switch ($onDelete) {
                case Database::RELATION_MUTATE_RESTRICT:
                    $this->deleteRestrict($relatedCollection, $document, $value, $relationType, $twoWay, $twoWayKey, $side);
                    break;
                case Database::RELATION_MUTATE_SET_NULL:
                    $this->deleteSetNull($collection, $relatedCollection, $document, $value, $relationType, $twoWay, $twoWayKey, $side);
                    break;
                case Database::RELATION_MUTATE_CASCADE:
                    foreach ($this->relationshipDeleteStack as $processedRelationship) {
                        $existingKey = $processedRelationship['key'];
                        $existingCollection = $processedRelationship['collection'];
                        $existingRelatedCollection = $processedRelationship['options']['relatedCollection'];
                        $existingTwoWayKey = $processedRelationship['options']['twoWayKey'];
                        $existingSide = $processedRelationship['options']['side'];

                        // If this relationship has already been fetched for this document, skip it
                        $reflexive = $processedRelationship == $relationship;

                        // If this relationship is the same as a previously fetched relationship, but on the other side, skip it
                        $symmetric = $existingKey === $twoWayKey
                            && $existingTwoWayKey === $key
                            && $existingRelatedCollection === $collection->getId()
                            && $existingCollection === $relatedCollection->getId()
                            && $existingSide !== $side;

                        // If this relationship is not directly related but relates across multiple collections, skip it.
                        //
                        // These conditions ensure that a relationship is considered transitive if it has the same
                        // two-way key and related collection, but is on the opposite side of the relationship (the first and second conditions).
                        //
                        // They also ensure that a relationship is considered transitive if it has the same key and related
                        // collection as an existing relationship, but a different two-way key (the third condition),
                        // or the same two-way key as an existing relationship, but a different key (the fourth condition).
                        $transitive = (($existingKey === $twoWayKey
                            && $existingCollection === $relatedCollection->getId()
                            && $existingSide !== $side)
                            || ($existingTwoWayKey === $key
                                && $existingRelatedCollection === $collection->getId()
                                && $existingSide !== $side)
                            || ($existingKey === $key
                                && $existingTwoWayKey !== $twoWayKey
                                && $existingRelatedCollection === $relatedCollection->getId()
                                && $existingSide !== $side)
                            || ($existingKey !== $key
                                && $existingTwoWayKey === $twoWayKey
                                && $existingRelatedCollection === $relatedCollection->getId()
                                && $existingSide !== $side));

                        if ($reflexive || $symmetric || $transitive) {
                            break 2;
                        }
                    }
                    $this->deleteCascade($collection, $relatedCollection, $document, $key, $value, $relationType, $twoWayKey, $side, $relationship);
                    break;
            }
        }

        return $document;
    }

    /**
     * @param Document $relatedCollection
     * @param Document $document
     * @param mixed $value
     * @param string $relationType
     * @param bool $twoWay
     * @param string $twoWayKey
     * @param string $side
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     * @throws StructureException
     */
    private function deleteRestrict(
        Document $relatedCollection,
        Document $document,
        mixed $value,
        string $relationType,
        bool $twoWay,
        string $twoWayKey,
        string $side
    ): void {
        if ($value instanceof Document && $value->isEmpty()) {
            $value = null;
        }

        if (
            !empty($value)
            && $relationType !== Database::RELATION_MANY_TO_ONE
            && $side === Database::RELATION_SIDE_PARENT
        ) {
            throw new RestrictedException('Cannot delete document because it has at least one related document.');
        }

        if (
            $relationType === Database::RELATION_ONE_TO_ONE
            && $side === Database::RELATION_SIDE_CHILD
            && !$twoWay
        ) {
            Authorization::skip(function () use ($document, $relatedCollection, $twoWayKey) {
                $related = $this->findOne($relatedCollection->getId(), [
                    Query::equal($twoWayKey, [$document->getId()])
                ]);

                if (!$related instanceof Document) {
                    return;
                }


                $this->skipRelationships(fn () => $this->updateDocument(
                    $relatedCollection->getId(),
                    $related->getId(),
                    $related->setAttribute($twoWayKey, null)
                ));
            });
        }

        if (
            $relationType === Database::RELATION_MANY_TO_ONE
            && $side === Database::RELATION_SIDE_CHILD
        ) {
            $related = Authorization::skip(fn () => $this->findOne($relatedCollection->getId(), [
                Query::equal($twoWayKey, [$document->getId()])
            ]));

            if ($related) {
                throw new RestrictedException('Cannot delete document because it has at least one related document.');
            }
        }
    }

    /**
     * @param Document $collection
     * @param Document $relatedCollection
     * @param Document $document
     * @param mixed $value
     * @param string $relationType
     * @param bool $twoWay
     * @param string $twoWayKey
     * @param string $side
     * @return void
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     * @throws StructureException
     */
    private function deleteSetNull(Document $collection, Document $relatedCollection, Document $document, mixed $value, string $relationType, bool $twoWay, string $twoWayKey, string $side): void
    {
        switch ($relationType) {
            case Database::RELATION_ONE_TO_ONE:
                if (!$twoWay && $side === Database::RELATION_SIDE_PARENT) {
                    break;
                }

                // Shouldn't need read or update permission to delete
                Authorization::skip(function () use ($document, $value, $relatedCollection, $twoWay, $twoWayKey, $side) {
                    if (!$twoWay && $side === Database::RELATION_SIDE_CHILD) {
                        $related = $this->findOne($relatedCollection->getId(), [
                            Query::equal($twoWayKey, [$document->getId()])
                        ]);
                    } else {
                        if (empty($value)) {
                            return;
                        }
                        $related = $this->getDocument($relatedCollection->getId(), $value->getId());
                    }

                    if (!$related instanceof Document) {
                        return;
                    }

                    $this->skipRelationships(fn () => $this->updateDocument(
                        $relatedCollection->getId(),
                        $related->getId(),
                        $related->setAttribute($twoWayKey, null)
                    ));
                });
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    break;
                }
                foreach ($value as $relation) {
                    Authorization::skip(function () use ($relatedCollection, $twoWayKey, $relation) {
                        $related = $this->getDocument($relatedCollection->getId(), $relation->getId());

                        $this->skipRelationships(fn () => $this->updateDocument(
                            $relatedCollection->getId(),
                            $related->getId(),
                            $related->setAttribute($twoWayKey, null)
                        ));
                    });
                }
                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    break;
                }

                if (!$twoWay) {
                    $value = $this->find($relatedCollection->getId(), [
                        Query::equal($twoWayKey, [$document->getId()]),
                        Query::limit(PHP_INT_MAX)
                    ]);
                }

                foreach ($value as $relation) {
                    Authorization::skip(function () use ($relatedCollection, $twoWayKey, $relation) {
                        $related = $this->getDocument($relatedCollection->getId(), $relation->getId());

                        $this->skipRelationships(fn () => $this->updateDocument(
                            $relatedCollection->getId(),
                            $related->getId(),
                            $related->setAttribute($twoWayKey, null)
                        ));
                    });
                }

                break;
            case Database::RELATION_MANY_TO_MANY:
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $junctions = $this->find($junction, [
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX)
                ]);

                foreach ($junctions as $document) {
                    $this->skipRelationships(fn () => $this->deleteDocument(
                        $junction,
                        $document->getId()
                    ));
                }
                break;
        }
    }

    /**
     * @param Document $collection
     * @param Document $relatedCollection
     * @param Document $document
     * @param string $key
     * @param mixed $value
     * @param string $relationType
     * @param string $twoWayKey
     * @param string $side
     * @param Document $relationship
     * @return void
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws DatabaseException
     * @throws RestrictedException
     * @throws StructureException
     */
    private function deleteCascade(Document $collection, Document $relatedCollection, Document $document, string $key, mixed $value, string $relationType, string $twoWayKey, string $side, Document $relationship): void
    {
        switch ($relationType) {
            case Database::RELATION_ONE_TO_ONE:
                if ($value !== null) {
                    $this->relationshipDeleteStack[] = $relationship;

                    $this->deleteDocument(
                        $relatedCollection->getId(),
                        $value->getId()
                    );

                    \array_pop($this->relationshipDeleteStack);
                }
                break;
            case Database::RELATION_ONE_TO_MANY:
                if ($side === Database::RELATION_SIDE_CHILD) {
                    break;
                }

                $this->relationshipDeleteStack[] = $relationship;

                foreach ($value as $relation) {
                    $this->deleteDocument(
                        $relatedCollection->getId(),
                        $relation->getId()
                    );
                }

                \array_pop($this->relationshipDeleteStack);

                break;
            case Database::RELATION_MANY_TO_ONE:
                if ($side === Database::RELATION_SIDE_PARENT) {
                    break;
                }

                $value = $this->find($relatedCollection->getId(), [
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX)
                ]);

                $this->relationshipDeleteStack[] = $relationship;

                foreach ($value as $relation) {
                    $this->deleteDocument(
                        $relatedCollection->getId(),
                        $relation->getId()
                    );
                }

                \array_pop($this->relationshipDeleteStack);

                break;
            case Database::RELATION_MANY_TO_MANY:
                $junction = $this->getJunctionCollection($collection, $relatedCollection, $side);

                $junctions = $this->find($junction, [
                    Query::equal($twoWayKey, [$document->getId()]),
                    Query::limit(PHP_INT_MAX)
                ]);

                $this->relationshipDeleteStack[] = $relationship;

                foreach ($junctions as $document) {
                    if ($side === Database::RELATION_SIDE_PARENT) {
                        $this->deleteDocument(
                            $relatedCollection->getId(),
                            $document->getAttribute($key)
                        );
                    }
                    $this->deleteDocument(
                        $junction,
                        $document->getId()
                    );
                }

                \array_pop($this->relationshipDeleteStack);
                break;
        }
    }

    /**
     * Cleans the all the collection's documents from the cache
     *
     * @param string $collection
     *
     * @return bool
     * @throws DatabaseException
     */
    public function purgeCachedCollection(string $collection): bool
    {
        return $this->cache->purge($this->cacheName . '-cache-' . $this->getNamespace() . ':' . $this->adapter->getTenant() . ':' . $collection . ':*');
    }

    /**
     * Cleans a specific document from cache
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     * @throws DatabaseException
     */
    public function purgeCachedDocument(string $collection, string $id): bool
    {
        return $this->cache->purge($this->cacheName . '-cache-' . $this->getNamespace() . ':' . $this->adapter->getTenant() . ':' . $collection . ':' . $id . ':*');
    }

    /**
     * Find Documents
     *
     * @param string $collection
     * @param array<Query> $queries
     *
     * @return array<Document>
     * @throws DatabaseException
     * @throws QueryException
     * @throws TimeoutException
     */
    public function find(string $collection, array $queries = []): array
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));

        if ($collection->isEmpty()) {
            throw new DatabaseException('Collection not found');
        }

        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        if ($this->validate) {
            $validator = new DocumentsValidator($attributes, $indexes);
            if (!$validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $authorization = new Authorization(self::PERMISSION_READ);
        $documentSecurity = $collection->getAttribute('documentSecurity', false);
        $skipAuth = $authorization->isValid($collection->getRead());

        if (!$skipAuth && !$documentSecurity && $collection->getId() !== self::METADATA) {
            throw new AuthorizationException($authorization->getDescription());
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn (Document $attribute) => $attribute->getAttribute('type') === self::VAR_RELATIONSHIP
        );

        $grouped = Query::groupByType($queries);
        $filters = $grouped['filters'];
        $selects = $grouped['selections'];
        $limit = $grouped['limit'];
        $offset = $grouped['offset'];
        $orderAttributes = $grouped['orderAttributes'];
        $orderTypes = $grouped['orderTypes'];
        $cursor = $grouped['cursor'];
        $cursorDirection = $grouped['cursorDirection'];

        if (!empty($cursor) && $cursor->getCollection() !== $collection->getId()) {
            throw new DatabaseException("cursor Document must be from the same Collection.");
        }

        $cursor = empty($cursor) ? [] : $this->encode($collection, $cursor)->getArrayCopy();

        /**  @var array<Query> $queries */
        $queries = \array_merge(
            $selects,
            self::convertQueries($collection, $filters)
        );

        $selections = $this->validateSelections($collection, $selects);
        $nestedSelections = [];

        foreach ($queries as $index => &$query) {
            switch ($query->getMethod()) {
                case Query::TYPE_SELECT:
                    $values = $query->getValues();
                    foreach ($values as $valueIndex => $value) {
                        if (\str_contains($value, '.')) {
                            // Shift the top level off the dot-path to pass the selection down the chain
                            // 'foo.bar.baz' becomes 'bar.baz'
                            $nestedSelections[] = Query::select([
                                \implode('.', \array_slice(\explode('.', $value), 1))
                            ]);

                            $key = \explode('.', $value)[0];

                            foreach ($relationships as $relationship) {
                                if ($relationship->getAttribute('key') === $key) {
                                    switch ($relationship->getAttribute('options')['relationType']) {
                                        case Database::RELATION_MANY_TO_MANY:
                                        case Database::RELATION_ONE_TO_MANY:
                                            unset($values[$valueIndex]);
                                            break;

                                        case Database::RELATION_MANY_TO_ONE:
                                        case Database::RELATION_ONE_TO_ONE:
                                            $values[$valueIndex] = $key;
                                            break;
                                    }
                                }
                            }
                        }
                    }
                    $query->setValues(\array_values($values));
                    break;
                default:
                    if (\str_contains($query->getAttribute(), '.')) {
                        unset($queries[$index]);
                    }
                    break;
            }
        }

        $queries = \array_values($queries);

        $getResults = fn () => $this->adapter->find(
            $collection->getId(),
            $queries,
            $limit ?? 25,
            $offset ?? 0,
            $orderAttributes,
            $orderTypes,
            $cursor,
            $cursorDirection ?? Database::CURSOR_AFTER
        );

        $results = $skipAuth ? Authorization::skip($getResults) : $getResults();

        foreach ($results as  &$node) {
            if ($this->resolveRelationships && (empty($selects) || !empty($nestedSelections))) {
                $node = $this->silent(fn () => $this->populateDocumentRelationships($collection, $node, $nestedSelections));
            }
            $node = $this->casting($collection, $node);
            $node = $this->decode($collection, $node, $selections);

            if (!$node->isEmpty()) {
                $node->setAttribute('$collection', $collection->getId());
            }
        }

        unset($query);

        // Remove internal attributes which are not queried
        foreach ($queries as $query) {
            if ($query->getMethod() === Query::TYPE_SELECT) {
                $values = $query->getValues();
                foreach ($results as $result) {
                    foreach (Database::INTERNAL_ATTRIBUTES as $internalAttribute) {
                        if (!\in_array($internalAttribute['$id'], $values)) {
                            $result->removeAttribute($internalAttribute['$id']);
                        }
                    }
                }
            }
        }

        $this->trigger(self::EVENT_DOCUMENT_FIND, $results);

        return $results;
    }

    /**
     * @param string $collection
     * @param array<Query> $queries
     * @return false|Document
     * @throws DatabaseException
     */
    public function findOne(string $collection, array $queries = []): false|Document
    {
        $results = $this->silent(fn () => $this->find($collection, \array_merge([
            Query::limit(1)
        ], $queries)));

        $found = \reset($results);

        $this->trigger(self::EVENT_DOCUMENT_FIND, $found);

        return $found;
    }

    /**
     * Count Documents
     *
     * Count the number of documents.
     *
     * @param string $collection
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int
     * @throws DatabaseException
     */
    public function count(string $collection, array $queries = [], ?int $max = null): int
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        if ($this->validate) {
            $validator = new DocumentsValidator($attributes, $indexes);
            if (!$validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $authorization = new Authorization(self::PERMISSION_READ);
        if ($authorization->isValid($collection->getRead())) {
            $skipAuth = true;
        }

        $queries = Query::groupByType($queries)['filters'];
        $queries = self::convertQueries($collection, $queries);

        $getCount = fn () => $this->adapter->count($collection->getId(), $queries, $max);
        $count = $skipAuth ?? false ? Authorization::skip($getCount) : $getCount();

        $this->trigger(self::EVENT_DOCUMENT_COUNT, $count);

        return $count;
    }

    /**
     * Sum an attribute
     *
     * Sum an attribute for all the documents. Pass $max=0 for unlimited count
     *
     * @param string $collection
     * @param string $attribute
     * @param array<Query> $queries
     * @param int|null $max
     *
     * @return int|float
     * @throws DatabaseException
     */
    public function sum(string $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        if ($this->adapter->getShareTables() && empty($this->adapter->getTenant())) {
            throw new DatabaseException('Missing tenant. Tenant must be set when table sharing is enabled.');
        }

        $collection = $this->silent(fn () => $this->getCollection($collection));
        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        if ($this->validate) {
            $validator = new DocumentsValidator($attributes, $indexes);
            if (!$validator->isValid($queries)) {
                throw new QueryException($validator->getDescription());
            }
        }

        $queries = self::convertQueries($collection, $queries);

        $sum = $this->adapter->sum($collection->getId(), $attribute, $queries, $max);

        $this->trigger(self::EVENT_DOCUMENT_SUM, $sum);

        return $sum;
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
     *
     * @return Document
     * @throws DatabaseException
     */
    public function encode(Document $collection, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);

        $internalAttributes = \array_filter(Database::INTERNAL_ATTRIBUTES, function ($attribute) {
            // We don't want to encode permissions into a JSON string
            return $attribute['$id'] !== '$permissions';
        });

        $attributes = \array_merge($attributes, $internalAttributes);

        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? '';
            $array = $attribute['array'] ?? false;
            $default = $attribute['default'] ?? null;
            $filters = $attribute['filters'] ?? [];
            $value = $document->getAttribute($key);

            // continue on optional param with no default
            if (is_null($value) && is_null($default)) {
                continue;
            }

            // assign default only if no value provided
            // False positive "Call to function is_null() with mixed will always evaluate to false"
            // @phpstan-ignore-next-line
            if (is_null($value) && !is_null($default)) {
                $value = ($array) ? $default : [$default];
            } else {
                $value = ($array) ? $value : [$value];
            }

            foreach ($value as &$node) {
                if (($node !== null)) {
                    foreach ($filters as $filter) {
                        $node = $this->encodeAttribute($filter, $node, $document);
                    }
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
            fn ($attribute) =>
            $attribute['type'] !== self::VAR_RELATIONSHIP
        );

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn ($attribute) =>
            $attribute['type'] === self::VAR_RELATIONSHIP
        );

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

        $attributes = array_merge($attributes, Database::INTERNAL_ATTRIBUTES);

        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? '';
            $array = $attribute['array'] ?? false;
            $filters = $attribute['filters'] ?? [];
            $value = $document->getAttribute($key);

            if (\is_null($value)) {
                $value = $document->getAttribute($this->adapter->filter($key));

                if (!\is_null($value)) {
                    $document->removeAttribute($this->adapter->filter($key));
                }
            }

            $value = ($array) ? $value : [$value];
            $value = (is_null($value)) ? [] : $value;

            foreach ($value as &$node) {
                foreach (array_reverse($filters) as $filter) {
                    $node = $this->decodeAttribute($filter, $node, $document);
                }
            }

            if (empty($selections) || \in_array($key, $selections) || \in_array('*', $selections)) {
                if (
                    empty($selections)
                    || \in_array($key, $selections)
                    || \in_array('*', $selections)
                    || \in_array($key, ['$createdAt', '$updatedAt'])
                ) {
                    // Prevent null values being set for createdAt and updatedAt
                    if (\in_array($key, ['$createdAt', '$updatedAt']) && $value[0] === null) {
                        continue;
                    } else {
                        $document->setAttribute($key, ($array) ? $value : $value[0]);
                    }
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
        if ($this->adapter->getSupportForCasting()) {
            return $document;
        }

        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? '';
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;
            $value = $document->getAttribute($key, null);
            if (is_null($value)) {
                continue;
            }

            if ($array) {
                $value = !is_string($value)
                    ? $value
                    : json_decode($value, true);
            } else {
                $value = [$value];
            }

            foreach ($value as &$node) {
                switch ($type) {
                    case self::VAR_BOOLEAN:
                        $node = (bool)$node;
                        break;
                    case self::VAR_INTEGER:
                        $node = (int)$node;
                        break;
                    case self::VAR_FLOAT:
                        $node = (float)$node;
                        break;
                    default:
                        break;
                }
            }

            $document->setAttribute($key, ($array) ? $value : $value[0]);
        }

        return $document;
    }

    /**
     * Encode Attribute
     *
     * Passes the attribute $value, and $document context to a predefined filter
     *  that allow you to manipulate the input format of the given attribute.
     *
     * @param string $name
     * @param mixed $value
     * @param Document $document
     *
     * @return mixed
     * @throws DatabaseException
     */
    protected function encodeAttribute(string $name, $value, Document $document): mixed
    {
        if (!array_key_exists($name, self::$filters) && !array_key_exists($name, $this->instanceFilters)) {
            throw new DatabaseException("Filter: {$name} not found");
        }

        try {
            if (array_key_exists($name, $this->instanceFilters)) {
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
     * @param string $name
     * @param mixed $value
     * @param Document $document
     *
     * @return mixed
     * @throws DatabaseException
     */
    protected function decodeAttribute(string $name, mixed $value, Document $document): mixed
    {
        if (!$this->filter) {
            return $value;
        }

        if (!array_key_exists($name, self::$filters) && !array_key_exists($name, $this->instanceFilters)) {
            throw new DatabaseException('Filter not found');
        }

        if (array_key_exists($name, $this->instanceFilters)) {
            $value = $this->instanceFilters[$name]['decode']($value, $document, $this);
        } else {
            $value = self::$filters[$name]['decode']($value, $document, $this);
        }

        return $value;
    }

    /**
     * Validate if a set of attributes can be selected from the collection
     *
     * @param Document $collection
     * @param array<Query> $queries
     * @return array<string>
     * @throws DatabaseException
     */
    private function validateSelections(Document $collection, array $queries): array
    {
        if (empty($queries)) {
            return [];
        }

        $selections = [];
        $relationshipSelections = [];

        foreach ($queries as $query) {
            if ($query->getMethod() == Query::TYPE_SELECT) {
                foreach ($query->getValues() as $value) {
                    if (\str_contains($value, '.')) {
                        $relationshipSelections[] = $value;
                        continue;
                    }
                    $selections[] = $value;
                }
            }
        }

        // Allow querying internal attributes
        $keys = \array_map(
            fn ($attribute) => $attribute['$id'],
            self::INTERNAL_ATTRIBUTES
        );

        foreach ($collection->getAttribute('attributes', []) as $attribute) {
            if ($attribute['type'] !== self::VAR_RELATIONSHIP) {
                // Fallback to $id when key property is not present in metadata table for some tables such as Indexes or Attributes
                $keys[] = $attribute['key'] ?? $attribute['$id'];
            }
        }

        $invalid = \array_diff($selections, $keys);
        if (!empty($invalid) && !\in_array('*', $invalid)) {
            throw new DatabaseException('Cannot select attributes: ' . \implode(', ', $invalid));
        }

        $selections = \array_merge($selections, $relationshipSelections);

        $selections[] = '$id';
        $selections[] = '$internalId';
        $selections[] = '$collection';
        $selections[] = '$createdAt';
        $selections[] = '$updatedAt';
        $selections[] = '$permissions';

        return $selections;
    }

    /**
     * Get adapter attribute limit, accounting for internal metadata
     * Returns 0 to indicate no limit
     *
     * @return int
     */
    public function getLimitForAttributes(): int
    {
        // If negative, return 0
        // -1 ==> virtual columns count as total, so treat as buffer
        return \max($this->adapter->getLimitForAttributes() - $this->adapter->getCountOfDefaultAttributes() - 1, 0);
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
     * @throws DatabaseException
     */
    public static function convertQueries(Document $collection, array $queries): array
    {
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            foreach ($queries as $query) {
                if ($query->getAttribute() === $attribute->getId()) {
                    $query->setOnArray($attribute->getAttribute('array', false));
                }
            }

            if ($attribute->getAttribute('type') == Database::VAR_DATETIME) {
                foreach ($queries as $index => $query) {
                    if ($query->getAttribute() === $attribute->getId()) {
                        $values = $query->getValues();
                        foreach ($values as $valueIndex => $value) {
                            $values[$valueIndex] = DateTime::setTimezone($value);
                        }
                        $query->setValues($values);
                        $queries[$index] = $query;
                    }
                }
            }
        }
        return $queries;
    }

    /**
     * @param Document $collection
     * @param string $id
     * @return void
     * @throws DatabaseException
     */
    private function purgeRelatedDocuments(Document $collection, string $id): void
    {
        if ($collection->getId() === self::METADATA) {
            return;
        }

        $relationships = \array_filter(
            $collection->getAttribute('attributes', []),
            fn ($attribute) =>
            $attribute['type'] === Database::VAR_RELATIONSHIP
        );

        if (empty($relationships)) {
            return;
        }

        $key = $this->cacheName . '-cache-' . $this->getNamespace() . ':map:' . $collection->getId() . ':' . $id;
        $cache = $this->cache->load($key, self::TTL);
        if (!empty($cache)) {
            foreach ($cache as $v) {
                list($collectionId, $documentId) = explode(':', $v);
                $this->purgeCachedDocument($collectionId, $documentId);
            }
            $this->cache->purge($key);
        }
    }
}

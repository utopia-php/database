<?php

namespace Utopia\Database;

use Exception;
use Throwable;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Structure;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Cache\Cache;

class Database
{
    const VAR_STRING = 'string';
    // Simple Types
    const VAR_INTEGER = 'integer';
    const VAR_FLOAT = 'double';
    const VAR_BOOLEAN = 'boolean';
    const VAR_DATETIME = 'datetime';

    // Relationships Types
    const VAR_DOCUMENT = 'document';

    // Index Types
    const INDEX_KEY = 'key';
    const INDEX_FULLTEXT = 'fulltext';
    const INDEX_UNIQUE = 'unique';
    const INDEX_SPATIAL = 'spatial';
    const INDEX_ARRAY = 'array';

    // Orders
    const ORDER_ASC = 'ASC';
    const ORDER_DESC = 'DESC';

    // Permissions
    const PERMISSION_CREATE= 'create';
    const PERMISSION_READ = 'read';
    const PERMISSION_UPDATE = 'update';
    const PERMISSION_DELETE = 'delete';

    const PERMISSIONS = [
        self::PERMISSION_CREATE,
        self::PERMISSION_READ,
        self::PERMISSION_UPDATE,
        self::PERMISSION_DELETE,
    ];

    const ROLES = [
        'any',
        'users',
        'user',
        'team',
        'member',
        'guests',
        'status',
        'role',
    ];

    // Collections
    const METADATA = '_metadata';

    // Cursor
    const CURSOR_BEFORE = 'before';
    const CURSOR_AFTER = 'after';

    // Lengths
    const LENGTH_KEY = 255;

    // Cache
    const TTL = 60 * 60 * 24; // 24 hours

    /**
     * @var Adapter
     */
    protected Adapter $adapter;

    /**
     * @var Cache
     */
    protected Cache $cache;

    /**
     * @var array
     */
    protected array $primitives = [
        self::VAR_STRING => true,
        self::VAR_INTEGER => true,
        self::VAR_FLOAT => true,
        self::VAR_BOOLEAN => true,
    ];

    /**
     * List of Internal Ids
     * @var array
     */
    protected array $attributes = [
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
            '$id' => '$collection',
            'type' => self::VAR_STRING,
            'size' => Database::LENGTH_KEY,
            'required' => true,
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
        ]
    ];

    /**
     * Parent Collection
     * Defines the structure for both system and custom collections
     *
     * @var array
     */
    protected array $collection = [
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
        ],
        'indexes' => [],
    ];

    /**
     * @var array
     */
    static protected array $filters = [];

    /**
     * @var array
     */
    private array $instanceFilters = [];

    /**
     * @param Adapter $adapter
     * @param Cache $cache
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
            function ($value) {
                $value = ($value instanceof Document) ? $value->getArrayCopy() : $value;

                if (!is_array($value) && !$value instanceof \stdClass) {
                    return $value;
                }

                return json_encode($value);
            },
            /**
             * @param mixed $value
             * @return mixed
             */
            function ($value) {
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
                if (is_null($value)) return null;
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
     * Set Namespace.
     *
     * Set namespace to divide different scope of data sets
     *
     * @param string $namespace
     *
     * @return $this
     *
     * @throws Exception
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
     *
     * @throws Exception
     */
    public function getNamespace(): string
    {
        return $this->adapter->getNamespace();
    }

    /**
     * Set database to use for current scope
     *
     * @param string $name
     * @param bool $reset
     *
     * @return bool
     * @throws Exception
     */
    public function setDefaultDatabase(string $name, bool $reset = false): bool
    {
        return $this->adapter->setDefaultDatabase($name, $reset);
    }

    /**
     * Get Database.
     *
     * Get Database from current scope
     *
     * @throws Exception
     *
     * @return string
     */
    public function getDefaultDatabase(): string
    {
        return $this->adapter->getDefaultDatabase();
    }

    /**
     * Create Database
     *
     * @param string $name
     *
     * @return bool
     */
    public function create(string $name): bool
    {
        $this->adapter->create($name);
        $this->setDefaultDatabase($name);
        $this->createMetadata();

        return true;
    }

    /**
     * Create Metadata collection.
     * @return bool
     * @throws LimitException
     * @throws AuthorizationException
     * @throws StructureException
     */
    public function createMetadata(): bool
    {
        /**
         * Create array of attribute documents
         * @var Document[] $attributes
         */
        $attributes = array_map(function ($attribute) {
            return new Document([
                '$id' => ID::custom($attribute[0]),
                'type' => $attribute[1],
                'size' => $attribute[2],
                'required' => $attribute[3],
            ]);
        }, [ // Array of [$id, $type, $size, $required]
            ['name', self::VAR_STRING, 512, true],
            ['attributes', self::VAR_STRING, 1000000, false],
            ['indexes', self::VAR_STRING, 1000000, false],
        ]);

        $this->createCollection(self::METADATA, $attributes);

        return true;
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string $database database name
     * @param string $collection (optional) collection name
     *
     * @return bool
     */
    public function exists(string $database, string $collection = null): bool
    {
        return $this->adapter->exists($database, $collection);
    }

    /**
     * List Databases
     *
     * @return array
     */
    public function list(): array
    {
        return $this->adapter->list();
    }

    /**
     * Delete Database
     *
     * @param string $name
     *
     * @return bool
     */
    public function delete(string $name): bool
    {
        return $this->adapter->delete($name);
    }

    /**
     * Create Collection
     *
     * @param string $id
     * @param Document[] $attributes (optional)
     * @param Document[] $indexes (optional)
     *
     * @return Document
     */
    public function createCollection(string $id, array $attributes = [], array $indexes = []): Document 
    {
        $collection = $this->getCollection($id);
        if (!$collection->isEmpty() && $id !== self::METADATA){
            throw new Duplicate('Collection ' . $id . ' Exists!');
        }

        $this->adapter->createCollection($id, $attributes, $indexes);

        if ($id === self::METADATA) {
            return new Document($this->collection);
        }

        $collection = new Document([
            '$id' => ID::custom($id),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => $id,
            'attributes' => $attributes,
            'indexes' => $indexes,
        ]);

        // Check index limits, if given
        if ($indexes && $this->adapter->getIndexCount($collection) > $this->adapter->getIndexLimit()) {
            throw new LimitException('Index limit of ' . $this->adapter->getIndexLimit() . ' exceeded. Cannot create collection.');
        }

        // check attribute limits, if given
        if ($attributes) {
            if (
                $this->adapter->getAttributeLimit() > 0 &&
                $this->adapter->getAttributeCount($collection) > $this->adapter->getAttributeLimit()
            ) {
                throw new LimitException('Column limit of ' . $this->adapter->getAttributeLimit() . ' exceeded. Cannot create collection.');
            }

            if (
                $this->adapter->getRowLimit() > 0 &&
                $this->adapter->getAttributeWidth($collection) > $this->adapter->getRowLimit()
            ) {
                throw new LimitException('Row width limit of ' . $this->adapter->getRowLimit() . ' exceeded. Cannot create collection.');
            }
        }

        return $this->createDocument(self::METADATA, $collection);
    }

    /**
     * Get Collection
     *
     * @param string $id
     *
     * @return Document
     * @throws Exception
     */
    public function getCollection(string $id): Document
    {
        return $this->getDocument(self::METADATA, $id);
    }

    /**
     * List Collections
     *
     * @param int $offset
     * @param int $limit
     *
     * @return array
     * @throws Exception
     */
    public function listCollections(int $limit = 25, int $offset = 0): array
    {
        Authorization::disable();

        $result = $this->find(self::METADATA, [
            Query::limit($limit),
            Query::offset($offset)
        ]);

        Authorization::reset();

        return $result;
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
        $this->adapter->deleteCollection($id);

        return $this->deleteDocument(self::METADATA, $id);
    }

    /**
     * Create Attribute
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size utf8mb4 chars length
     * @param bool $required
     * @param array|bool|callable|int|float|object|resource|string|null $default
     * @param bool $signed
     * @param bool $array
     * @param string $format optional validation format of attribute
     * @param string $formatOptions assoc array with custom options that can be passed for the format validation
     * @param array $filters
     *
     * @return bool
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $required, $default = null, bool $signed = true, bool $array = false, string $format = null, array $formatOptions = [], array $filters = []): bool
    {
        $collection = $this->getCollection($collection);

        // attribute IDs are case insensitive
        $attributes = $collection->getAttribute('attributes', []);
        /** @var Document[] $attributes */
        foreach ($attributes as $attribute) {
            if (\strtolower($attribute->getId()) === \strtolower($id)) {
                throw new DuplicateException('Attribute already exists');
            }
        }

        if (
            $this->adapter->getAttributeLimit() > 0 &&
            $this->adapter->getAttributeCount($collection) >= $this->adapter->getAttributeLimit()
        ) {
            throw new LimitException('Column limit reached. Cannot create new attribute.');
        }

        if ($format) {
            if (!Structure::hasFormat($format, $type)) {
                throw new Exception('Format ("' . $format . '") not available for this attribute type ("' . $type . '")');
            }
        }

        $collection->setAttribute('attributes', new Document([
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
        ]), Document::SET_TYPE_APPEND);

        if (
            $this->adapter->getRowLimit() > 0 &&
            $this->adapter->getAttributeWidth($collection) >= $this->adapter->getRowLimit()
        ) {
            throw new LimitException('Row width limit reached. Cannot create new attribute.');
        }

        switch ($type) {
            case self::VAR_STRING:
                if ($size > $this->adapter->getStringLimit()) {
                    throw new Exception('Max size allowed for string is: ' . number_format($this->adapter->getStringLimit()));
                }
                break;

            case self::VAR_INTEGER:
                $limit = ($signed) ? $this->adapter->getIntLimit() / 2 : $this->adapter->getIntLimit();
                if ($size > $limit) {
                    throw new Exception('Max size allowed for int is: ' . number_format($limit));
                }
                break;
            case self::VAR_FLOAT:
            case self::VAR_BOOLEAN:
            case self::VAR_DATETIME:
                break;
            default:
                throw new Exception('Unknown attribute type: ' . $type);
                break;
        }

        // only execute when $default is given
        if (!\is_null($default)) {
            if ($required === true) {
                throw new Exception('Cannot set a default value on a required attribute');
            }
            switch (\gettype($default)) {
                    // first enforce typed array for each value in $default
                case 'array':
                    foreach ($default as $value) {
                        if ($type !== \gettype($value)) {
                            throw new Exception('Default value contents do not match given type ' . $type);
                        }
                    }
                    break;
                    // then enforce for primitive types
                case self::VAR_STRING:
                case self::VAR_INTEGER:
                case self::VAR_FLOAT:
                case self::VAR_BOOLEAN:
                    if ($type !== \gettype($default)) {
                        throw new Exception('Default value ' . $default . ' does not match given type ' . $type);
                    }
                    break;
                default:
                    throw new Exception('Unknown attribute type for: ' . $default);
                    break;
            }
        }

        $attribute = $this->adapter->createAttribute($collection->getId(), $id, $type, $size, $signed, $array);

        if ($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }

        return $attribute;
    }

    /**
     * Update attribute metadata. Utility method for update attribute methods.
     *
     * @param string $collection
     * @param string $id
     * @param string $key Metadata key to update
     * @param callable $updateCallback method that recieves document, and returns it with changes applied
     *
     * @return Document
     */
    private function updateAttributeMeta(string $collection, string $id, callable $updateCallback): void
    {
        // Load
        $collection = $this->getCollection($collection);

        $attributes = $collection->getAttribute('attributes', []);

        $attributeIndex = \array_search($id, \array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($attributeIndex === false) {
            throw new Exception('Attribute not found');
        }

        // Execute update from callback
        call_user_func($updateCallback, $attributes[$attributeIndex], $collection, $attributeIndex);

        // Save
        $collection->setAttribute('attributes', $attributes, Document::SET_TYPE_ASSIGN);

        if ($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }
    }

    /**
     * Update required status of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param bool $required
     *
     * @return void
     */
    public function updateAttributeRequired(string $collection, string $id, bool $required): void
    {
        $this->updateAttributeMeta($collection, $id, function ($attribute) use ($required) {
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
     * @return void
     */
    public function updateAttributeFormat(string $collection, string $id, string $format): void
    {
        $this->updateAttributeMeta($collection, $id, function ($attribute) use ($format) {
            if (!Structure::hasFormat($format, $attribute->getAttribute('type'))) {
                throw new Exception('Format ("' . $format . '") not available for this attribute type ("' . $attribute->getAttribute('type') . '")');
            }

            $attribute->setAttribute('format', $format);
        });
    }

    /**
     * Update format options of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param array $formatOptions assoc array with custom options that can be passed for the format validation
     *
     * @return void
     */
    public function updateAttributeFormatOptions(string $collection, string $id, array $formatOptions): void
    {
        $this->updateAttributeMeta($collection, $id, function ($attribute) use ($formatOptions) {
            $attribute->setAttribute('formatOptions', $formatOptions);
        });
    }

    /**
     * Update filters of attribute.
     *
     * @param string $collection
     * @param string $id
     * @param array $filters
     *
     * @return void
     */
    public function updateAttributeFilters(string $collection, string $id, array $filters): void
    {
        $this->updateAttributeMeta($collection, $id, function ($attribute) use ($filters) {
            $attribute->setAttribute('filters', $filters);
        });
    }

    /**
     * Update default value of attribute
     *
     * @param string $collection
     * @param string $id
     * @param array|bool|callable|int|float|object|resource|string|null $default
     *
     * @return void
     */
    public function updateAttributeDefault(string $collection, string $id, $default = null): void
    {
        $this->updateAttributeMeta($collection, $id, function ($attribute) use ($default) {
            if ($attribute->getAttribute('required') === true) {
                throw new Exception('Cannot set a default value on a required attribute');
            }
            switch (\gettype($default)) {
                    // first enforce typed array for each value in $default
                case 'array':
                    foreach ($default as $value) {
                        if ($attribute->getAttribute('type') !== \gettype($value)) {
                            throw new Exception('Default value contents do not match given type ' . $attribute->getAttribute('type'));
                        }
                    }
                    break;
                    // then enforce for primitive types
                case self::VAR_STRING:
                case self::VAR_INTEGER:
                case self::VAR_FLOAT:
                case self::VAR_BOOLEAN:
                    if ($attribute->getAttribute('type') !== \gettype($default)) {
                        throw new Exception('Default value ' . $default . ' does not match given type ' . $attribute->getAttribute('type'));
                    }
                    break;
                case 'NULL':
                    // Disable null. No validation required
                    break;
                default:
                    throw new Exception('Unknown attribute type for: ' . $default);
                    break;
            }

            $attribute->setAttribute('default', $default);
        });
    }

    /**
     * Update Attribute. This method is for updating data that causes underlying structure to change. Check out other updateAttribute methods if you are looking for metadata adjustments.
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size utf8mb4 chars length
     * @param bool $signed
     * @param bool $array
     *
     * To update attribute key (ID), use renameAttribute instead.
     *
     * @return bool
     */
    public function updateAttribute(string $collection, string $id, string $type = null, int $size = null, bool $signed = null, bool $array = null): bool
    {
        $this->updateAttributeMeta($collection, $id, function ($attribute, $collectionDoc, $attributeIndex) use ($collection, $id, $type, $size, $signed, $array, &$success) {
            if ($type !== null || $size !== null || $signed !== null || $array !== null) {
                $type ??= $attribute->getAttribute('type');
                $size ??= $attribute->getAttribute('size');
                $signed ??= $attribute->getAttribute('signed');
                $array ??= $attribute->getAttribute('array');

                switch ($type) {
                    case self::VAR_STRING:
                        if ($size > $this->adapter->getStringLimit()) {
                            throw new Exception('Max size allowed for string is: ' . number_format($this->adapter->getStringLimit()));
                        }
                        break;

                    case self::VAR_INTEGER:
                        $limit = ($signed) ? $this->adapter->getIntLimit() / 2 : $this->adapter->getIntLimit();
                        if ($size > $limit) {
                            throw new Exception('Max size allowed for int is: ' . number_format($limit));
                        }
                        break;
                    case self::VAR_FLOAT:
                    case self::VAR_BOOLEAN:
                        break;
                    default:
                        throw new Exception('Unknown attribute type: ' . $type);
                        break;
                }

                $attribute
                    ->setAttribute('type', $type)
                    ->setAttribute('size', $size)
                    ->setAttribute('signed', $signed)
                    ->setAttribute('array', $array);

                $attributes = $collectionDoc->getAttribute('attributes');
                $attributes[$attributeIndex] = $attribute;
                $collectionDoc->setAttribute('attributes', $attributes, Document::SET_TYPE_ASSIGN);

                if (
                    $this->adapter->getRowLimit() > 0 &&
                    $this->adapter->getAttributeWidth($collectionDoc) >= $this->adapter->getRowLimit()
                ) {
                    throw new LimitException('Row width limit reached. Cannot create new attribute.');
                }

                $this->adapter->updateAttribute($collection, $id, $type, $size, $signed, $array);
            }
        });

        return true;
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
        $collection = clone $collection;

        $collection->setAttribute('attributes', $attribute, Document::SET_TYPE_APPEND);

        if (
            $this->adapter->getAttributeLimit() > 0 &&
            $this->adapter->getAttributeCount($collection) > $this->adapter->getAttributeLimit()
        ) {
            throw new LimitException('Column limit reached. Cannot create new attribute.');
            return false;
        }

        if (
            $this->adapter->getRowLimit() > 0 &&
            $this->adapter->getAttributeWidth($collection) >= $this->adapter->getRowLimit()
        ) {
            throw new LimitException('Row width limit reached. Cannot create new attribute.');
            return false;
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
     */
    public function deleteAttribute(string $collection, string $id): bool
    {
        $collection = $this->getCollection($collection);

        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $id) {
                unset($attributes[$key]);
            }
        }

        $collection->setAttribute('attributes', $attributes);

        if ($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }

        return $this->adapter->deleteAttribute($collection->getId(), $id);
    }

    /**
     * Rename Attribute
     *
     * @param string $collection
     * @param string $old Current attribute ID
     * @param string $name New attribute ID
     *
     * @return bool
     */
    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        $collection = $this->getCollection($collection);
        $attributes = $collection->getAttribute('attributes', []);
        $indexes = $collection->getAttribute('indexes', []);

        $attribute = \in_array($old, \array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($attribute === false) {
            throw new Exception('Attribute not found');
        }

        $attributeNew = \in_array($new, \array_map(fn ($attribute) => $attribute['$id'], $attributes));

        if ($attributeNew !== false) {
            throw new DuplicateException('Attribute name already used');
        }

        foreach ($attributes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $old) {
                $attributes[$key]['key'] = $new;
                $attributes[$key]['$id'] = $new;
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
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }

        return $this->adapter->renameAttribute($collection->getId(), $old, $new);
    }

    /**
     * Rename Index
     *
     * @param string $collection
     * @param string $old
     * @param string $new
     *
     * @return bool
     */
    public function renameIndex(string $collection, string $old, string $new): bool
    {
        $collection = $this->getCollection($collection);

        $indexes = $collection->getAttribute('indexes', []);

        $index = \in_array($old, \array_map(fn ($index) => $index['$id'], $indexes));

        if ($index === false) {
            throw new Exception('Index not found');
        }

        $indexNew = \in_array($new, \array_map(fn ($index) => $index['$id'], $indexes));

        if ($indexNew !== false) {
            throw new DuplicateException('Index name already used');
        }

        foreach ($indexes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $old) {
                $indexes[$key]['key'] = $new;
                $indexes[$key]['$id'] = $new;
                break;
            }
        }

        $collection->setAttribute('indexes', $indexes);

        if ($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }

        return $this->adapter->renameIndex($collection->getId(), $old, $new);
    }

    /**
     * Create Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array $attributes
     * @param array $lengths
     * @param array $orders
     *
     * @return bool
     */
    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths = [], array $orders = []): bool
    {
        if (empty($attributes)) {
            throw new Exception('Missing attributes');
        }

        $collection = $this->getCollection($collection);

        // index IDs are case insensitive
        $indexes = $collection->getAttribute('indexes', []);
        /** @var Document[] $indexes */
        foreach ($indexes as $index) {
            if (\strtolower($index->getId()) === \strtolower($id)) {
                throw new DuplicateException('Index already exists');
            }
        }

        if ($this->adapter->getIndexCount($collection) >= $this->adapter->getIndexLimit()) {
            throw new LimitException('Index limit reached. Cannot create new index.');
        }

        switch ($type) {
            case self::INDEX_KEY:
                if (!$this->adapter->getSupportForIndex()) {
                    throw new Exception('Key index is not supported');
                }
                break;

            case self::INDEX_UNIQUE:
                if (!$this->adapter->getSupportForUniqueIndex()) {
                    throw new Exception('Unique index is not supported');
                }
                break;

            case self::INDEX_FULLTEXT:
                if (!$this->adapter->getSupportForUniqueIndex()) {
                    throw new Exception('Fulltext index is not supported');
                }
                break;

            default:
                throw new Exception('Unknown index type: ' . $type);
                break;
        }

        $index = $this->adapter->createIndex($collection->getId(), $id, $type, $attributes, $lengths, $orders);

        $collection->setAttribute('indexes', new Document([
            '$id' => ID::custom($id),
            'key' => $id,
            'type' => $type,
            'attributes' => $attributes,
            'lengths' => $lengths,
            'orders' => $orders,
        ]), Document::SET_TYPE_APPEND);

        if ($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }

        return $index;
    }

    /**
     * Delete Index
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function deleteIndex(string $collection, string $id): bool
    {
        $collection = $this->getCollection($collection);

        $indexes = $collection->getAttribute('indexes', []);

        foreach ($indexes as $key => $value) {
            if (isset($value['$id']) && $value['$id'] === $id) {
                unset($indexes[$key]);
            }
        }

        $collection->setAttribute('indexes', $indexes);

        if ($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }

        return $this->adapter->deleteIndex($collection->getId(), $id);
    }

    /**
     * Get Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return Document
     */
    public function getDocument(string $collection, string $id): Document
    {
        if ($collection === self::METADATA && $id === self::METADATA) {
            return new Document($this->collection);
        }

        if (empty($collection)) {
            throw new Exception('test exception: ' . $collection . ':' . $id);
        }

        $collection = $this->getCollection($collection);
        $document = null;
        $cache = null;

        $validator = new Authorization(self::PERMISSION_READ);

        // TODO@kodumbeats Check if returned cache id matches request
        if ($cache = $this->cache->load('cache-' . $this->getNamespace() . ':' . $collection->getId() . ':' . $id, self::TTL)) {
            $document = new Document($cache);

            $permitted = $collection->getId() === self::METADATA
                || $validator->isValid($collection->getRead());

            if (!$permitted) { // Check if user has read access to this collection
                return new Document();
            }

            return $document;
        }

        $document = $this->adapter->getDocument($collection->getId(), $id);
        $document->setAttribute('$collection', $collection->getId());

        $permitted = $collection->getId() === self::METADATA
            || $validator->isValid($collection->getRead());

        if (!$permitted && $collection->getId() !== self::METADATA) { // Check if user has read access to this collection
            return new Document();
        }
        if ($document->isEmpty()) {
            return $document;
        }

        $document = $this->casting($collection, $document);
        $document = $this->decode($collection, $document);

        $this->cache->save('cache-' . $this->getNamespace() . ':' . $collection->getId() . ':' . $id, $document->getArrayCopy()); // save to cache after fetching from db

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
     * @throws StructureException
     * @throws Exception
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $validator = new Authorization(self::PERMISSION_CREATE);

        $collection = $this->getCollection($collection);

        $time = DateTime::now();

        $permitted = $collection->getId() === self::METADATA
            || $validator->isValid($collection->getCreate());

        if (!$permitted) { // Check if user has update access to this collection
            throw new AuthorizationException($validator->getDescription());
        }

        $document
            ->setAttribute('$id', empty($document->getId()) ? ID::unique() : $document->getId())
            ->setAttribute('$collection', $collection->getId())
            ->setAttribute('$createdAt', $time)
            ->setAttribute('$updatedAt', $time);

        $document = $this->encode($collection, $document);

        $validator = new Structure($collection);

        if (!$validator->isValid($document)) {
            throw new StructureException($validator->getDescription());
        }

        $document = $this->adapter->createDocument($collection->getId(), $document);

        $document = $this->decode($collection, $document);

        return $document;
    }

    /**
     * Update Document
     *
     * @param string $collection
     * @param string $id
     *
     * @return Document
     *
     * @throws Exception
     */
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        if (!$document->getId() || !$id) {
            throw new Exception('Must define $id attribute');
        }

        $time = DateTime::now();
        $document->setAttribute('$updatedAt', $time);

        $old = $this->getDocument($collection, $id); // TODO make sure user don\'t need read permission for write operations
        $collection = $this->getCollection($collection);

        // Make sure reserved keys stay constant
        // $data['$id'] = $old->getId();
        // $data['$collection'] = $old->getCollection();

        $validator = new Authorization(self::PERMISSION_UPDATE);

        $permitted = $collection->getId() === self::METADATA
            || $validator->isValid($collection->getUpdate());

        if (!$permitted) { // Check if user has update access to this collection
            throw new AuthorizationException($validator->getDescription());
        }

        $document = $this->encode($collection, $document);

        $validator = new Structure($collection);

        if (!$validator->isValid($document)) { // Make sure updated structure still apply collection rules (if any)
            throw new StructureException($validator->getDescription());
        }

        $document = $this->adapter->updateDocument($collection->getId(), $document);
        $document = $this->decode($collection, $document);

        $this->cache->purge('cache-' . $this->getNamespace() . ':' . $collection->getId() . ':' . $id);

        return $document;
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
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $validator = new Authorization(self::PERMISSION_DELETE);
        $document = $this->getDocument($collection, $id);
        $collection = $this->getCollection($collection);

        $permitted = $collection->getId() === self::METADATA
            || $validator->isValid($collection->getDelete());

        if (!$permitted) { // Check if user has update access to this document
            throw new AuthorizationException($validator->getDescription());
        }

        $this->cache->purge('cache-' . $this->getNamespace() . ':' . $collection->getId() . ':' . $id);

        return $this->adapter->deleteDocument($collection->getId(), $id);
    }

    /**
     * Cleans the all the collection's documents from the cache
     *
     * @param string $collection
     *
     * @return bool
     */
    public function deleteCachedCollection(string $collection): bool
    {
        return $this->cache->purge('cache-' . $this->getNamespace() . ':' . $collection . ':*');
    }

    /**
     * Cleans a specific document from cache
     *
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function deleteCachedDocument(string $collection, string $id): bool
    {
        return $this->cache->purge('cache-' . $this->getNamespace() . ':' . $collection . ':' . $id);
    }

    /**
     * Find Documents
     *
     * @param string $collection
     * @param Query[] $queries
     *
     * @return Document[]
     * @throws Exception
     */
    public function find(string $collection, array $queries = []): array
    {
        $collection = $this->getCollection($collection);

        $grouped = Query::groupByType($queries);
        /** @var Query[] */ $filters = $grouped['filters'];
        /** @var int */ $limit = $grouped['limit'];
        /** @var int */ $offset = $grouped['offset'];
        /** @var string[] */ $orderAttributes = $grouped['orderAttributes'];
        /** @var string[] */ $orderTypes = $grouped['orderTypes'];
        /** @var Document */ $cursor = $grouped['cursor'];
        /** @var string */ $cursorDirection = $grouped['cursorDirection'];

        if (!empty($cursor) && $cursor->getCollection() !== $collection->getId()) {
            throw new Exception("cursor Document must be from the same Collection.");
        }

        $cursor = empty($cursor) ? [] : $cursor->getArrayCopy();

        $queries = self::convertQueries($collection, $filters);

        $results = $this->adapter->find(
            $collection->getId(),
            $queries,
            $limit ?? 25,
            $offset ?? 0,
            $orderAttributes,
            $orderTypes,
            $cursor ?? [],
            $cursorDirection ?? Database::CURSOR_AFTER,
        );

        foreach ($results as &$node) {
            $node = $this->casting($collection, $node);
            $node = $this->decode($collection, $node);
            $node->setAttribute('$collection', $collection->getId());
        }

        return $results;
    }

    /**
     * @param string $collection
     * @param array $queries
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     * @param Document|null $cursor
     * @param string $cursorDirection
     *
     * @return Document|bool
     */
    public function findOne(string $collection, array $queries = [])
    {
        $results = $this->find($collection, \array_merge([Query::limit(1)], $queries));
        return \reset($results);
    }

    /**
     * Count Documents
     *
     * Count the number of documents. Pass $max=0 for unlimited count
     *
     * @param string $collection
     * @param Query[] $queries
     * @param int $max
     *
     * @return int
     * @throws Exception
     */
    public function count(string $collection, array $queries = [], int $max = 0): int
    {
        $collection = $this->getCollection($collection);

        if ($collection->isEmpty()) {
            throw new Exception("Collection not found");
        }

        $queries = self::convertQueries($collection, $queries);

        return $this->adapter->count($collection->getId(), $queries, $max);
    }

    /**
     * Sum an attribute
     *
     * Sum an attribute for all the documents. Pass $max=0 for unlimited count
     *
     * @param string $collection
     * @param string $attribute
     * @param Query[] $queries
     * @param int $max
     *
     * @return int|float
     * @throws Exception
     */
    public function sum(string $collection, string $attribute, array $queries = [], int $max = 0)
    {
        $collection = $this->getCollection($collection);

        if ($collection->isEmpty()) {
            throw new Exception("Collection not found");
        }

        $queries = self::convertQueries($collection, $queries);
        return $this->adapter->sum($collection->getId(), $attribute, $queries, $max);
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
    static public function addFilter(string $name, callable $encode, callable $decode): void
    {
        self::$filters[$name] = [
            'encode' => $encode,
            'decode' => $decode,
        ];
    }

    /**
     * @return array Document
     * @throws Exception
     */
    public function getInternalAttributes(): array
    {
        $attributes = [];
        foreach ($this->attributes as $internal){
            $attributes[] = new Document($internal);
        }
        return $attributes;
    }

    /**
     * Encode Document
     *
     * @param Document $collection
     * @param Document $document
     *
     * @return Document
     * @throws Exception|Throwable
     */
    public function encode(Document $collection, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);
        $attributes = array_merge($attributes, $this->getInternalAttributes());
        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? '';
            $array = $attribute['array'] ?? false;
            $default = $attribute['default'] ?? null;
            $filters = $attribute['filters'] ?? [];
            $value = $document->getAttribute($key, null);

            // continue on optional param with no default
            if (is_null($value) && is_null($default)) {
                continue;
            }

            // assign default only if no no value provided
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
     *
     * @return Document
     * @throws Throwable|Exception
     */
    public function decode(Document $collection, Document $document): Document
    {
        $attributes = $collection->getAttribute('attributes', []);
        $attributes = array_merge($attributes, $this->getInternalAttributes());
        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? '';
            $array = $attribute['array'] ?? false;
            $filters = $attribute['filters'] ?? [];
            $value = $document->getAttribute($key, null);

            $value = ($array) ? $value : [$value];
            $value = (is_null($value)) ? [] : $value;

            foreach ($value as &$node) {
                foreach (array_reverse($filters) as $filter) {
                    $node = $this->decodeAttribute($filter, $node, $document);
                }
            }

            $document->setAttribute($key, ($array) ? $value : $value[0]);
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
                $value = (!is_string($value)) ? ($value ?? []) : json_decode($value, true);
            } else {
                $value = [$value];
            }

            foreach ($value as &$node) {
                if (is_null($value)) {
                    continue;
                }
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
                    case self::VAR_DATETIME:
                        break;
                    default:
                        # code...
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
     * @throws Throwable
     */
    protected function encodeAttribute(string $name, $value, Document $document)
    {
        if (!array_key_exists($name, self::$filters) && !array_key_exists($name, $this->instanceFilters)) {
            throw new Exception('Filter not found');
        }

        try {
            if (array_key_exists($name, $this->instanceFilters)) {
                $value = $this->instanceFilters[$name]['encode']($value, $document, $this);
            } else {
                $value = self::$filters[$name]['encode']($value, $document, $this);
            }
        } catch (\Throwable $th) {
            throw $th;
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
     */
    protected function decodeAttribute(string $name, $value, Document $document)
    {
        if (!array_key_exists($name, self::$filters) && !array_key_exists($name, $this->instanceFilters)) {
            throw new Exception('Filter not found');
        }

        try {
            if (array_key_exists($name, $this->instanceFilters)) {
                $value = $this->instanceFilters[$name]['decode']($value, $document, $this);
            } else {
                $value = self::$filters[$name]['decode']($value, $document, $this);
            }
        } catch (\Throwable $th) {
            throw $th;
        }

        return $value;
    }

    /**
     * Get adapter attribute limit, accounting for internal metadata
     * Returns 0 to indicate no limit
     *
     * @return int
     */
    public function getAttributeLimit()
    {
        // If negative, return 0
        // -1 ==> virtual columns count as total, so treat as buffer
        return \max($this->adapter->getAttributeLimit() - $this->adapter->getNumberOfDefaultAttributes() - 1, 0);
    }

    /**
     * Get adapter index limit
     *
     * @return int
     */
    public function getIndexLimit()
    {
        return $this->adapter->getIndexLimit() - $this->adapter->getNumberOfDefaultIndexes();
    }

    /**
     * @param Document $collection
     * @param Query[] $queries
     * @return Query[]
     * @throws Exception
     */
    public static function convertQueries(Document $collection, array $queries): array
    {
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $v) {
            /* @var $v Document */
            switch ($v->getAttribute('type')) {
                case Database::VAR_DATETIME:
                    foreach ($queries as $qk => $q) {
                        if ($q->getAttribute() === $v->getId()) {
                            $arr = $q->getValues();
                            foreach ($arr as $vk => $vv) {
                                $arr[$vk] = DateTime::setTimezone($vv);
                            }
                            $q->setValues($arr);
                            $queries[$qk] = $q;
                        }
                    }
                    break;
            }
        }
        return $queries;
    }
}

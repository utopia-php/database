<?php

namespace Utopia\Database;

use Exception;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Structure;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Cache\Cache;

class Database
{
    // Simple Types
    const VAR_STRING = 'string';
    const VAR_INTEGER = 'integer';
    const VAR_FLOAT = 'double';
    const VAR_BOOLEAN = 'boolean';

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
    const PERMISSION_READ = 'read';
    const PERMISSION_WRITE = 'write';

    // Collections
    const METADATA = '_metadata';

    // Lengths
    const LENGTH_KEY = 255;

    // Cache
    const TTL = 60 * 60 * 24; // 24 hours

    /**
     * @var Adapter
     */
    protected $adapter;

    /**
     * @var Cache
     */
    protected $cache;

    /**
     * @var array
     */
    protected $primitives = [
        self::VAR_STRING => true,
        self::VAR_INTEGER => true,
        self::VAR_FLOAT => true,
        self::VAR_BOOLEAN => true,
    ];

    /**
     * Parent Collection
     * Defines the structure for both system and custom collections
     * 
     * @var array
     */
    protected $collection = [
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
    static protected $filters = [];

    /**
     * @param Adapter $adapter
     * @param Cache $cache
     */
    public function __construct(Adapter $adapter, Cache $cache)
    {
        $this->adapter = $adapter;
        $this->cache = $cache;

        self::addFilter('json',
            /**
             * @param mixed $value
             * @return mixed
             */
            function($value) {
                $value = ($value instanceof Document) ? $value->getArrayCopy() : $value;

                if(!is_array($value) && !$value instanceof \stdClass) {
                    return $value;
                }

                return json_encode($value);
            },
            /**
             * @param mixed $value
             * @return mixed
             */
            function($value) {
                if(!is_string($value)) {
                    return $value;
                }

                $value = json_decode($value, true);

                if(array_key_exists('$id', $value)) {
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
     * Create Database
     *
     * @return bool
     */
    public function create(): bool
    {
        $this->adapter->create();

        /**
         * Create array of attribute documents
         * @var Document[] $attributes
         */
        $attributes = array_map(function ($attribute) {
            return new Document([
                '$id' => $attribute[0],
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
     *
     * @return bool
     */
    public function exists(): bool
    {
        return $this->adapter->exists();
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
     * @return bool
     */
    public function delete(): bool
    {
        return $this->adapter->delete();
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
        $this->adapter->createCollection($id, $attributes, $indexes);

        if($id === self::METADATA) {
            return new Document($this->collection);
        }

        $collection = new Document([
            '$id' => $id,
            '$read' => ['role:all'],
            '$write' => ['role:all'],
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
            if ($this->adapter->getAttributeLimit() > 0 && 
                $this->adapter->getAttributeCount($collection) > $this->adapter->getAttributeLimit())
            {
                throw new LimitException('Column limit of ' . $this->adapter->getAttributeLimit() . ' exceeded. Cannot create collection.');
            }

            if ($this->adapter->getRowLimit() > 0 && 
                $this->adapter->getAttributeWidth($collection) > $this->adapter->getRowLimit())
            {
                throw new LimitException('Row width limit of ' . $this->adapter->getRowLimit() . ' exceeded. Cannot create collection.');
            }
        }

        return $this->createDocument(self::METADATA, $collection);
    }

    /**
     * Get Collection
     * 
     * @param string $collection
     * @param string $id
     *
     * @return Document
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
     */
    public function listCollections($limit = 25, $offset = 0): array
    {
        Authorization::disable();

        $result = $this->find(self::METADATA, [], $limit, $offset);

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
        $attributes = $collection->getAttribute('attributes', []); /** @var Document[] $attributes */
        foreach ($attributes as $attribute) {
            if (\strtolower($attribute->getId()) === \strtolower($id)) {
                throw new DuplicateException('Attribute already exists');
            }
        }

        if ($this->adapter->getAttributeLimit() > 0 && 
            $this->adapter->getAttributeCount($collection) >= $this->adapter->getAttributeLimit())
        {
            throw new LimitException('Column limit reached. Cannot create new attribute.');
        }

        if ($format) {
            if (!Structure::hasFormat($format, $type)) {
                throw new Exception('Format ("'.$format.'") not available for this attribute type ("'.$type.'")');
            }
        } 

        $collection->setAttribute('attributes', new Document([
            '$id' => $id,
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

        if ($this->adapter->getRowLimit() > 0 && 
            $this->adapter->getAttributeWidth($collection) >= $this->adapter->getRowLimit())
        {
            throw new LimitException('Row width limit reached. Cannot create new attribute.');
        }
    
        if($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }
 
        switch ($type) {
            case self::VAR_STRING:
                if($size > $this->adapter->getStringLimit()) {
                    throw new Exception('Max size allowed for string is: '.number_format($this->adapter->getStringLimit()));
                }
                break;

            case self::VAR_INTEGER:
                $limit = ($signed) ? $this->adapter->getIntLimit() / 2 : $this->adapter->getIntLimit();
                if($size > $limit) {
                    throw new Exception('Max size allowed for int is: '.number_format($limit));
                }
                break;
            case self::VAR_FLOAT:
            case self::VAR_BOOLEAN:
                break;
            default:
                throw new Exception('Unknown attribute type: '.$type);
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
                    throw new Exception('Unknown attribute type for: '.$default);
                    break;
            }
        }

        return $this->adapter->createAttribute($collection->getId(), $id, $type, $size, $signed, $array);
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

        if ($this->adapter->getAttributeLimit() > 0 &&
            $this->adapter->getAttributeCount($collection) > $this->adapter->getAttributeLimit())
        {
            throw new LimitException('Column limit reached. Cannot create new attribute.');
            return false;
        }

        if ($this->adapter->getRowLimit() > 0 &&
            $this->adapter->getAttributeWidth($collection) >= $this->adapter->getRowLimit())
        {
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
            if(isset($value['$id']) && $value['$id'] === $id) {
                unset($attributes[$key]);
            }
        }

        $collection->setAttribute('attributes', $attributes);
    
        if($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }

        return $this->adapter->deleteAttribute($collection->getId(), $id);
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
        if(empty($attributes)) {
            throw new Exception('Missing attributes');
        }

        $collection = $this->getCollection($collection);

        // index IDs are case insensitive
        $indexes = $collection->getAttribute('indexes', []); /** @var Document[] $indexes */
        foreach ($indexes as $index) {
            if (\strtolower($index->getId()) === \strtolower($id)) {
                throw new DuplicateException('Index already exists');
            }
        }

        if ($this->adapter->getIndexCount($collection) >= $this->adapter->getIndexLimit()) {
            throw new LimitException('Index limit reached. Cannot create new index.');
        }

        $collection->setAttribute('indexes', new Document([
            '$id' => $id,
            'key' => $id,
            'type' => $type,
            'attributes' => $attributes,
            'lengths' => $lengths,
            'orders' => $orders,
        ]), Document::SET_TYPE_APPEND);

        if($collection->getId() !== self::METADATA) {
            $this->updateDocument(self::METADATA, $collection->getId(), $collection);
        }

        switch ($type) {
            case self::INDEX_KEY:
                if(!$this->adapter->getSupportForIndex()) {
                    throw new Exception('Key index is not supported');
                }
                break;

            case self::INDEX_UNIQUE:
                if(!$this->adapter->getSupportForUniqueIndex()) {
                    throw new Exception('Unique index is not supported');
                }
                break;

            case self::INDEX_FULLTEXT:
                if(!$this->adapter->getSupportForUniqueIndex()) {
                    throw new Exception('Fulltext index is not supported');
                }
                break;

            default:
                throw new Exception('Unknown index type: '.$type);
                break;
        }

        return $this->adapter->createIndex($collection->getId(), $id, $type, $attributes, $lengths, $orders);
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
            if(isset($value['$id']) && $value['$id'] === $id) {
                unset($indexes[$key]);
            }
        }

        $collection->setAttribute('indexes', $indexes);
    
        if($collection->getId() !== self::METADATA) {
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
        if($collection === self::METADATA && $id === self::METADATA) {
            return new Document($this->collection);
        }

        if(empty($collection)) {
            throw new Exception('test exception: '.$collection .':'. $id);
        }

        $collection = $this->getCollection($collection);
        $document = null;
        $cache = null;

        // TODO@kodumbeats Check if returned cache id matches request
        if ($cache = $this->cache->load('cache-'.$this->getNamespace().':'.$collection->getId().':'.$id, self::TTL)) {
            $document = new Document($cache);
            $validator = new Authorization(self::PERMISSION_READ);

            if (!$validator->isValid($document->getRead()) && $collection->getId() !== self::METADATA) { // Check if user has read access to this document
                return new Document();
            }

            return $document;
        }

        $document = $this->adapter->getDocument($collection->getId(), $id);

        $document->setAttribute('$collection', $collection->getId());

        $validator = new Authorization(self::PERMISSION_READ);

        if (!$validator->isValid($document->getRead()) && $collection->getId() !== self::METADATA) { // Check if user has read access to this document
            return new Document();
        }

        if($document->isEmpty()) {
            return $document;
        }

        $document = $this->casting($collection, $document);
        $document = $this->decode($collection, $document);

        $this->cache->save('cache-'.$this->getNamespace().':'.$collection->getId().':'.$id, $document->getArrayCopy()); // save to cache after fetching from db

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
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $validator = new Authorization(self::PERMISSION_WRITE);

        if (!$validator->isValid($document->getWrite())) { // Check if user has write access to this document
            throw new AuthorizationException($validator->getDescription());
        }

        $collection = $this->getCollection($collection);

        $document
            ->setAttribute('$id', empty($document->getId()) ? $this->getId(): $document->getId())
            ->setAttribute('$collection', $collection->getId())
        ;

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

        $old = $this->getDocument($collection, $id); // TODO make sure user don\'t need read permission for write operations
        $collection = $this->getCollection($collection);

        // Make sure reserved keys stay constant
        // $data['$id'] = $old->getId();
        // $data['$collection'] = $old->getCollection();

        $validator = new Authorization('write');

        if (!$validator->isValid($old->getWrite())) { // Check if user has write access to this document
            throw new AuthorizationException($validator->getDescription());
        }

        if (!$validator->isValid($document->getWrite())) { // Check if user has write access to this document
            throw new AuthorizationException($validator->getDescription());
        }

        $document = $this->encode($collection, $document);

        $validator = new Structure($collection);

        if (!$validator->isValid($document)) { // Make sure updated structure still apply collection rules (if any)
            throw new StructureException($validator->getDescription());
        }

        $document = $this->adapter->updateDocument($collection->getId(), $document);
        $document = $this->decode($collection, $document);

        $this->cache->purge('cache-'.$this->getNamespace().':'.$collection->getId().':'.$id);

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
        $document = $this->getDocument($collection, $id);

        $validator = new Authorization('write');

        if (!$validator->isValid($document->getWrite())) { // Check if user has write access to this document
            throw new AuthorizationException($validator->getDescription());
        }

        $this->cache->purge('cache-'.$this->getNamespace().':'.$collection.':'.$id);

        return $this->adapter->deleteDocument($collection, $id);
    }

    /**
     * Cleans the Document from Cache
     * 
     * @param string $collection
     * @param string $id
     *
     * @return bool
     */
    public function purgeDocument(string $collection, string $id): bool
    {
        return $this->cache->purge('cache-'.$this->getNamespace().':'.$collection.':'.$id);
    }

    /**
     * Find Documents
     * 
     * @param string $collection
     * @param Query[] $queries
     * @param int $limit
     * @param int $offset
     * @param array $orderAttributes
     * @param array $orderTypes
     * @param Document|null $orderAfter
     *
     * @return Document[]
     */
    public function find(string $collection, array $queries = [], int $limit = 25, int $offset = 0, array $orderAttributes = [], array $orderTypes = [], Document $orderAfter = null): array
    {
        $collection = $this->getCollection($collection);

        if (!empty($orderAfter) && $orderAfter->getCollection() !== $collection->getId()) {
            throw new Exception("orderAfter Document must be from the same Collection.");
        }

        $orderAfter = empty($orderAfter) ? [] : $orderAfter->getArrayCopy();

        $results = $this->adapter->find($collection->getId(), $queries, $limit, $offset, $orderAttributes, $orderTypes, $orderAfter);

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
     * @param Document|null $orderAfter
     * 
     * @return Document|bool
     */
    public function findOne(string $collection, array $queries = [], int $offset = 0, array $orderAttributes = [], array $orderTypes = [], Document $orderAfter = null)
    {
        $results = $this->find($collection, $queries, /*limit*/ 1, $offset, $orderAttributes, $orderTypes, $orderAfter);
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
     */
    public function count(string $collection, array $queries = [], int $max = 0): int
    {
        $count = $this->adapter->count($collection, $queries, $max);

        return $count;
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
     */
    public function sum(string $collection, string $attribute, array $queries = [], int $max = 0)
    {
        $count = $this->adapter->sum($collection, $attribute, $queries, $max);

        return $count;
    }

    // /**
    //  * @param array $data
    //  *
    //  * @return Document|false
    //  *
    //  * @throws Exception
    //  */
    // public function overwriteDocument(array $data)
    // {
    //     if (!isset($data['$id'])) {
    //         throw new Exception('Must define $id attribute');
    //     }

    //     $document = $this->getDocument($data['$collection'], $data['$id']); // TODO make sure user don\'t need read permission for write operations

    //     $validator = new Authorization($document, 'write');

    //     if (!$validator->isValid($document->getWrite())) { // Check if user has write access to this document
    //         throw new AuthorizationException($validator->getDescription());
    //     }

    //     $new = new Document($data);

    //     if (!$validator->isValid($new->getWrite())) { // Check if user has write access to this document
    //         throw new AuthorizationException($validator->getDescription());
    //     }

    //     $new = $this->encode($new);

    //     $validator = new Structure($this);

    //     if (!$validator->isValid($new)) { // Make sure updated structure still apply collection rules (if any)
    //         throw new StructureException($validator->getDescription());
    //     }

    //     $new = new Document($this->adapter->updateDocument($this->getCollection($new->getCollection()), $new->getId(), $new->getArrayCopy()));

    //     $new = $this->decode($new);

    //     return $new;
    // }

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
     * Encode Document
     * 
     * @param Document $collection
     * @param Document $document
     * 
     * @return Document
     */
    public function encode(Document $collection, Document $document):Document
    {
        $attributes = $collection->getAttribute('attributes', []);

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

            if(!$array) {
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
     */
    public function decode(Document $collection, Document $document):Document
    {
        $attributes = $collection->getAttribute('attributes', []);

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
    public function casting(Document $collection, Document $document):Document
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

            if($array) {
                $value = (!is_string($value)) ? ($value ?? []) : json_decode($value, true);
            }
            else {
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
     */
    protected function encodeAttribute(string $name, $value, Document $document)
    {
        if (!isset(self::$filters[$name])) {
            throw new Exception('Filter not found');
        }

        try {
            $value = self::$filters[$name]['encode']($value, $document, $this);
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
        if (!isset(self::$filters[$name])) {
            throw new Exception('Filter not found');
        }

        try {
            $value = self::$filters[$name]['decode']($value, $document, $this);
        } catch (\Throwable $th) {
            throw $th;
        }

        return $value;
    }

    /**
     * Get 13 Chars Unique ID.
     * 
     * @return string
     */
    public function getId(): string
    {
        return \uniqid();
    }
}
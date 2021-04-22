<?php

namespace Utopia\Database;

use Exception;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Structure;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Structure as StructureException;

class Database
{
    const METADATA = 'metadata';

    // Simple Types
    const VAR_STRING = 'string';
    const VAR_INTEGER = 'integer';
    const VAR_FLOAT = 'float';
    const VAR_BOOLEAN = 'boolean';
    
    // Relationships Types
    const VAR_DOCUMENT = 'document';
    
    // Index Types
    const INDEX_KEY = 'text';
    const INDEX_FULLTEXT = 'fulltext';
    const INDEX_UNIQUE = 'unique';
    const INDEX_SPATIAL = 'spatial';

    // Orders
    const ORDER_ASC = 'ASC';
    const ORDER_DESC = 'DESC';

    // Permissions
    const PERMISSION_READ = 'read';
    const PERMISSION_WRITE = 'write';

    // Collections
    const COLLECTIONS = 'collections';

    /**
     * @var Adapter
     */
    protected $adapter;

    /**
     * Parent Collection
     * Defines the structure for both system and custom collections
     * 
     * @var array
     */
    protected $collection = [
        '$id' => self::COLLECTIONS,
        '$collection' => self::COLLECTIONS,
        'name' => 'collections',
        'attributes' => [
            [
                '$id' => 'name',
                'type' => self::VAR_STRING,
                'size' => 256,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'attributes',
                'type' => self::VAR_STRING,
                'size' => 1000000,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
            [
                '$id' => 'indexes',
                'type' => self::VAR_STRING,
                'size' => 1000000,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
        ],
        'indexes' => [],
    ];

    /**
     * @var array
     */
    static public $filters = [];

    /**
     * @param Adapter $adapter
     */
    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;
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

        $this->createCollection(self::COLLECTIONS);
        $this->createAttribute(self::COLLECTIONS, 'name', self::VAR_STRING, 128);
        $this->createAttribute(self::COLLECTIONS, 'attributes', self::VAR_STRING, 8064);
        $this->createAttribute(self::COLLECTIONS, 'indexes', self::VAR_STRING, 8064);
        $this->createIndex(self::COLLECTIONS, '_key_1', self::INDEX_UNIQUE, ['name']);

        return true;
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
     * 
     * @return Document
     */
    public function createCollection(string $id): Document
    {
        $this->adapter->createCollection($id);

        if($id === self::COLLECTIONS) {
            return new Document($this->collection);
        }
        
        return $this->createDocument(Database::COLLECTIONS, new Document([
            '$id' => $id,
            '$read' => ['*'],
            '$write' => ['*'],
            'name' => $id,
            'attributes' => [],
            'indexes' => [],
        ]));
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
        return $this->getDocument(self::COLLECTIONS, $id);
    }

    /**
     * List Collections
     * 
     * @return array
     */
    public function listCollections(): array
    {
        // TODO add a search here
        return [];
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

        return $this->deleteDocument(self::COLLECTIONS, $id);
    }

    /**
     * Create Attribute
     * 
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param int $size
     * @param bool $array
     * 
     * @return bool
     */
    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, array $filters = []): bool
    {
        $collection = $this->getCollection($collection);

        $collection->setAttribute('attributes', new Document([
            '$id' => $id,
            'type' => $type,
            'size' => $size,
            'signed' => $signed,
            'array' => $array,
            'filters' => $filters,
        ]), Document::SET_TYPE_APPEND);
    
        if($collection->getId() !== self::COLLECTIONS) {
            $this->updateDocument(self::COLLECTIONS, $collection->getId(), $collection);
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

        return $this->adapter->createAttribute($collection->getId(), $id, $type, $size, $signed, $array);
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
    
        if($collection->getId() !== self::COLLECTIONS) {
            $this->updateDocument(self::COLLECTIONS, $collection->getId(), $collection);
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

        $collection->setAttribute('indexes', new Document([
            '$id' => $id,
            'type' => $type,
            'attributes' => $attributes,
            'lengths' => $lengths,
            'orders' => $orders,
        ]), Document::SET_TYPE_APPEND);
    
        if($collection->getId() !== self::COLLECTIONS) {
            $this->updateDocument(self::COLLECTIONS, $collection->getId(), $collection);
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
    
        if($collection->getId() !== self::COLLECTIONS) {
            $this->updateDocument(self::COLLECTIONS, $collection->getId(), $collection);
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
        if($collection === self::COLLECTIONS && $id === self::COLLECTIONS) {
            return new Document($this->collection);
        }

        if(empty($collection)) {
            throw new Exception('test exception: '.$collection .':'. $id);
        }

        $collection = $this->getCollection($collection);

        $document   = $this->adapter->getDocument($collection->getId(), $id);

        $document->setAttribute('$collection', $collection->getId());

        $validator = new Authorization($document, self::PERMISSION_READ);

        if (!$validator->isValid($document->getRead()) && $collection->getId() !== self::COLLECTIONS) { // Check if user has read access to this document
            return new Document();
        }

        if($document->isEmpty()) {
            return $document;
        }

        $document = $this->decode($collection, $document);

        return $document;
    }

    /**
     * Create Document
     * 
     * @param string $collection
     * @param Document $data
     *
     * @return Document
     *
     * @throws AuthorizationException
     * @throws StructureException
     */
    public function createDocument(string $collection, Document $document): Document
    {
        $validator = new Authorization($document, self::PERMISSION_WRITE);

        if (!$validator->isValid($document->getWrite())) { // Check if user has write access to this document
            throw new AuthorizationException($validator->getDescription());
        }

        // $document = $this->encode($document);
        // $validator = new Structure($this);

        // if (!$validator->isValid($document)) {
        //     throw new StructureException($validator->getDescription()); // var_dump($validator->getDescription()); return false;
        // }

        $document->setAttribute('$id', empty($document->getId()) ? $this->getId(): $document->getId());
        
        $document = $this->adapter->createDocument($collection, $document);

        $document->setAttribute('$collection', $collection);
        
        // $document = $this->decode($document);

        return $document;
    }

    /**
     * Update Document
     * 
     * @param string $collection
     * @param string $id
     * @param Document $document
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

        // Make sure reserved keys stay constant
        // $data['$id'] = $old->getId();
        // $data['$collection'] = $old->getCollection();

        $validator = new Authorization($old, 'write');

        if (!$validator->isValid($old->getWrite())) { // Check if user has write access to this document
            throw new AuthorizationException($validator->getDescription()); // var_dump($validator->getDescription()); return false;
        }

        if (!$validator->isValid($document->getWrite())) { // Check if user has write access to this document
            throw new AuthorizationException($validator->getDescription()); // var_dump($validator->getDescription()); return false;
        }

        // $document = $this->encode($document);

        // $validator = new Structure($this);

        // if (!$validator->isValid($document)) { // Make sure updated structure still apply collection rules (if any)
        //     throw new StructureException($validator->getDescription()); // var_dump($validator->getDescription()); return false;
        // }

        $document = $this->adapter->updateDocument($collection, $document);
        
        // $new = $this->decode($new);

        return $document;
    }

    /**
     * @param string $collection
     * @param string $id
     *
     * @return false
     *
     * @throws AuthorizationException
     */
    public function deleteDocument(string $collection, string $id): bool
    {
        $document = $this->getDocument($collection, $id);

        $validator = new Authorization($document, 'write');

        if (!$validator->isValid($document->getWrite())) { // Check if user has write access to this document
            throw new AuthorizationException($validator->getDescription());
        }

        return $this->adapter->deleteDocument($collection, $id);
    }

    // /**
    //  * @param string $collection
    //  * @param array $options
    //  *
    //  * @return Document[]
    //  */
    // public function find(string $collection, array $options)
    // {
    //     $options = \array_merge([
    //         'offset' => 0,
    //         'limit' => 15,
    //         'search' => '',
    //         'relations' => true,
    //         'orderField' => '',
    //         'orderType' => 'ASC',
    //         'orderCast' => 'int',
    //         'filters' => [],
    //     ], $options);

    //     $results = $this->adapter->find($this->getCollection($collection), $options);

    //     foreach ($results as &$node) {
    //         $node = $this->decode(new Document($node));
    //     }

    //     return $results;
    // }

    // /**
    //  * @param string $collection
    //  * @param array $options
    //  *
    //  * @return Document
    //  */
    // public function findFirst(string $collection, array $options)
    // {
    //     $results = $this->find($collection, $options);
    //     return \reset($results);
    // }

    // /**
    //  * @param string $collection
    //  * @param array $options
    //  *
    //  * @return Document
    //  */
    // public function findLast(string $collection, array $options)
    // {
    //     $results = $this->find($collection, $options);
    //     return \end($results);
    // }

    // /**
    //  * @param array $options
    //  *
    //  * @return int
    //  */
    // public function count(array $options)
    // {
    //     $options = \array_merge([
    //         'filters' => [],
    //     ], $options);

    //     $results = $this->adapter->count($options);

    //     return $results;
    // }

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
    //         throw new AuthorizationException($validator->getDescription()); // var_dump($validator->getDescription()); return false;
    //     }

    //     $new = new Document($data);

    //     if (!$validator->isValid($new->getWrite())) { // Check if user has write access to this document
    //         throw new AuthorizationException($validator->getDescription()); // var_dump($validator->getDescription()); return false;
    //     }

    //     $new = $this->encode($new);

    //     $validator = new Structure($this);

    //     if (!$validator->isValid($new)) { // Make sure updated structure still apply collection rules (if any)
    //         throw new StructureException($validator->getDescription()); // var_dump($validator->getDescription()); return false;
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

    public function encode(Document $collection, Document $document):Document
    {
        $rules = $collection->getAttribute('rules', []);

        foreach ($rules as $key => $rule) {
            $key = $rule->getAttribute('key', null);
            $type = $rule->getAttribute('type', null);
            $array = $rule->getAttribute('array', false);
            $filters = $rule->getAttribute('filter', []);
            $value = $document->getAttribute($key, null);

            if (($value !== null)) {
                foreach ($filters as $filter) {
                    $value = $this->encodeAttribute($filter, $value);
                    $document->setAttribute($key, $value);
                }
            }
        }

        return $document;
    }

    public function decode(Document $collection, Document $document):Document
    {
        $rules = $collection->getAttribute('attributes', []);

        foreach ($rules as $rule) {
            $key = $rule['$id'] ?? '';
            $type = $rule['type'] ?? '';
            $array = $rule['array'] ?? false;
            $filters = $rule['filters'] ?? [];
            $value = $document->getAttribute($key, null);

            if($array) {
                $value = json_decode($value, true);
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
     * @param string $name
     * @param mixed $value
     * 
     * @return mixed
     */
    static protected function encodeAttribute(string $name, $value)
    {
        if (!isset(self::$filters[$name])) {
            return $value;
            throw new Exception('Filter not found');
        }

        try {
            $value = self::$filters[$name]['encode']($value);
        } catch (\Throwable $th) {
            $value = null;
        }

        return $value;
    }

    /**
     * Decode Attribute
     * 
     * @param string $name
     * @param mixed $value
     * 
     * @return mixed
     */
    static protected function decodeAttribute(string $name, $value)
    {
        if (!isset(self::$filters[$name])) {
            return $value;
            throw new Exception('Filter not found');
        }

        try {
            $value = self::$filters[$name]['decode']($value);
        } catch (\Throwable $th) {
            $value = null;
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

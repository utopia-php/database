<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\Structure;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;

class StructureTest extends TestCase
{
    /**
     * @var array
     */
    protected $collection = [
        '$id' => Database::COLLECTIONS,
        '$collection' => Database::COLLECTIONS,
        'name' => 'collections',
        'attributes' => [
            [
                '$id' => 'title',
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'description',
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 1000000,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'rating',
                'type' => Database::VAR_INTEGER,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'price',
                'type' => Database::VAR_FLOAT,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'published',
                'type' => Database::VAR_BOOLEAN,
                'format' => '',
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'tags',
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 55,
                'required' => true,
                'signed' => true,
                'array' => true,
                'filters' => [],
            ],
        ],
        'indexes' => [],
    ];

    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testDocumentInstance()
    {
        $validator = new Structure(new Document($this->collection));

        $this->assertEquals(false, $validator->isValid('string'));
        $this->assertEquals(false, $validator->isValid(null));
        $this->assertEquals(false, $validator->isValid(false));
        $this->assertEquals(false, $validator->isValid(1));

        $this->assertEquals('Invalid document structure: Value must be an instance of Document', $validator->getDescription());
    }

    public function testCollectionAttribute()
    {
        $validator = new Structure(new Document());

        $this->assertEquals(false, $validator->isValid(new Document()));

        $this->assertEquals('Invalid document structure: Missing collection attribute $collection', $validator->getDescription());
    }

    public function testCollection()
    {
        $validator = new Structure(new Document());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Collection "" not found', $validator->getDescription());
    }

    public function testReuiredKeys()
    {
        $validator = new Structure(new Document($this->collection));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Missing required attribute "title"', $validator->getDescription());
    }
    
    public function testUnknownKeys()
    {    
        $validator = new Structure(new Document($this->collection));

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'Demo Title',
            'titlex' => 'Unknown Attribute',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Unknown attribute: ""titlex"', $validator->getDescription());
    }  
            
    public function testValidDocument()
    { 
        $validator = new Structure(new Document($this->collection));
        
        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

    }

    public function testStringValidation()
    { 
        $validator = new Structure(new Document($this->collection));
        
        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 5,
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "title" has invalid type. Value must be a string', $validator->getDescription());
    }

    public function testArrayOfStringsValidation()
    { 
        $validator = new Structure(new Document($this->collection));
        
        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => [1, 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "tags[\'0\']" has invalid type. Value must be a string', $validator->getDescription());

        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => [true],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "tags[\'0\']" has invalid type. Value must be a string', $validator->getDescription());

        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => true,
            'tags' => [],
        ])));
    }

    public function testIntegerValidation()
    { 
        $validator = new Structure(new Document($this->collection));
        
        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => true,
            'price' => 1.99,
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid integer', $validator->getDescription());
        
        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => '',
            'price' => 1.99,
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "rating" has invalid type. Value must be a valid integer', $validator->getDescription());
    }

    public function testFloatValidation()
    { 
        $validator = new Structure(new Document($this->collection));
        
        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 2,
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "price" has invalid type. Value must be a valid float', $validator->getDescription());
        
        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => '',
            'published' => false,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "price" has invalid type. Value must be a valid float', $validator->getDescription());
    }

    public function testBooleanValidation()
    { 
        $validator = new Structure(new Document($this->collection));
        
        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => 1,
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "published" has invalid type. Value must be a boolean', $validator->getDescription());
        
        $this->assertEquals(false, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'string',
            'description' => 'Demo description',
            'rating' => 5,
            'price' => 1.99,
            'published' => '',
            'tags' => ['dog', 'cat', 'mouse'],
        ])));

        $this->assertEquals('Invalid document structure: Attribute "published" has invalid type. Value must be a boolean', $validator->getDescription());
    }
}

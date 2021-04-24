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
                'size' => 256,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'description',
                'type' => Database::VAR_STRING,
                'size' => 1000000,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'rating',
                'type' => Database::VAR_INTEGER,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ],
            [
                '$id' => 'published',
                'type' => Database::VAR_BOOLEAN,
                'size' => 5,
                'required' => true,
                'signed' => true,
                'array' => false,
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
            'published' => true,
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
            'published' => true,
        ])));

        $this->assertEquals('Invalid document structure: Missing required key "title"', $validator->getDescription());
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
            'published' => true,
        ])));

        $this->assertEquals('Invalid document structure: Unknown property: ""titlex"', $validator->getDescription());
        
        $this->assertEquals(true, $validator->isValid(new Document([
            '$collection' => 'posts',
            'title' => 'Demo Title',
            'description' => 'Demo description',
            'rating' => 5,
            'published' => true,
        ])));
    }
}

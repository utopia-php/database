<?php

namespace Utopia\Tests;

use Utopia\Database\Document;
use PHPUnit\Framework\TestCase;

class DocumentTest extends TestCase
{
    /**
     * @var Document
     */
    protected $document = null;

    /**
     * @var Document
     */
    protected $empty = null;
    
    /**
     * @var string
     */
    protected $id = null;
    
    /**
     * @var string
     */
    protected $collection = null;

    public function setUp(): void
    {
        $this->id = uniqid();
        
        $this->collection = uniqid();

        $this->document = new Document([
            '$id' => $this->id,
            '$collection' => $this->collection,
            '$read' => ['user:123', 'team:123'],
            '$write' => ['*'],
            'title' => 'This is a test.',
            'list' => [
                'one'
            ],
            'children' => [
                new Document(['name' => 'x']),
                new Document(['name' => 'y']),
                new Document(['name' => 'z']),
            ]
        ]);

        $this->empty = new Document();
    }

    public function tearDown(): void
    {
    }

    public function testId()
    {
        $this->assertEquals($this->id, $this->document->getId());
        $this->assertEquals(null, $this->empty->getId());
    }

    public function testCollection()
    {
        $this->assertEquals($this->collection, $this->document->getCollection());
        $this->assertEquals(null, $this->empty->getCollection());
    }

    public function testPermissions()
    {
        $this->assertEquals(['user:123', 'team:123'], $this->document->getRead());
        $this->assertEquals(['*'], $this->document->getWrite());
    }

    public function testGetAttributes()
    {
        $this->assertEquals([
            'title' => 'This is a test.',
            'list' => [
                'one'
            ],
            'children' => [
                new Document(['name' => 'x']),
                new Document(['name' => 'y']),
                new Document(['name' => 'z']),
            ]
        ], $this->document->getAttributes());
    }

    public function testGetAttribute()
    {
        $this->assertEquals('This is a test.', $this->document->getAttribute('title', ''));
        $this->assertEquals('', $this->document->getAttribute('titlex', ''));
    }

    public function testSetAttribute()
    {
        $this->assertEquals('This is a test.', $this->document->getAttribute('title', ''));
        $this->assertEquals(['one'], $this->document->getAttribute('list', []));
        $this->assertEquals('', $this->document->getAttribute('titlex', ''));
        
        $this->document->setAttribute('title', 'New title');
     
        $this->assertEquals('New title', $this->document->getAttribute('title', ''));
        $this->assertEquals('', $this->document->getAttribute('titlex', ''));

        $this->document->setAttribute('list', 'two', Document::SET_TYPE_APPEND);
        $this->assertEquals(['one', 'two'], $this->document->getAttribute('list', []));

        $this->document->setAttribute('list', 'zero', Document::SET_TYPE_PREPEND);
        $this->assertEquals(['zero', 'one', 'two'], $this->document->getAttribute('list', []));

        $this->document->setAttribute('list', ['one'], Document::SET_TYPE_ASSIGN);
        $this->assertEquals(['one'], $this->document->getAttribute('list', []));
    }

    public function testRemoveAttribute()
    {
        $this->document->removeAttribute('list');
        $this->assertEquals([], $this->document->getAttribute('list', []));
    }

    public function testSearch()
    {
        $this->assertEquals(null, $this->document->search('find', 'one'));
        
        $this->document->setAttribute('findString', 'demo');
        $this->assertEquals($this->document, $this->document->search('findString', 'demo'));
        
        $this->document->setAttribute('findArray', ['demo']);
        $this->assertEquals(null, $this->document->search('findArray', 'demo'));
        $this->assertEquals($this->document, $this->document->search('findArray', ['demo']));

        $this->assertEquals($this->document->getAttribute('children')[0], $this->document->search('name', 'x', $this->document->getAttribute('children')));
        $this->assertEquals($this->document->getAttribute('children')[2], $this->document->search('name', 'z', $this->document->getAttribute('children')));
        $this->assertEquals(null, $this->document->search('name', 'v', $this->document->getAttribute('children')));
    }

    public function testIsEmpty()
    {
        $this->assertEquals(false, $this->document->isEmpty());
        $this->assertEquals(true, $this->empty->isEmpty());
    }

    public function testIsSet()
    {
        $this->assertEquals(false, $this->document->isSet('titlex'));
        $this->assertEquals(false, $this->empty->isSet('titlex'));
        $this->assertEquals(true, $this->document->isSet('title'));
    }

    public function testFilterStatus()
    {
        $this->assertEquals(false, $this->document->getFilterStatus());
        $this->assertEquals($this->document, $this->document->setFilterStatus(true));
        $this->assertEquals(true, $this->document->getFilterStatus());
    }

    public function testCastingStatus()
    {
        $this->assertEquals(false, $this->document->getCastingStatus());
        $this->assertEquals($this->document, $this->document->setCastingStatus(true));
        $this->assertEquals(true, $this->document->getCastingStatus());
    }

    public function testGetArrayCopy()
    {
        $this->assertEquals([
            '$id' => $this->id,
            '$collection' => $this->collection,
            '$read' => ['user:123', 'team:123'],
            '$write' => ['*'],
            'title' => 'This is a test.',
            'list' => [
                'one'
            ],
            'children' => [
                ['name' => 'x'],
                ['name' => 'y'],
                ['name' => 'z'],
            ]
        ], $this->document->getArrayCopy());
        $this->assertEquals([], $this->empty->getArrayCopy());
    }
}
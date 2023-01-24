<?php

namespace Utopia\Tests;

use Utopia\Database\Database;
use Utopia\Database\Document;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

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
            '$id' => ID::custom($this->id),
            '$collection' => ID::custom($this->collection),
            '$permissions' => [
                Permission::read(Role::user(ID::custom('123'))),
                Permission::read(Role::team(ID::custom('123'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('creator'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('updater'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('deleter'))),
            ],
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

    public function testGetCreate()
    {
        $this->assertEquals(['any', 'user:creator'], $this->document->getCreate());
        $this->assertEquals([], $this->empty->getCreate());
    }

    public function testGetRead()
    {
        $this->assertEquals(['user:123', 'team:123'], $this->document->getRead());
        $this->assertEquals([], $this->empty->getRead());
    }

    public function testGetUpdate()
    {
        $this->assertEquals(['any', 'user:updater'], $this->document->getUpdate());
        $this->assertEquals([], $this->empty->getUpdate());
    }

    public function testGetDelete()
    {
        $this->assertEquals(['any', 'user:deleter'], $this->document->getDelete());
        $this->assertEquals([], $this->empty->getDelete());
    }

    public function testGetPermissionByType()
    {
        $this->assertEquals(['any','user:creator'], $this->document->getPermissionsByType(Database::PERMISSION_CREATE));
        $this->assertEquals([], $this->empty->getPermissionsByType(Database::PERMISSION_CREATE));

        $this->assertEquals(['user:123','team:123'], $this->document->getPermissionsByType(Database::PERMISSION_READ));
        $this->assertEquals([], $this->empty->getPermissionsByType(Database::PERMISSION_READ));

        $this->assertEquals(['any','user:updater'], $this->document->getPermissionsByType(Database::PERMISSION_UPDATE));
        $this->assertEquals([], $this->empty->getPermissionsByType(Database::PERMISSION_UPDATE));

        $this->assertEquals(['any','user:deleter'], $this->document->getPermissionsByType(Database::PERMISSION_DELETE));
        $this->assertEquals([], $this->empty->getPermissionsByType(Database::PERMISSION_DELETE));
    }

    public function testGetPermissions()
    {
        $this->assertEquals([
            Permission::read(Role::user(ID::custom('123'))),
            Permission::read(Role::team(ID::custom('123'))),
            Permission::create(Role::any()),
            Permission::create(Role::user(ID::custom('creator'))),
            Permission::update(Role::any()),
            Permission::update(Role::user(ID::custom('updater'))),
            Permission::delete(Role::any()),
            Permission::delete(Role::user(ID::custom('deleter'))),
        ], $this->document->getPermissions());
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

    public function testFind()
    {
        $this->assertEquals(null, $this->document->find('find', 'one'));

        $this->document->setAttribute('findString', 'demo');
        $this->assertEquals($this->document, $this->document->find('findString', 'demo'));

        $this->document->setAttribute('findArray', ['demo']);
        $this->assertEquals(null, $this->document->find('findArray', 'demo'));
        $this->assertEquals($this->document, $this->document->find('findArray', ['demo']));

        $this->assertEquals($this->document->getAttribute('children')[0], $this->document->find('name', 'x', 'children'));
        $this->assertEquals($this->document->getAttribute('children')[2], $this->document->find('name', 'z', 'children'));
        $this->assertEquals(null, $this->document->find('name', 'v', 'children'));
    }

    public function testFindAndReplace()
    {
        $document = new Document([
            '$id' => ID::custom($this->id),
            '$collection' => ID::custom($this->collection),
            '$permissions' => [
                Permission::read(Role::user(ID::custom('123'))),
                Permission::read(Role::team(ID::custom('123'))),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
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

        $this->assertEquals(true, $document->findAndReplace('name', 'x', new Document(['name' => '1', 'test' => true]), 'children'));
        $this->assertEquals('1', $document->getAttribute('children')[0]['name']);
        $this->assertEquals(true, $document->getAttribute('children')[0]['test']);

        // Array with wrong value
        $this->assertEquals(false, $document->findAndReplace('name', 'xy', new Document(['name' => '1', 'test' => true]), 'children'));

        // Array with wrong key
        $this->assertEquals(false, $document->findAndReplace('namex', 'x', new Document(['name' => '1', 'test' => true]), 'children'));

        // No array
        $this->assertEquals(true, $document->findAndReplace('title', 'This is a test.', 'new'));
        $this->assertEquals('new', $document->getAttribute('title'));

        // No array with wrong value
        $this->assertEquals(false, $document->findAndReplace('title', 'test', 'new'));

        // No array with wrong key
        $this->assertEquals(false, $document->findAndReplace('titlex', 'This is a test.', 'new'));
    }

    public function testFindAndRemove()
    {
        $document = new Document([
            '$id' => ID::custom($this->id),
            '$collection' => ID::custom($this->collection),
            '$permissions' => [
                Permission::read(Role::user(ID::custom('123'))),
                Permission::read(Role::team(ID::custom('123'))),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
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
        $this->assertEquals(true, $document->findAndRemove('name', 'x', 'children'));
        $this->assertEquals('y', $document->getAttribute('children')[1]['name']);
        $this->assertCount(2, $document->getAttribute('children'));

        // Array with wrong value
        $this->assertEquals(false, $document->findAndRemove('name', 'xy', 'children'));

        // Array with wrong key
        $this->assertEquals(false, $document->findAndRemove('namex', 'x', 'children'));

        // No array
        $this->assertEquals(true, $document->findAndRemove('title', 'This is a test.'));
        $this->assertEquals(false, $document->isset('title'));

        // No array with wrong value
        $this->assertEquals(false, $document->findAndRemove('title', 'new'));

        // No array with wrong key
        $this->assertEquals(false, $document->findAndRemove('titlex', 'This is a test.'));
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

    public function testGetArrayCopy()
    {
        $this->assertEquals([
            '$id' => ID::custom($this->id),
            '$collection' => ID::custom($this->collection),
            '$permissions' => [
                Permission::read(Role::user(ID::custom('123'))),
                Permission::read(Role::team(ID::custom('123'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('creator'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('updater'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('deleter'))),
            ],
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

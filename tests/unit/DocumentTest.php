<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class DocumentTest extends TestCase
{
    /**
     * @var Document
     */
    protected ?Document $document = null;

    /**
     * @var Document
     */
    protected ?Document $empty = null;

    /**
     * @var string
     */
    protected ?string $id = null;

    /**
     * @var string
     */
    protected ?string $collection = null;

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

    public function testDocumentNulls(): void
    {
        $data = [
            'cat' => null,
            'dog' => null, // last entry is null
        ];

        $document = new Document($data);

        $this->assertSame(null, $document['cat']);
        $this->assertSame(false, isset($document['cat']));
        $this->assertSame('cat', $document->getAttribute('cat', 'cat'));

        $this->assertSame(null, $document['dog']);
        $this->assertSame(false, isset($document['dog']));
        $this->assertSame('dog', $document->getAttribute('dog', 'dog'));
    }

    public function testId(): void
    {
        $this->assertSame($this->id, $this->document->getId());
        $this->assertSame(null, $this->empty->getId());
    }

    public function testCollection(): void
    {
        $this->assertSame($this->collection, $this->document->getCollection());
        $this->assertSame(null, $this->empty->getCollection());
    }

    public function testGetCreate(): void
    {
        $this->assertSame(['any', 'user:creator'], $this->document->getCreate());
        $this->assertSame([], $this->empty->getCreate());
    }

    public function testGetRead(): void
    {
        $this->assertSame(['user:123', 'team:123'], $this->document->getRead());
        $this->assertSame([], $this->empty->getRead());
    }

    public function testGetUpdate(): void
    {
        $this->assertSame(['any', 'user:updater'], $this->document->getUpdate());
        $this->assertSame([], $this->empty->getUpdate());
    }

    public function testGetDelete(): void
    {
        $this->assertSame(['any', 'user:deleter'], $this->document->getDelete());
        $this->assertSame([], $this->empty->getDelete());
    }

    public function testGetPermissionByType(): void
    {
        $this->assertSame(['any','user:creator'], $this->document->getPermissionsByType(Database::PERMISSION_CREATE));
        $this->assertSame([], $this->empty->getPermissionsByType(Database::PERMISSION_CREATE));

        $this->assertSame(['user:123','team:123'], $this->document->getPermissionsByType(Database::PERMISSION_READ));
        $this->assertSame([], $this->empty->getPermissionsByType(Database::PERMISSION_READ));

        $this->assertSame(['any','user:updater'], $this->document->getPermissionsByType(Database::PERMISSION_UPDATE));
        $this->assertSame([], $this->empty->getPermissionsByType(Database::PERMISSION_UPDATE));

        $this->assertSame(['any','user:deleter'], $this->document->getPermissionsByType(Database::PERMISSION_DELETE));
        $this->assertSame([], $this->empty->getPermissionsByType(Database::PERMISSION_DELETE));
    }

    public function testGetPermissions(): void
    {
        $this->assertSame([
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

    public function testGetAttributes(): void
    {
        $this->assertSame([
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

    public function testGetAttribute(): void
    {
        $this->assertSame('This is a test.', $this->document->getAttribute('title', ''));
        $this->assertSame('', $this->document->getAttribute('titlex', ''));
    }

    public function testSetAttribute(): void
    {
        $this->assertSame('This is a test.', $this->document->getAttribute('title', ''));
        $this->assertSame(['one'], $this->document->getAttribute('list', []));
        $this->assertSame('', $this->document->getAttribute('titlex', ''));

        $this->document->setAttribute('title', 'New title');

        $this->assertSame('New title', $this->document->getAttribute('title', ''));
        $this->assertSame('', $this->document->getAttribute('titlex', ''));

        $this->document->setAttribute('list', 'two', Document::SET_TYPE_APPEND);
        $this->assertSame(['one', 'two'], $this->document->getAttribute('list', []));

        $this->document->setAttribute('list', 'zero', Document::SET_TYPE_PREPEND);
        $this->assertSame(['zero', 'one', 'two'], $this->document->getAttribute('list', []));

        $this->document->setAttribute('list', ['one'], Document::SET_TYPE_ASSIGN);
        $this->assertSame(['one'], $this->document->getAttribute('list', []));
    }

    public function testSetAttributes(): void
    {
        $document = new Document(['$id' => ID::custom(''), '$collection' => 'users']);

        $otherDocument = new Document([
            '$id' => ID::custom('new'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::user('new')),
                Permission::delete(Role::user('new')),
            ],
            'email' => 'joe@example.com',
            'prefs' => new \stdClass(),
        ]);

        $document->setAttributes($otherDocument->getArrayCopy());

        $this->assertSame($otherDocument->getId(), $document->getId());
        $this->assertSame('users', $document->getCollection());
        $this->assertSame($otherDocument->getPermissions(), $document->getPermissions());
        $this->assertSame($otherDocument->getAttribute('email'), $document->getAttribute('email'));
        $this->assertSame($otherDocument->getAttribute('prefs'), $document->getAttribute('prefs'));
    }

    public function testRemoveAttribute(): void
    {
        $this->document->removeAttribute('list');
        $this->assertSame([], $this->document->getAttribute('list', []));
    }

    public function testFind(): void
    {
        $this->assertSame(null, $this->document->find('find', 'one'));

        $this->document->setAttribute('findString', 'demo');
        $this->assertSame($this->document, $this->document->find('findString', 'demo'));

        $this->document->setAttribute('findArray', ['demo']);
        $this->assertSame(null, $this->document->find('findArray', 'demo'));
        $this->assertSame($this->document, $this->document->find('findArray', ['demo']));

        $this->assertSame($this->document->getAttribute('children')[0], $this->document->find('name', 'x', 'children'));
        $this->assertSame($this->document->getAttribute('children')[2], $this->document->find('name', 'z', 'children'));
        $this->assertSame(null, $this->document->find('name', 'v', 'children'));
    }

    public function testFindAndReplace(): void
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

        $this->assertSame(true, $document->findAndReplace('name', 'x', new Document(['name' => '1', 'test' => true]), 'children'));
        $this->assertSame('1', $document->getAttribute('children')[0]['name']);
        $this->assertSame(true, $document->getAttribute('children')[0]['test']);

        // Array with wrong value
        $this->assertSame(false, $document->findAndReplace('name', 'xy', new Document(['name' => '1', 'test' => true]), 'children'));

        // Array with wrong key
        $this->assertSame(false, $document->findAndReplace('namex', 'x', new Document(['name' => '1', 'test' => true]), 'children'));

        // No array
        $this->assertSame(true, $document->findAndReplace('title', 'This is a test.', 'new'));
        $this->assertSame('new', $document->getAttribute('title'));

        // No array with wrong value
        $this->assertSame(false, $document->findAndReplace('title', 'test', 'new'));

        // No array with wrong key
        $this->assertSame(false, $document->findAndReplace('titlex', 'This is a test.', 'new'));
    }

    public function testFindAndRemove(): void
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
        $this->assertSame(true, $document->findAndRemove('name', 'x', 'children'));
        $this->assertSame('y', $document->getAttribute('children')[1]['name']);
        $this->assertCount(2, $document->getAttribute('children'));

        // Array with wrong value
        $this->assertSame(false, $document->findAndRemove('name', 'xy', 'children'));

        // Array with wrong key
        $this->assertSame(false, $document->findAndRemove('namex', 'x', 'children'));

        // No array
        $this->assertSame(true, $document->findAndRemove('title', 'This is a test.'));
        $this->assertSame(false, $document->isset('title'));

        // No array with wrong value
        $this->assertSame(false, $document->findAndRemove('title', 'new'));

        // No array with wrong key
        $this->assertSame(false, $document->findAndRemove('titlex', 'This is a test.'));
    }

    public function testIsEmpty(): void
    {
        $this->assertSame(false, $this->document->isEmpty());
        $this->assertSame(true, $this->empty->isEmpty());
    }

    public function testIsSet(): void
    {
        $this->assertSame(false, $this->document->isSet('titlex'));
        $this->assertSame(false, $this->empty->isSet('titlex'));
        $this->assertSame(true, $this->document->isSet('title'));
    }

    public function testClone(): void
    {
        $before = new Document([
            'level' => 0,
            'name' => '_',
            'document' => new Document(['name' => 'zero']),
            'children' => [
                new Document([
                    'level' => 1,
                    'name' => 'a',
                    'document' => new Document(['name' => 'one']),
                    'children' => [
                        new Document([
                            'level' => 2,
                            'name' => 'x',
                            'document' => new Document(['name' => 'two']),
                            'children' => [
                                new Document([
                                    'level' => 3,
                                    'name' => 'i'
                                ]),
                            ]
                        ])
                    ]
                ])
            ]
        ]);

        $after = clone $before;

        $before->setAttribute('name', 'before');
        $before->getAttribute('document')->setAttribute('name', 'before_one');
        $before->getAttribute('children')[0]->setAttribute('name', 'before_a');
        $before->getAttribute('children')[0]->getAttribute('document')->setAttribute('name', 'before_two');
        $before->getAttribute('children')[0]->getAttribute('children')[0]->setAttribute('name', 'before_x');

        $this->assertSame('_', $after->getAttribute('name'));
        $this->assertSame('zero', $after->getAttribute('document')->getAttribute('name'));
        $this->assertSame('a', $after->getAttribute('children')[0]->getAttribute('name'));
        $this->assertSame('one', $after->getAttribute('children')[0]->getAttribute('document')->getAttribute('name'));
        $this->assertSame('x', $after->getAttribute('children')[0]->getAttribute('children')[0]->getAttribute('name'));
    }

    public function testGetArrayCopy(): void
    {
        $this->assertSame([
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
        $this->assertSame([], $this->empty->getArrayCopy());
    }

    public function testEmptyDocumentSequence(): void
    {
        $empty = new Document();

        $this->assertNull($empty->getSequence());
        $this->assertNotSame('', $empty->getSequence());
    }
}

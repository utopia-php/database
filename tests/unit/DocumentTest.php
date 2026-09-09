<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PermissionType;
use Utopia\Database\SetType;

class DocumentTest extends TestCase
{
    protected Document $document;

    protected Document $empty;

    protected string $id;

    protected string $collection;

    protected function setUp(): void
    {
        $this->id = uniqid();

        $this->collection = uniqid();

        $this->document = new Document([
            Document::ID => ID::custom($this->id),
            Document::COLLECTION => ID::custom($this->collection),
            Document::PERMISSIONS => [
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
                'one',
            ],
            'children' => [
                new Document(['name' => 'x']),
                new Document(['name' => 'y']),
                new Document(['name' => 'z']),
            ],
        ]);

        $this->empty = new Document();
    }

    protected function tearDown(): void
    {
    }

    public function test_document_nulls(): void
    {
        $data = [
            'cat' => null,
            'dog' => null, // last entry is null
        ];

        $document = new Document($data);

        $this->assertEquals(null, $document['cat']);
        $this->assertEquals(false, isset($document['cat']));
        $this->assertEquals('cat', $document->getAttribute('cat', 'cat'));

        $this->assertEquals(null, $document['dog']);
        $this->assertEquals(false, isset($document['dog']));
        $this->assertEquals('dog', $document->getAttribute('dog', 'dog'));
    }

    public function test_id(): void
    {
        $this->assertEquals($this->id, $this->document->getId());
        $this->assertEquals(null, $this->empty->getId());
    }

    public function test_non_string_id_throws(): void
    {
        $this->expectException(StructureException::class);
        $this->expectExceptionMessage(Document::ID.' must be of type string');

        new Document([
            Document::ID => 123,
        ]);
    }

    public function testFromRowCoercesNullIdToEmptyString(): void
    {
        $document = Document::fromRow([
            Document::ID => null,
            'name' => 'unmatched',
        ]);

        $this->assertSame('', $document->getId());
        $this->assertSame('unmatched', $document->getAttribute('name'));
    }

    public function testFromRowDropsPdoColumnIndexes(): void
    {
        $document = Document::fromRow([
            0 => 1,
            1 => 'migration',
            Document::ID => 'migration',
            'state' => 'pending',
            Document::SEQUENCE => '1',
        ]);

        $this->assertSame(['$id', 'state', '$sequence'], \array_keys($document->getArrayCopy()));
        $this->assertSame('migration', $document->getId());
        $this->assertSame('pending', $document->getAttribute('state'));
        $this->assertNull($document->getAttribute('0'));
    }

    public function test_id_and_collection_accessors(): void
    {
        $document = new Document([
            Document::ID => 'doc-1',
            Document::COLLECTION => 'users',
        ]);

        $this->assertSame('doc-1', $document->getId());
        $this->assertSame('users', $document->getCollection());
    }

    public function test_collection(): void
    {
        $this->assertEquals($this->collection, $this->document->getCollection());
        $this->assertEquals(null, $this->empty->getCollection());
    }

    public function test_get_create(): void
    {
        $this->assertEquals(['any', 'user:creator'], $this->document->getCreate());
        $this->assertEquals([], $this->empty->getCreate());
    }

    public function test_get_read(): void
    {
        $this->assertEquals(['user:123', 'team:123'], $this->document->getRead());
        $this->assertEquals([], $this->empty->getRead());
    }

    public function test_get_update(): void
    {
        $this->assertEquals(['any', 'user:updater'], $this->document->getUpdate());
        $this->assertEquals([], $this->empty->getUpdate());
    }

    public function test_get_delete(): void
    {
        $this->assertEquals(['any', 'user:deleter'], $this->document->getDelete());
        $this->assertEquals([], $this->empty->getDelete());
    }

    public function test_get_permission_by_type(): void
    {
        $this->assertEquals(['any', 'user:creator'], $this->document->getPermissionsByType(PermissionType::Create));
        $this->assertEquals([], $this->empty->getPermissionsByType(PermissionType::Create));

        $this->assertEquals(['user:123', 'team:123'], $this->document->getPermissionsByType(PermissionType::Read));
        $this->assertEquals([], $this->empty->getPermissionsByType(PermissionType::Read));

        $this->assertEquals(['any', 'user:updater'], $this->document->getPermissionsByType(PermissionType::Update));
        $this->assertEquals([], $this->empty->getPermissionsByType(PermissionType::Update));

        $this->assertEquals(['any', 'user:deleter'], $this->document->getPermissionsByType(PermissionType::Delete));
        $this->assertEquals([], $this->empty->getPermissionsByType(PermissionType::Delete));
    }

    public function test_get_permissions(): void
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

    public function test_get_attributes(): void
    {
        $this->assertEquals([
            'title' => 'This is a test.',
            'list' => [
                'one',
            ],
            'children' => [
                new Document(['name' => 'x']),
                new Document(['name' => 'y']),
                new Document(['name' => 'z']),
            ],
        ], $this->document->getAttributes());
    }

    public function test_get_attribute(): void
    {
        $this->assertEquals('This is a test.', $this->document->getAttribute('title', ''));
        $this->assertEquals('', $this->document->getAttribute('titlex', ''));
    }

    public function test_set_attribute(): void
    {
        $this->assertEquals('This is a test.', $this->document->getAttribute('title', ''));
        $this->assertEquals(['one'], $this->document->getAttribute('list', []));
        $this->assertEquals('', $this->document->getAttribute('titlex', ''));

        $this->document->setAttribute('title', 'New title');

        $this->assertEquals('New title', $this->document->getAttribute('title', ''));
        $this->assertEquals('', $this->document->getAttribute('titlex', ''));

        $this->document->setAttribute('list', 'two', SetType::Append);
        $this->assertEquals(['one', 'two'], $this->document->getAttribute('list', []));

        $this->document->setAttribute('list', 'zero', SetType::Prepend);
        $this->assertEquals(['zero', 'one', 'two'], $this->document->getAttribute('list', []));

        $this->document->setAttribute('list', ['one'], SetType::Assign);
        $this->assertEquals(['one'], $this->document->getAttribute('list', []));
    }

    public function test_set_attributes(): void
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

        $this->assertEquals($otherDocument->getId(), $document->getId());
        $this->assertEquals('users', $document->getCollection());
        $this->assertEquals($otherDocument->getPermissions(), $document->getPermissions());
        $this->assertEquals($otherDocument->getAttribute('email'), $document->getAttribute('email'));
        $this->assertEquals($otherDocument->getAttribute('prefs'), $document->getAttribute('prefs'));
    }

    public function test_remove_attribute(): void
    {
        $this->document->removeAttribute('list');
        $this->assertEquals([], $this->document->getAttribute('list', []));
    }

    public function test_find(): void
    {
        $this->assertEquals(null, $this->document->find('find', 'one'));

        $this->document->setAttribute('findString', 'demo');
        $this->assertEquals($this->document, $this->document->find('findString', 'demo'));

        $this->document->setAttribute('findArray', ['demo']);
        $this->assertEquals(null, $this->document->find('findArray', 'demo'));
        $this->assertEquals($this->document, $this->document->find('findArray', ['demo']));

        /** @var array<Document> $children */
        $children = $this->document->getAttribute('children');
        $this->assertEquals($children[0], $this->document->find('name', 'x', 'children'));
        $this->assertEquals($children[2], $this->document->find('name', 'z', 'children'));
        $this->assertEquals(null, $this->document->find('name', 'v', 'children'));
    }

    public function test_find_and_replace(): void
    {
        $id = $this->id;
        $collection = $this->collection;

        $document = new Document([
            '$id' => ID::custom($id),
            '$collection' => ID::custom($collection),
            '$permissions' => [
                Permission::read(Role::user(ID::custom('123'))),
                Permission::read(Role::team(ID::custom('123'))),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'title' => 'This is a test.',
            'list' => [
                'one',
            ],
            'children' => [
                new Document(['name' => 'x']),
                new Document(['name' => 'y']),
                new Document(['name' => 'z']),
            ],
        ]);

        $this->assertEquals(true, $document->findAndReplace('name', 'x', new Document(['name' => '1', 'test' => true]), 'children'));
        /** @var array<array<string, mixed>> $children */
        $children = $document->getAttribute('children');
        $this->assertEquals('1', $children[0]['name']);
        $this->assertEquals(true, $children[0]['test']);

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

    public function test_find_and_remove(): void
    {
        $id = $this->id;
        $collection = $this->collection;

        $document = new Document([
            '$id' => ID::custom($id),
            '$collection' => ID::custom($collection),
            '$permissions' => [
                Permission::read(Role::user(ID::custom('123'))),
                Permission::read(Role::team(ID::custom('123'))),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'title' => 'This is a test.',
            'list' => [
                'one',
            ],
            'children' => [
                new Document(['name' => 'x']),
                new Document(['name' => 'y']),
                new Document(['name' => 'z']),
            ],
        ]);
        $this->assertEquals(true, $document->findAndRemove('name', 'x', 'children'));
        /** @var array<array<string, mixed>> $childrenAfterRemove */
        $childrenAfterRemove = $document->getAttribute('children');
        $this->assertEquals('y', $childrenAfterRemove[1]['name']);
        $this->assertCount(2, $childrenAfterRemove);

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

    public function test_is_empty(): void
    {
        $this->assertEquals(false, $this->document->isEmpty());
        $this->assertEquals(true, $this->empty->isEmpty());
    }

    public function test_is_set(): void
    {
        $this->assertEquals(false, $this->document->isSet('titlex'));
        $this->assertEquals(false, $this->empty->isSet('titlex'));
        $this->assertEquals(true, $this->document->isSet('title'));
    }

    public function test_clone(): void
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
                                    'name' => 'i',
                                ]),
                            ],
                        ]),
                    ],
                ]),
            ],
        ]);

        $after = clone $before;

        $before->setAttribute('name', 'before');
        /** @var Document $beforeDoc */
        $beforeDoc = $before->getAttribute('document');
        $beforeDoc->setAttribute('name', 'before_one');
        /** @var array<Document> $beforeChildren */
        $beforeChildren = $before->getAttribute('children');
        $beforeChildren[0]->setAttribute('name', 'before_a');
        /** @var Document $beforeChildDoc */
        $beforeChildDoc = $beforeChildren[0]->getAttribute('document');
        $beforeChildDoc->setAttribute('name', 'before_two');
        /** @var array<Document> $beforeChildChildren */
        $beforeChildChildren = $beforeChildren[0]->getAttribute('children');
        $beforeChildChildren[0]->setAttribute('name', 'before_x');

        $this->assertEquals('_', $after->getAttribute('name'));
        /** @var Document $afterDoc */
        $afterDoc = $after->getAttribute('document');
        $this->assertEquals('zero', $afterDoc->getAttribute('name'));
        /** @var array<Document> $afterChildren */
        $afterChildren = $after->getAttribute('children');
        $this->assertEquals('a', $afterChildren[0]->getAttribute('name'));
        /** @var Document $afterChildDoc */
        $afterChildDoc = $afterChildren[0]->getAttribute('document');
        $this->assertEquals('one', $afterChildDoc->getAttribute('name'));
        /** @var array<Document> $afterChildChildren */
        $afterChildChildren = $afterChildren[0]->getAttribute('children');
        $this->assertEquals('x', $afterChildChildren[0]->getAttribute('name'));
    }

    public function test_get_array_copy(): void
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
                'one',
            ],
            'children' => [
                ['name' => 'x'],
                ['name' => 'y'],
                ['name' => 'z'],
            ],
        ], $this->document->getArrayCopy());
        $this->assertEquals([], $this->empty->getArrayCopy());
    }

    public function test_empty_document_sequence(): void
    {
        $empty = new Document();

        $this->assertNull($empty->getSequence());
        $this->assertNotSame('', $empty->getSequence());
    }

    public function testConstructionPreservesScalarArraysAndConvertsOnlyDocuments(): void
    {
        $object = new \stdClass();
        $input = [
            'empty' => [],
            'values' => [7 => 'text', 'null' => null, 'bool' => false, 'object' => $object],
            'child' => ['$id' => 'child', 'name' => 'nested'],
            'children' => ['first' => ['$id' => 'first'], 9 => 'plain'],
        ];
        $document = new Document($input);

        $child = $document->getAttribute('child');
        $children = $document->getArray('children');

        $this->assertSame([], $document->getAttribute('empty'));
        $this->assertSame($input['values'], $document->getAttribute('values'));
        $this->assertInstanceOf(Document::class, $child);
        $this->assertSame('child', $child->getId());
        $this->assertInstanceOf(Document::class, $children['first']);
        $this->assertSame('first', $children['first']->getId());
        $this->assertSame('plain', $children[9]);
        $this->assertSame($input['children']['first'], $children['first']->getArrayCopy());
    }

    public function testArrayCopyPreservesKeysAndFiltersNestedDocuments(): void
    {
        $document = new Document([
            'name' => 'parent',
            'secret' => 'hidden',
            'values' => [7 => 'seven', 'null' => null, 'empty' => []],
            'children' => ['child' => new Document(['name' => 'nested', 'secret' => 'hidden'])],
        ]);
        $copy = $document->getArrayCopy(['name', 'secret', 'values', 'children'], ['secret']);

        $this->assertSame([
            'name' => 'parent',
            'values' => [7 => 'seven', 'null' => null, 'empty' => []],
            'children' => ['child' => ['name' => 'nested']],
        ], $copy);
        $copy['values'][7] = 'changed';
        $copy['children']['child']['name'] = 'changed';

        $nested = $document->getArray('children')['child'];

        $this->assertSame('seven', $document->getArray('values')[7]);
        $this->assertInstanceOf(Document::class, $nested);
        $this->assertSame('nested', $nested->getAttribute('name'));
    }

    public function testClonePreservesScalarKeysAndIsolatesNestedDocuments(): void
    {
        $object = new \stdClass();
        $original = new Document([
            'empty' => [],
            'values' => [7 => 'seven', 'object' => $object],
            'children' => ['child' => new Document(['name' => 'nested']), 9 => 'plain'],
        ]);
        $copy = clone $original;
        $this->assertIsArray($copy['values']);
        $copy['values'][7] = 'changed';

        $copiedChild = $copy->getArray('children')['child'];
        $this->assertInstanceOf(Document::class, $copiedChild);
        $copiedChild->setAttribute('name', 'changed');

        $originalChild = $original->getArray('children')['child'];

        $this->assertSame([], $copy->getAttribute('empty'));
        $this->assertSame([7, 'object'], array_keys($copy->getArray('values')));
        $this->assertSame($object, $copy->getArray('values')['object']);
        $this->assertSame('seven', $original->getArray('values')[7]);
        $this->assertInstanceOf(Document::class, $originalChild);
        $this->assertSame('nested', $originalChild->getAttribute('name'));
        $this->assertSame('plain', $copy->getArray('children')[9]);
    }

    public function testArrayCopyAndCloneDetachReferencedArrayElements(): void
    {
        $scalar = 'before';
        $nested = ['value' => 'before'];
        $document = new Document(['values' => ['first' => &$scalar, 7 => &$nested, 'last' => false]]);
        $export = $document->getArrayCopy();
        $clone = clone $document;
        $scalar = 'after';
        $nested['value'] = 'after';

        $expected = ['first' => 'before', 7 => ['value' => 'before'], 'last' => false];
        $this->assertSame($expected, $export['values']);
        $this->assertSame($expected, $clone->getAttribute('values'));
        $this->assertSame('after', $document->getArray('values')['first']);
    }

    /**
     * getArrayCopy() must not wrap a scalar array's elements in references.
     *
     * That is a pure allocation property, and deliberately measured as one.
     * PHP unwraps a reference whose refcount is 1 when the array is copied,
     * so an export whose elements were wrapped is indistinguishable from a
     * clean one through every userland probe: write-through in either
     * direction, ReflectionReference::fromArrayElement(), var_dump() and
     * serialize() all report the wrapped array as unwrapped. The detachment
     * assertions in this file therefore do not cover it, and there is no
     * behavioural assertion that would.
     */
    public function testScalarArrayExportAvoidsReferenceAllocationOverhead(): void
    {
        $values = range(1, 100_000);
        $document = new Document(['values' => $values]);

        // What one honest copy of this array costs on this build, measured in
        // the same process, so the bound below is a multiple of the allocator
        // in front of it rather than a byte count tuned to one platform.
        memory_reset_peak_usage();
        $mark = memory_get_usage();
        $plain = $values;
        $plain[0] = 0;
        $plainCost = memory_get_peak_usage() - $mark;
        unset($plain);

        memory_reset_peak_usage();
        $mark = memory_get_usage();
        $copy = $document->getArrayCopy();
        $exportCost = memory_get_peak_usage() - $mark;

        $this->assertIsArray($copy['values']);
        $this->assertCount(100_000, $copy['values']);
        $this->assertGreaterThan(0, $plainCost, 'The allocator reported no cost for a plain copy, so the ratio below is meaningless');
        $this->assertLessThan(
            2 * $plainCost,
            $exportCost,
            \sprintf(
                'Export allocated %d bytes where a plain copy of the same array costs %d: elements are being wrapped in references.',
                $exportCost,
                $plainCost,
            ),
        );
        $copy['values'][0] = 0;
        $this->assertSame(1, $document->getArray('values')[0]);
    }
}

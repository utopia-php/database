<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\Authorization;

abstract class Base extends TestCase
{
    /**
     * @return Adapter
     */
    abstract static protected function getDatabase(): Database;

    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
        Authorization::reset();
    }

    public function testCreateDelete()
    {
        $this->assertEquals(true, static::getDatabase()->create());
        $this->assertEquals(true, static::getDatabase()->delete());
        $this->assertEquals(true, static::getDatabase()->create());
    }

    /**
     * @depends testCreateDelete
     */
    public function testCreateDeleteCollection()
    {
        $this->assertInstanceOf('Utopia\Database\Document', static::getDatabase()->createCollection('actors'));
        $this->assertEquals(true, static::getDatabase()->deleteCollection('actors'));
    }

    public function testCreateDeleteAttribute()
    {
        static::getDatabase()->createCollection('attributes');

        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string1', Database::VAR_STRING, 128));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string2', Database::VAR_STRING, 16383+1));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string3', Database::VAR_STRING, 65535+1));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string4', Database::VAR_STRING, 16777215+1));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer', Database::VAR_INTEGER, 0));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float', Database::VAR_FLOAT, 0));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean', Database::VAR_BOOLEAN, 0));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(7, $collection->getAttribute('attributes'));

        // Array
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string_list', Database::VAR_STRING, 128, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer_list', Database::VAR_INTEGER, 0, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float_list', Database::VAR_FLOAT, 0, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean_list', Database::VAR_BOOLEAN, 0, true, true));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(11, $collection->getAttribute('attributes'));

        // Delete
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string1'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string2'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string3'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string4'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(4, $collection->getAttribute('attributes'));

        // Delete Array
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean_list'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(0, $collection->getAttribute('attributes'));

        static::getDatabase()->deleteCollection('attributes');
    }

    public function testCreateDeleteIndex()
    {
        static::getDatabase()->createCollection('indexes');

        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'string', Database::VAR_STRING, 128));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'integer', Database::VAR_INTEGER, 0));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'float', Database::VAR_FLOAT, 0));
        $this->assertEquals(true, static::getDatabase()->createAttribute('indexes', 'boolean', Database::VAR_BOOLEAN, 0));

        // Indexes
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index2', Database::INDEX_KEY, ['float', 'integer'], [], [Database::ORDER_ASC, Database::ORDER_DESC]));
        $this->assertEquals(true, static::getDatabase()->createIndex('indexes', 'index3', Database::INDEX_KEY, ['integer', 'boolean'], [], [Database::ORDER_ASC, Database::ORDER_DESC, Database::ORDER_DESC]));
        
        $collection = static::getDatabase()->getCollection('indexes');
        $this->assertCount(3, $collection->getAttribute('indexes'));

        // Delete Indexes
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index2'));
        $this->assertEquals(true, static::getDatabase()->deleteIndex('indexes', 'index3'));

        $collection = static::getDatabase()->getCollection('indexes');
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->deleteCollection('indexes');
    }

    public function testCreateDocument()
    {
        static::getDatabase()->createCollection('documents');

        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'string', Database::VAR_STRING, 128));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'integer', Database::VAR_INTEGER, 0));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'float', Database::VAR_FLOAT, 0));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'boolean', Database::VAR_BOOLEAN, 0));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'colors', Database::VAR_STRING, 32, true, true));
        
        $document = static::getDatabase()->createDocument('documents', new Document([
            '$read' => ['*', 'user1', 'user2'],
            '$write' => ['*', 'user1x', 'user2x'],
            'string' => 'text📝',
            'integer' => 5,
            'float' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $this->assertNotEmpty(true, $document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
        $this->assertIsInt($document->getAttribute('integer'));
        $this->assertEquals(5, $document->getAttribute('integer'));
        $this->assertIsFloat($document->getAttribute('float'));
        $this->assertEquals(5.55, $document->getAttribute('float'));
        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(true, $document->getAttribute('boolean'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testGetDocument(Document $document)
    {
        $document = static::getDatabase()->getDocument('documents', $document->getId());

        $this->assertNotEmpty(true, $document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer'));
        $this->assertEquals(5, $document->getAttribute('integer'));
        $this->assertIsFloat($document->getAttribute('float'));
        $this->assertEquals(5.55, $document->getAttribute('float'));
        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(true, $document->getAttribute('boolean'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));

        return $document;
    }

    /**
     * @depends testGetDocument
     */
    public function testUpdateDocument(Document $document)
    {
        $document
            ->setAttribute('string', 'text📝 updated')
            ->setAttribute('integer', 6)
            ->setAttribute('float', 5.56)
            ->setAttribute('boolean', false)
            ->setAttribute('colors', 'red', Document::SET_TYPE_APPEND)
        ;

        $new = $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);

        $this->assertNotEmpty(true, $new->getId());
        $this->assertIsString($new->getAttribute('string'));
        $this->assertEquals('text📝 updated', $new->getAttribute('string'));
        $this->assertIsInt($new->getAttribute('integer'));
        $this->assertEquals(6, $new->getAttribute('integer'));
        $this->assertIsFloat($new->getAttribute('float'));
        $this->assertEquals(5.56, $new->getAttribute('float'));
        $this->assertIsBool($new->getAttribute('boolean'));
        $this->assertEquals(false, $new->getAttribute('boolean'));
        $this->assertIsArray($new->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue', 'red'], $new->getAttribute('colors'));

        return $document;
    }

    /**
     * @depends testUpdateDocument
     */
    public function testDeleteDocument(Document $document)
    {
        $result = $this->getDatabase()->deleteDocument($document->getCollection(), $document->getId());
        $document = $this->getDatabase()->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $result);
        $this->assertEquals(true, $document->isEmpty());
    }

    // public function testCreateDocument()
    // {
    //     $collection1 = self::$database->createDocument(Database::COLLECTIONS, [
    //         '$collection' => Database::COLLECTIONS,
    //         '$permissions' => ['read' => ['*']],
    //         'name' => 'Create Documents',
    //         'rules' => [
    //             [
    //                 '$collection' => Database::COLLECTION_RULES,
    //                 '$permissions' => ['read' => ['*']],
    //                 'label' => 'Name',
    //                 'key' => 'name',
    //                 'type' => Database::VAR_STRING,
    //                 'default' => '',
    //                 'required' => true,
    //                 'array' => false,
    //             ],
    //             [
    //                 '$collection' => Database::COLLECTION_RULES,
    //                 '$permissions' => ['read' => ['*']],
    //                 'label' => 'Links',
    //                 'key' => 'links',
    //                 'type' => Database::VAR_STRING,
    //                 'default' => '',
    //                 'required' => true,
    //                 'array' => true,
    //             ],
    //         ]
    //     ]);

    //     $this->assertEquals(true, self::$database->createCollection($collection1->getId(), [], []));
        
    //     $document0 = new Document([
    //         '$collection' => $collection1->getId(),
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'name' => 'Task #0',
    //         'links' => [
    //             'http://example.com/link-1',
    //             'http://example.com/link-2',
    //             'http://example.com/link-3',
    //             'http://example.com/link-4',
    //         ],
    //     ]);
        
    //     $document1 = self::$database->createDocument($collection1->getId(), [
    //         '$collection' => $collection1->getId(),
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'name' => 'Task #1️⃣',
    //         'links' => [
    //             'http://example.com/link-5',
    //             'http://example.com/link-6',
    //             'http://example.com/link-7',
    //             'http://example.com/link-8',
    //         ],
    //     ]);

    //     $this->assertNotEmpty($document1->getId());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertArrayHasKey('read', $document1->getPermissions());
    //     $this->assertArrayHasKey('write', $document1->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document1->getAttribute('name'));
    //     $this->assertCount(4, $document1->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document1->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document1->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document1->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document1->getAttribute('links')[3]);

    //     $document2 = self::$database->createDocument(Database::COLLECTION_USERS, [
    //         '$collection' => Database::COLLECTION_USERS,
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'email' => 'test@appwrite.io',
    //         'emailVerification' => false,
    //         'status' => 0,
    //         'password' => 'secrethash',
    //         'password-update' => \time(),
    //         'registration' => \time(),
    //         'reset' => false,
    //         'name' => 'Test',
    //     ]);

    //     $this->assertNotEmpty($document2->getId());
    //     $this->assertIsArray($document2->getPermissions());
    //     $this->assertArrayHasKey('read', $document2->getPermissions());
    //     $this->assertArrayHasKey('write', $document2->getPermissions());
    //     $this->assertEquals('test@appwrite.io', $document2->getAttribute('email'));
    //     $this->assertIsString($document2->getAttribute('email'));
    //     $this->assertEquals(0, $document2->getAttribute('status'));
    //     $this->assertIsInt($document2->getAttribute('status'));
    //     $this->assertEquals(false, $document2->getAttribute('emailVerification'));
    //     $this->assertIsBool($document2->getAttribute('emailVerification'));

    //     $document2 = self::$database->getDocument(Database::COLLECTION_USERS, $document2->getId());

    //     $this->assertNotEmpty($document2->getId());
    //     $this->assertIsArray($document2->getPermissions());
    //     $this->assertArrayHasKey('read', $document2->getPermissions());
    //     $this->assertArrayHasKey('write', $document2->getPermissions());
    //     $this->assertEquals('test@appwrite.io', $document2->getAttribute('email'));
    //     $this->assertIsString($document2->getAttribute('email'));
    //     $this->assertEquals(0, $document2->getAttribute('status'));
    //     $this->assertIsInt($document2->getAttribute('status'));
    //     $this->assertEquals(false, $document2->getAttribute('emailVerification'));
    //     $this->assertIsBool($document2->getAttribute('emailVerification'));

    //     $types = [
    //         Database::VAR_STRING,
    //         Database::VAR_NUMBER,
    //         Database::VAR_BOOLEAN,
    //         Database::VAR_DOCUMENT,
    //     ];

    //     $rules = [];

    //     foreach($types as $type) {
    //         $rules[] = [
    //             '$collection' => Database::COLLECTION_RULES,
    //             '$permissions' => ['read' => ['*']],
    //             'label' => ucfirst($type),
    //             'key' => $type,
    //             'type' => $type,
    //             'default' => null,
    //             'required' => true,
    //             'array' => false,
    //             'list' => ($type === Database::VAR_DOCUMENT) ? [$collection1->getId()] : [],
    //         ];

    //         $rules[] = [
    //             '$collection' => Database::COLLECTION_RULES,
    //             '$permissions' => ['read' => ['*']],
    //             'label' => ucfirst($type),
    //             'key' => $type.'s',
    //             'type' => $type,
    //             'default' => null,
    //             'required' => true,
    //             'array' => true,
    //             'list' => ($type === Database::VAR_DOCUMENT) ? [$collection1->getId()] : [],
    //         ];
    //     }

    //     $rules[] = [
    //         '$collection' => Database::COLLECTION_RULES,
    //         '$permissions' => ['read' => ['*']],
    //         'label' => 'document2',
    //         'key' => 'document2',
    //         'type' => Database::VAR_DOCUMENT,
    //         'default' => null,
    //         'required' => true,
    //         'array' => false,
    //         'list' => [$collection1->getId()],
    //     ];

    //     $rules[] = [
    //         '$collection' => Database::COLLECTION_RULES,
    //         '$permissions' => ['read' => ['*']],
    //         'label' => 'documents2',
    //         'key' => 'documents2',
    //         'type' => Database::VAR_DOCUMENT,
    //         'default' => null,
    //         'required' => true,
    //         'array' => true,
    //         'list' => [$collection1->getId()],
    //     ];

    //     $collection2 = self::$database->createDocument(Database::COLLECTIONS, [
    //         '$collection' => Database::COLLECTIONS,
    //         '$permissions' => ['read' => ['*']],
    //         'name' => 'Create Documents',
    //         'rules' => $rules,
    //     ]);

    //     $this->assertEquals(true, self::$database->createCollection($collection2->getId(), [], []));
        
    //     $document3 = self::$database->createDocument($collection2->getId(), [
    //         '$collection' => $collection2->getId(),
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'text' => 'Hello World',
    //         'texts' => ['Hello World 1', 'Hello World 2'],
    //         // 'document' => $document0,
    //         // 'documents' => [$document0],
    //         'document' => $document0,
    //         'documents' => [$document1, $document0],
    //         'document2' => $document1,
    //         'documents2' => [$document0, $document1],
    //         'integer' => 1,
    //         'integers' => [5, 3, 4],
    //         'float' => 2.22,
    //         'floats' => [1.13, 4.33, 8.9999],
    //         'numeric' => 1,
    //         'numerics' => [1, 5, 7.77],
    //         'boolean' => true,
    //         'booleans' => [true, false, true],
    //         'email' => 'test@appwrite.io',
    //         'emails' => [
    //             'test4@appwrite.io',
    //             'test3@appwrite.io',
    //             'test2@appwrite.io',
    //             'test1@appwrite.io'
    //         ],
    //         'url' => 'http://example.com/welcome',
    //         'urls' => [
    //             'http://example.com/welcome-1',
    //             'http://example.com/welcome-2',
    //             'http://example.com/welcome-3'
    //         ],
    //         'ipv4' => '172.16.254.1',
    //         'ipv4s' => [
    //             '172.16.254.1',
    //             '172.16.254.5'
    //         ],
    //         'ipv6' => '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    //         'ipv6s' => [
    //             '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    //             '2001:0db8:85a3:0000:0000:8a2e:0370:7337'
    //         ],
    //         'key' => uniqid(),
    //         'keys' => [uniqid(), uniqid(), uniqid()],
    //     ]);

    //     $document3 = self::$database->getDocument($collection2->getId(), $document3->getId());

    //     $this->assertIsString($document3->getId());
    //     $this->assertIsString($document3->getCollection());
    //     $this->assertEquals([
    //         'read' => ['*'],
    //         'write' => ['user:123'],
    //     ], $document3->getPermissions());
    //     $this->assertEquals('Hello World', $document3->getAttribute('text'));
    //     $this->assertCount(2, $document3->getAttribute('texts'));
        
    //     $this->assertIsString($document3->getAttribute('text'));
    //     $this->assertEquals('Hello World', $document3->getAttribute('text'));
    //     $this->assertEquals(['Hello World 1', 'Hello World 2'], $document3->getAttribute('texts'));
    //     $this->assertCount(2, $document3->getAttribute('texts'));
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('document'));
    //     $this->assertIsString($document3->getAttribute('document')->getId());
    //     $this->assertNotEmpty($document3->getAttribute('document')->getId());
    //     $this->assertIsArray($document3->getAttribute('document')->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('document')->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('document')->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('document')->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('document')->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('document')->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('document')->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('document')->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('document')->getAttribute('links')[3]);
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents')[0]);
    //     $this->assertIsString($document3->getAttribute('documents')[0]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents')[0]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document3->getAttribute('documents')[0]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents')[0]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document3->getAttribute('documents')[0]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document3->getAttribute('documents')[0]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document3->getAttribute('documents')[0]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document3->getAttribute('documents')[0]->getAttribute('links')[3]);
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents')[1]);
    //     $this->assertIsString($document3->getAttribute('documents')[1]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents')[1]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('documents')[1]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents')[1]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('documents')[1]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('documents')[1]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('documents')[1]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('documents')[1]->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('document2'));
    //     $this->assertIsString($document3->getAttribute('document2')->getId());
    //     $this->assertNotEmpty($document3->getAttribute('document2')->getId());
    //     $this->assertIsArray($document3->getAttribute('document2')->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('document2')->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('document2')->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document3->getAttribute('document2')->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('document2')->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document3->getAttribute('document2')->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document3->getAttribute('document2')->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document3->getAttribute('document2')->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document3->getAttribute('document2')->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents2')[0]);
    //     $this->assertIsString($document3->getAttribute('documents2')[0]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents2')[0]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('documents2')[0]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents2')[0]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('documents2')[0]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('documents2')[0]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('documents2')[0]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('documents2')[0]->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents2')[1]);
    //     $this->assertIsString($document3->getAttribute('documents2')[1]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents2')[1]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document3->getAttribute('documents2')[1]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents2')[1]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document3->getAttribute('documents2')[1]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document3->getAttribute('documents2')[1]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document3->getAttribute('documents2')[1]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document3->getAttribute('documents2')[1]->getAttribute('links')[3]);
        
    //     $this->assertIsInt($document3->getAttribute('integer'));
    //     $this->assertEquals(1, $document3->getAttribute('integer'));
    //     $this->assertIsInt($document3->getAttribute('integers')[0]);
    //     $this->assertIsInt($document3->getAttribute('integers')[1]);
    //     $this->assertIsInt($document3->getAttribute('integers')[2]);
    //     $this->assertEquals([5, 3, 4], $document3->getAttribute('integers'));
    //     $this->assertCount(3, $document3->getAttribute('integers'));

    //     $this->assertIsFloat($document3->getAttribute('float'));
    //     $this->assertEquals(2.22, $document3->getAttribute('float'));
    //     $this->assertIsFloat($document3->getAttribute('floats')[0]);
    //     $this->assertIsFloat($document3->getAttribute('floats')[1]);
    //     $this->assertIsFloat($document3->getAttribute('floats')[2]);
    //     $this->assertEquals([1.13, 4.33, 8.9999], $document3->getAttribute('floats'));
    //     $this->assertCount(3, $document3->getAttribute('floats'));

    //     $this->assertIsBool($document3->getAttribute('boolean'));
    //     $this->assertEquals(true, $document3->getAttribute('boolean'));
    //     $this->assertIsBool($document3->getAttribute('booleans')[0]);
    //     $this->assertIsBool($document3->getAttribute('booleans')[1]);
    //     $this->assertIsBool($document3->getAttribute('booleans')[2]);
    //     $this->assertEquals([true, false, true], $document3->getAttribute('booleans'));
    //     $this->assertCount(3, $document3->getAttribute('booleans'));

    //     $this->assertIsString($document3->getAttribute('email'));
    //     $this->assertEquals('test@appwrite.io', $document3->getAttribute('email'));
    //     $this->assertIsString($document3->getAttribute('emails')[0]);
    //     $this->assertIsString($document3->getAttribute('emails')[1]);
    //     $this->assertIsString($document3->getAttribute('emails')[2]);
    //     $this->assertIsString($document3->getAttribute('emails')[3]);
    //     $this->assertEquals([
    //         'test4@appwrite.io',
    //         'test3@appwrite.io',
    //         'test2@appwrite.io',
    //         'test1@appwrite.io'
    //     ], $document3->getAttribute('emails'));
    //     $this->assertCount(4, $document3->getAttribute('emails'));

    //     $this->assertIsString($document3->getAttribute('url'));
    //     $this->assertEquals('http://example.com/welcome', $document3->getAttribute('url'));
    //     $this->assertIsString($document3->getAttribute('urls')[0]);
    //     $this->assertIsString($document3->getAttribute('urls')[1]);
    //     $this->assertIsString($document3->getAttribute('urls')[2]);
    //     $this->assertEquals([
    //         'http://example.com/welcome-1',
    //         'http://example.com/welcome-2',
    //         'http://example.com/welcome-3'
    //     ], $document3->getAttribute('urls'));
    //     $this->assertCount(3, $document3->getAttribute('urls'));

    //     $this->assertIsString($document3->getAttribute('ipv4'));
    //     $this->assertEquals('172.16.254.1', $document3->getAttribute('ipv4'));
    //     $this->assertIsString($document3->getAttribute('ipv4s')[0]);
    //     $this->assertIsString($document3->getAttribute('ipv4s')[1]);
    //     $this->assertEquals([
    //         '172.16.254.1',
    //         '172.16.254.5'
    //     ], $document3->getAttribute('ipv4s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv4s'));

    //     $this->assertIsString($document3->getAttribute('ipv6'));
    //     $this->assertEquals('2001:0db8:85a3:0000:0000:8a2e:0370:7334', $document3->getAttribute('ipv6'));
    //     $this->assertIsString($document3->getAttribute('ipv6s')[0]);
    //     $this->assertIsString($document3->getAttribute('ipv6s')[1]);
    //     $this->assertEquals([
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7337'
    //     ], $document3->getAttribute('ipv6s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv6s'));

    //     $this->assertEquals('2001:0db8:85a3:0000:0000:8a2e:0370:7334', $document3->getAttribute('ipv6'));
    //     $this->assertEquals([
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7337'
    //     ], $document3->getAttribute('ipv6s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv6s'));

    //     $this->assertIsString($document3->getAttribute('key'));
    //     $this->assertCount(3, $document3->getAttribute('keys'));
    // }

    // public function testGetDocument()
    // {
    //     // Mocked document
    //     $document = self::$database->getDocument(Database::COLLECTIONS, Database::COLLECTION_USERS);

    //     $this->assertEquals(Database::COLLECTION_USERS, $document->getId());
    //     $this->assertEquals(Database::COLLECTIONS, $document->getCollection());

    //     $collection1 = self::$database->createDocument(Database::COLLECTIONS, [
    //         '$collection' => Database::COLLECTIONS,
    //         '$permissions' => ['read' => ['*']],
    //         'name' => 'Create Documents',
    //         'rules' => [
    //             [
    //                 '$collection' => Database::COLLECTION_RULES,
    //                 '$permissions' => ['read' => ['*']],
    //                 'label' => 'Name',
    //                 'key' => 'name',
    //                 'type' => Database::VAR_STRING,
    //                 'default' => '',
    //                 'required' => true,
    //                 'array' => false,
    //             ],
    //             [
    //                 '$collection' => Database::COLLECTION_RULES,
    //                 '$permissions' => ['read' => ['*']],
    //                 'label' => 'Links',
    //                 'key' => 'links',
    //                 'type' => Database::VAR_STRING,
    //                 'default' => '',
    //                 'required' => true,
    //                 'array' => true,
    //             ],
    //         ]
    //     ]);

    //     $this->assertEquals(true, self::$database->createCollection($collection1->getId(), [], []));
        
    //     $document1 = self::$database->createDocument($collection1->getId(), [
    //         '$collection' => $collection1->getId(),
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'name' => 'Task #1️⃣',
    //         'links' => [
    //             'http://example.com/link-5',
    //             'http://example.com/link-6',
    //             'http://example.com/link-7',
    //             'http://example.com/link-8',
    //         ],
    //     ]);

    //     $document1 = self::$database->getDocument($collection1->getId(), $document1->getId());

    //     $this->assertFalse($document1->isEmpty());
    //     $this->assertNotEmpty($document1->getId());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertArrayHasKey('read', $document1->getPermissions());
    //     $this->assertArrayHasKey('write', $document1->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document1->getAttribute('name'));
    //     $this->assertCount(4, $document1->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document1->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document1->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document1->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document1->getAttribute('links')[3]);

    //     $document1 = self::$database->getDocument($collection1->getId(), $document1->getId().'x');

    //     $this->assertTrue($document1->isEmpty());
    //     $this->assertEmpty($document1->getId());
    //     $this->assertEmpty($document1->getCollection());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertEmpty($document1->getPermissions());
    // }

    // public function testUpdateDocument()
    // {
    //     $collection1 = self::$database->createDocument(Database::COLLECTIONS, [
    //         '$collection' => Database::COLLECTIONS,
    //         '$permissions' => ['read' => ['*']],
    //         'name' => 'Create Documents',
    //         'rules' => [
    //             [
    //                 '$collection' => Database::COLLECTION_RULES,
    //                 '$permissions' => ['read' => ['*']],
    //                 'label' => 'Name',
    //                 'key' => 'name',
    //                 'type' => Database::VAR_STRING,
    //                 'default' => '',
    //                 'required' => true,
    //                 'array' => false,
    //             ],
    //             [
    //                 '$collection' => Database::COLLECTION_RULES,
    //                 '$permissions' => ['read' => ['*']],
    //                 'label' => 'Links',
    //                 'key' => 'links',
    //                 'type' => Database::VAR_STRING,
    //                 'default' => '',
    //                 'required' => true,
    //                 'array' => true,
    //             ],
    //         ]
    //     ]);

    //     $this->assertEquals(true, self::$database->createCollection($collection1->getId(), [], []));
        
    //     $document0 = new Document([
    //         '$collection' => $collection1->getId(),
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'name' => 'Task #0',
    //         'links' => [
    //             'http://example.com/link-1',
    //             'http://example.com/link-2',
    //             'http://example.com/link-3',
    //             'http://example.com/link-4',
    //         ],
    //     ]);
        
    //     $document1 = self::$database->createDocument($collection1->getId(), [
    //         '$collection' => $collection1->getId(),
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'name' => 'Task #1️⃣',
    //         'links' => [
    //             'http://example.com/link-5',
    //             'http://example.com/link-6',
    //             'http://example.com/link-7',
    //             'http://example.com/link-8',
    //         ],
    //     ]);

    //     $this->assertNotEmpty($document1->getId());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertArrayHasKey('read', $document1->getPermissions());
    //     $this->assertArrayHasKey('write', $document1->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document1->getAttribute('name'));
    //     $this->assertCount(4, $document1->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document1->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document1->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document1->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document1->getAttribute('links')[3]);
        
    //     $document1 = self::$database->getDocument($collection1->getId(), $document1->getId());

    //     $this->assertNotEmpty($document1->getId());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertArrayHasKey('read', $document1->getPermissions());
    //     $this->assertArrayHasKey('write', $document1->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document1->getAttribute('name'));
    //     $this->assertCount(4, $document1->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document1->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document1->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document1->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document1->getAttribute('links')[3]);

    //     $document1 = self::$database->updateDocument($collection1->getId(), $document1->getId(), [
    //         '$id' => $document1->getId(),
    //         '$collection' => $collection1->getId(),
    //         '$permissions' => [
    //             'read' => ['user:1234'],
    //             'write' => ['user:1234'],
    //         ],
    //         'name' => 'Task #1x',
    //         'links' => [
    //             'http://example.com/link-5x',
    //             'http://example.com/link-6x',
    //             'http://example.com/link-7x',
    //             'http://example.com/link-8x',
    //         ],
    //     ]);

    //     $this->assertNotEmpty($document1->getId());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertArrayHasKey('read', $document1->getPermissions());
    //     $this->assertArrayHasKey('write', $document1->getPermissions());
    //     $this->assertEquals('Task #1x', $document1->getAttribute('name'));
    //     $this->assertCount(4, $document1->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5x', $document1->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6x', $document1->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7x', $document1->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8x', $document1->getAttribute('links')[3]);

    //     $document1 = self::$database->getDocument($collection1->getId(), $document1->getId());

    //     $this->assertNotEmpty($document1->getId());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertArrayHasKey('read', $document1->getPermissions());
    //     $this->assertArrayHasKey('write', $document1->getPermissions());
    //     $this->assertEquals('Task #1x', $document1->getAttribute('name'));
    //     $this->assertCount(4, $document1->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5x', $document1->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6x', $document1->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7x', $document1->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8x', $document1->getAttribute('links')[3]);

    //     $document2 = self::$database->createDocument(Database::COLLECTION_USERS, [
    //         '$collection' => Database::COLLECTION_USERS,
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'email' => 'test5@appwrite.io',
    //         'emailVerification' => false,
    //         'status' => 0,
    //         'password' => 'secrethash',
    //         'password-update' => \time(),
    //         'registration' => \time(),
    //         'reset' => false,
    //         'name' => 'Test',
    //     ]);

    //     $this->assertNotEmpty($document2->getId());
    //     $this->assertIsArray($document2->getPermissions());
    //     $this->assertArrayHasKey('read', $document2->getPermissions());
    //     $this->assertArrayHasKey('write', $document2->getPermissions());
    //     $this->assertEquals('test5@appwrite.io', $document2->getAttribute('email'));
    //     $this->assertIsString($document2->getAttribute('email'));
    //     $this->assertEquals(0, $document2->getAttribute('status'));
    //     $this->assertIsInt($document2->getAttribute('status'));
    //     $this->assertEquals(false, $document2->getAttribute('emailVerification'));
    //     $this->assertIsBool($document2->getAttribute('emailVerification'));

    //     $document2 = self::$database->getDocument(Database::COLLECTION_USERS, $document2->getId());

    //     $this->assertNotEmpty($document2->getId());
    //     $this->assertIsArray($document2->getPermissions());
    //     $this->assertArrayHasKey('read', $document2->getPermissions());
    //     $this->assertArrayHasKey('write', $document2->getPermissions());
    //     $this->assertEquals('test5@appwrite.io', $document2->getAttribute('email'));
    //     $this->assertIsString($document2->getAttribute('email'));
    //     $this->assertEquals(0, $document2->getAttribute('status'));
    //     $this->assertIsInt($document2->getAttribute('status'));
    //     $this->assertEquals(false, $document2->getAttribute('emailVerification'));
    //     $this->assertIsBool($document2->getAttribute('emailVerification'));

    //     $document2 = self::$database->updateDocument(Database::COLLECTION_USERS, $document2->getId(), [
    //         '$id' => $document2->getId(),
    //         '$collection' => Database::COLLECTION_USERS,
    //         '$permissions' => [
    //             'read' => ['user:1234'],
    //             'write' => ['user:1234'],
    //         ],
    //         'email' => 'test5x@appwrite.io',
    //         'emailVerification' => true,
    //         'status' => 1,
    //         'password' => 'secrethashx',
    //         'password-update' => \time(),
    //         'registration' => \time(),
    //         'reset' => true,
    //         'name' => 'Testx',
    //     ]);

    //     $this->assertNotEmpty($document2->getId());
    //     $this->assertIsArray($document2->getPermissions());
    //     $this->assertArrayHasKey('read', $document2->getPermissions());
    //     $this->assertArrayHasKey('write', $document2->getPermissions());
    //     $this->assertEquals('test5x@appwrite.io', $document2->getAttribute('email'));
    //     $this->assertIsString($document2->getAttribute('email'));
    //     $this->assertEquals(1, $document2->getAttribute('status'));
    //     $this->assertIsInt($document2->getAttribute('status'));
    //     $this->assertEquals(true, $document2->getAttribute('emailVerification'));
    //     $this->assertIsBool($document2->getAttribute('emailVerification'));

    //     $document2 = self::$database->getDocument(Database::COLLECTION_USERS, $document2->getId());

    //     $this->assertNotEmpty($document2->getId());
    //     $this->assertIsArray($document2->getPermissions());
    //     $this->assertArrayHasKey('read', $document2->getPermissions());
    //     $this->assertArrayHasKey('write', $document2->getPermissions());
    //     $this->assertEquals('test5x@appwrite.io', $document2->getAttribute('email'));
    //     $this->assertIsString($document2->getAttribute('email'));
    //     $this->assertEquals(1, $document2->getAttribute('status'));
    //     $this->assertIsInt($document2->getAttribute('status'));
    //     $this->assertEquals(true, $document2->getAttribute('emailVerification'));
    //     $this->assertIsBool($document2->getAttribute('emailVerification'));

    //     $types = [
    //         Database::VAR_STRING,
    //         Database::VAR_NUMBER,
    //         Database::VAR_BOOLEAN,
    //         Database::VAR_DOCUMENT,
    //     ];

    //     $rules = [];

    //     foreach($types as $type) {
    //         $rules[] = [
    //             '$collection' => Database::COLLECTION_RULES,
    //             '$permissions' => ['read' => ['*']],
    //             'label' => ucfirst($type),
    //             'key' => $type,
    //             'type' => $type,
    //             'default' => null,
    //             'required' => true,
    //             'array' => false,
    //             'list' => ($type === Database::VAR_DOCUMENT) ? [$collection1->getId()] : [],
    //         ];

    //         $rules[] = [
    //             '$collection' => Database::COLLECTION_RULES,
    //             '$permissions' => ['read' => ['*']],
    //             'label' => ucfirst($type),
    //             'key' => $type.'s',
    //             'type' => $type,
    //             'default' => null,
    //             'required' => true,
    //             'array' => true,
    //             'list' => ($type === Database::VAR_DOCUMENT) ? [$collection1->getId()] : [],
    //         ];
    //     }

    //     $rules[] = [
    //         '$collection' => Database::COLLECTION_RULES,
    //         '$permissions' => ['read' => ['*']],
    //         'label' => 'document2',
    //         'key' => 'document2',
    //         'type' => Database::VAR_DOCUMENT,
    //         'default' => null,
    //         'required' => true,
    //         'array' => false,
    //         'list' => [$collection1->getId()],
    //     ];

    //     $rules[] = [
    //         '$collection' => Database::COLLECTION_RULES,
    //         '$permissions' => ['read' => ['*']],
    //         'label' => 'documents2',
    //         'key' => 'documents2',
    //         'type' => Database::VAR_DOCUMENT,
    //         'default' => null,
    //         'required' => true,
    //         'array' => true,
    //         'list' => [$collection1->getId()],
    //     ];

    //     $collection2 = self::$database->createDocument(Database::COLLECTIONS, [
    //         '$collection' => Database::COLLECTIONS,
    //         '$permissions' => ['read' => ['*']],
    //         'name' => 'Create Documents',
    //         'rules' => $rules,
    //     ]);

    //     $this->assertEquals(true, self::$database->createCollection($collection2->getId(), [], []));
        
    //     $document3 = self::$database->createDocument($collection2->getId(), [
    //         '$collection' => $collection2->getId(),
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'text' => 'Hello World',
    //         'texts' => ['Hello World 1', 'Hello World 2'],
    //         // 'document' => $document0,
    //         // 'documents' => [$document0],
    //         'document' => $document0,
    //         'documents' => [$document1, $document0],
    //         'document2' => $document1,
    //         'documents2' => [$document0, $document1],
    //         'integer' => 1,
    //         'integers' => [5, 3, 4],
    //         'float' => 2.22,
    //         'floats' => [1.13, 4.33, 8.9999],
    //         'numeric' => 1,
    //         'numerics' => [1, 5, 7.77],
    //         'boolean' => true,
    //         'booleans' => [true, false, true],
    //         'email' => 'test@appwrite.io',
    //         'emails' => [
    //             'test4@appwrite.io',
    //             'test3@appwrite.io',
    //             'test2@appwrite.io',
    //             'test1@appwrite.io'
    //         ],
    //         'url' => 'http://example.com/welcome',
    //         'urls' => [
    //             'http://example.com/welcome-1',
    //             'http://example.com/welcome-2',
    //             'http://example.com/welcome-3'
    //         ],
    //         'ipv4' => '172.16.254.1',
    //         'ipv4s' => [
    //             '172.16.254.1',
    //             '172.16.254.5'
    //         ],
    //         'ipv6' => '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    //         'ipv6s' => [
    //             '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    //             '2001:0db8:85a3:0000:0000:8a2e:0370:7337'
    //         ],
    //         'key' => uniqid(),
    //         'keys' => [uniqid(), uniqid(), uniqid()],
    //     ]);

    //     $document3 = self::$database->getDocument($collection2->getId(), $document3->getId());

    //     $this->assertIsString($document3->getId());
    //     $this->assertIsString($document3->getCollection());
    //     $this->assertEquals([
    //         'read' => ['*'],
    //         'write' => ['user:123'],
    //     ], $document3->getPermissions());
    //     $this->assertEquals('Hello World', $document3->getAttribute('text'));
    //     $this->assertCount(2, $document3->getAttribute('texts'));
        
    //     $this->assertIsString($document3->getAttribute('text'));
    //     $this->assertEquals('Hello World', $document3->getAttribute('text'));
    //     $this->assertEquals(['Hello World 1', 'Hello World 2'], $document3->getAttribute('texts'));
    //     $this->assertCount(2, $document3->getAttribute('texts'));
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('document'));
    //     $this->assertIsString($document3->getAttribute('document')->getId());
    //     $this->assertNotEmpty($document3->getAttribute('document')->getId());
    //     $this->assertIsArray($document3->getAttribute('document')->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('document')->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('document')->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('document')->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('document')->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('document')->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('document')->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('document')->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('document')->getAttribute('links')[3]);
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents')[0]);
    //     $this->assertIsString($document3->getAttribute('documents')[0]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents')[0]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertEquals('Task #1x', $document3->getAttribute('documents')[0]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents')[0]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5x', $document3->getAttribute('documents')[0]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6x', $document3->getAttribute('documents')[0]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7x', $document3->getAttribute('documents')[0]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8x', $document3->getAttribute('documents')[0]->getAttribute('links')[3]);
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents')[1]);
    //     $this->assertIsString($document3->getAttribute('documents')[1]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents')[1]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('documents')[1]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents')[1]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('documents')[1]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('documents')[1]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('documents')[1]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('documents')[1]->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('document2'));
    //     $this->assertIsString($document3->getAttribute('document2')->getId());
    //     $this->assertNotEmpty($document3->getAttribute('document2')->getId());
    //     $this->assertIsArray($document3->getAttribute('document2')->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('document2')->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('document2')->getPermissions());
    //     $this->assertEquals('Task #1x', $document3->getAttribute('document2')->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('document2')->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5x', $document3->getAttribute('document2')->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6x', $document3->getAttribute('document2')->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7x', $document3->getAttribute('document2')->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8x', $document3->getAttribute('document2')->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents2')[0]);
    //     $this->assertIsString($document3->getAttribute('documents2')[0]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents2')[0]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('documents2')[0]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents2')[0]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('documents2')[0]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('documents2')[0]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('documents2')[0]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('documents2')[0]->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents2')[1]);
    //     $this->assertIsString($document3->getAttribute('documents2')[1]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents2')[1]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertEquals('Task #1x', $document3->getAttribute('documents2')[1]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents2')[1]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5x', $document3->getAttribute('documents2')[1]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6x', $document3->getAttribute('documents2')[1]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7x', $document3->getAttribute('documents2')[1]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8x', $document3->getAttribute('documents2')[1]->getAttribute('links')[3]);
        
    //     $this->assertIsInt($document3->getAttribute('integer'));
    //     $this->assertEquals(1, $document3->getAttribute('integer'));
    //     $this->assertIsInt($document3->getAttribute('integers')[0]);
    //     $this->assertIsInt($document3->getAttribute('integers')[1]);
    //     $this->assertIsInt($document3->getAttribute('integers')[2]);
    //     $this->assertEquals([5, 3, 4], $document3->getAttribute('integers'));
    //     $this->assertCount(3, $document3->getAttribute('integers'));

    //     $this->assertIsFloat($document3->getAttribute('float'));
    //     $this->assertEquals(2.22, $document3->getAttribute('float'));
    //     $this->assertIsFloat($document3->getAttribute('floats')[0]);
    //     $this->assertIsFloat($document3->getAttribute('floats')[1]);
    //     $this->assertIsFloat($document3->getAttribute('floats')[2]);
    //     $this->assertEquals([1.13, 4.33, 8.9999], $document3->getAttribute('floats'));
    //     $this->assertCount(3, $document3->getAttribute('floats'));

    //     $this->assertIsBool($document3->getAttribute('boolean'));
    //     $this->assertEquals(true, $document3->getAttribute('boolean'));
    //     $this->assertIsBool($document3->getAttribute('booleans')[0]);
    //     $this->assertIsBool($document3->getAttribute('booleans')[1]);
    //     $this->assertIsBool($document3->getAttribute('booleans')[2]);
    //     $this->assertEquals([true, false, true], $document3->getAttribute('booleans'));
    //     $this->assertCount(3, $document3->getAttribute('booleans'));

    //     $this->assertIsString($document3->getAttribute('email'));
    //     $this->assertEquals('test@appwrite.io', $document3->getAttribute('email'));
    //     $this->assertIsString($document3->getAttribute('emails')[0]);
    //     $this->assertIsString($document3->getAttribute('emails')[1]);
    //     $this->assertIsString($document3->getAttribute('emails')[2]);
    //     $this->assertIsString($document3->getAttribute('emails')[3]);
    //     $this->assertEquals([
    //         'test4@appwrite.io',
    //         'test3@appwrite.io',
    //         'test2@appwrite.io',
    //         'test1@appwrite.io'
    //     ], $document3->getAttribute('emails'));
    //     $this->assertCount(4, $document3->getAttribute('emails'));

    //     $this->assertIsString($document3->getAttribute('url'));
    //     $this->assertEquals('http://example.com/welcome', $document3->getAttribute('url'));
    //     $this->assertIsString($document3->getAttribute('urls')[0]);
    //     $this->assertIsString($document3->getAttribute('urls')[1]);
    //     $this->assertIsString($document3->getAttribute('urls')[2]);
    //     $this->assertEquals([
    //         'http://example.com/welcome-1',
    //         'http://example.com/welcome-2',
    //         'http://example.com/welcome-3'
    //     ], $document3->getAttribute('urls'));
    //     $this->assertCount(3, $document3->getAttribute('urls'));

    //     $this->assertIsString($document3->getAttribute('ipv4'));
    //     $this->assertEquals('172.16.254.1', $document3->getAttribute('ipv4'));
    //     $this->assertIsString($document3->getAttribute('ipv4s')[0]);
    //     $this->assertIsString($document3->getAttribute('ipv4s')[1]);
    //     $this->assertEquals([
    //         '172.16.254.1',
    //         '172.16.254.5'
    //     ], $document3->getAttribute('ipv4s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv4s'));

    //     $this->assertIsString($document3->getAttribute('ipv6'));
    //     $this->assertEquals('2001:0db8:85a3:0000:0000:8a2e:0370:7334', $document3->getAttribute('ipv6'));
    //     $this->assertIsString($document3->getAttribute('ipv6s')[0]);
    //     $this->assertIsString($document3->getAttribute('ipv6s')[1]);
    //     $this->assertEquals([
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7337'
    //     ], $document3->getAttribute('ipv6s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv6s'));

    //     $this->assertEquals('2001:0db8:85a3:0000:0000:8a2e:0370:7334', $document3->getAttribute('ipv6'));
    //     $this->assertEquals([
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7337'
    //     ], $document3->getAttribute('ipv6s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv6s'));

    //     $this->assertIsString($document3->getAttribute('key'));
    //     $this->assertCount(3, $document3->getAttribute('keys'));

    //     // Update

    //     $document3 = self::$database->updateDocument($collection2->getId(), $document3->getId(), [
    //         '$id' => $document3->getId(),
    //         '$collection' => $collection2->getId(),
    //         '$permissions' => [
    //             'read' => ['user:1234'],
    //             'write' => ['user:1234'],
    //         ],
    //         'text' => 'Hello Worldx',
    //         'texts' => ['Hello World 1x', 'Hello World 2x'],
    //         'document' => $document0,
    //         'documents' => [$document1, $document0],
    //         'document2' => $document1,
    //         'documents2' => [$document0, $document1],
    //         'integer' => 2,
    //         'integers' => [6, 4, 5],
    //         'float' => 3.22,
    //         'floats' => [2.13, 5.33, 9.9999],
    //         'numeric' => 2,
    //         'numerics' => [2, 6, 8.77],
    //         'boolean' => false,
    //         'booleans' => [false, true, false],
    //         'email' => 'testx@appwrite.io',
    //         'emails' => [
    //             'test4x@appwrite.io',
    //             'test3x@appwrite.io',
    //             'test2x@appwrite.io',
    //             'test1x@appwrite.io'
    //         ],
    //         'url' => 'http://example.com/welcomex',
    //         'urls' => [
    //             'http://example.com/welcome-1x',
    //             'http://example.com/welcome-2x',
    //             'http://example.com/welcome-3x'
    //         ],
    //         'ipv4' => '172.16.254.2',
    //         'ipv4s' => [
    //             '172.16.254.2',
    //             '172.16.254.6'
    //         ],
    //         'ipv6' => '2001:0db8:85a3:0000:0000:8a2e:0370:7335',
    //         'ipv6s' => [
    //             '2001:0db8:85a3:0000:0000:8a2e:0370:7335',
    //             '2001:0db8:85a3:0000:0000:8a2e:0370:7338'
    //         ],
    //         'key' => uniqid().'x',
    //         'keys' => [uniqid().'x', uniqid().'x', uniqid().'x'],
    //     ]);

    //     $document3 = self::$database->getDocument($collection2->getId(), $document3->getId());

    //     $this->assertIsString($document3->getId());
    //     $this->assertIsString($document3->getCollection());
    //     $this->assertEquals([
    //         'read' => ['user:1234'],
    //         'write' => ['user:1234'],
    //     ], $document3->getPermissions());
    //     $this->assertEquals('Hello Worldx', $document3->getAttribute('text'));
    //     $this->assertCount(2, $document3->getAttribute('texts'));
        
    //     $this->assertIsString($document3->getAttribute('text'));
    //     $this->assertEquals('Hello Worldx', $document3->getAttribute('text'));
    //     $this->assertEquals(['Hello World 1x', 'Hello World 2x'], $document3->getAttribute('texts'));
    //     $this->assertCount(2, $document3->getAttribute('texts'));
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('document'));
    //     $this->assertIsString($document3->getAttribute('document')->getId());
    //     $this->assertNotEmpty($document3->getAttribute('document')->getId());
    //     $this->assertIsArray($document3->getAttribute('document')->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('document')->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('document')->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('document')->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('document')->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('document')->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('document')->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('document')->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('document')->getAttribute('links')[3]);
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents')[0]);
    //     $this->assertIsString($document3->getAttribute('documents')[0]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents')[0]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents')[0]->getPermissions());
    //     $this->assertEquals('Task #1x', $document3->getAttribute('documents')[0]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents')[0]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5x', $document3->getAttribute('documents')[0]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6x', $document3->getAttribute('documents')[0]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7x', $document3->getAttribute('documents')[0]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8x', $document3->getAttribute('documents')[0]->getAttribute('links')[3]);
        
    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents')[1]);
    //     $this->assertIsString($document3->getAttribute('documents')[1]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents')[1]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents')[1]->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('documents')[1]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents')[1]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('documents')[1]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('documents')[1]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('documents')[1]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('documents')[1]->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('document2'));
    //     $this->assertIsString($document3->getAttribute('document2')->getId());
    //     $this->assertNotEmpty($document3->getAttribute('document2')->getId());
    //     $this->assertIsArray($document3->getAttribute('document2')->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('document2')->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('document2')->getPermissions());
    //     $this->assertEquals('Task #1x', $document3->getAttribute('document2')->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('document2')->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5x', $document3->getAttribute('document2')->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6x', $document3->getAttribute('document2')->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7x', $document3->getAttribute('document2')->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8x', $document3->getAttribute('document2')->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents2')[0]);
    //     $this->assertIsString($document3->getAttribute('documents2')[0]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents2')[0]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents2')[0]->getPermissions());
    //     $this->assertEquals('Task #0', $document3->getAttribute('documents2')[0]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents2')[0]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-1', $document3->getAttribute('documents2')[0]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-2', $document3->getAttribute('documents2')[0]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-3', $document3->getAttribute('documents2')[0]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-4', $document3->getAttribute('documents2')[0]->getAttribute('links')[3]);

    //     $this->assertInstanceOf(Document::class, $document3->getAttribute('documents2')[1]);
    //     $this->assertIsString($document3->getAttribute('documents2')[1]->getId());
    //     $this->assertNotEmpty($document3->getAttribute('documents2')[1]->getId());
    //     $this->assertIsArray($document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertArrayHasKey('read', $document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertArrayHasKey('write', $document3->getAttribute('documents2')[1]->getPermissions());
    //     $this->assertEquals('Task #1x', $document3->getAttribute('documents2')[1]->getAttribute('name'));
    //     $this->assertCount(4, $document3->getAttribute('documents2')[1]->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5x', $document3->getAttribute('documents2')[1]->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6x', $document3->getAttribute('documents2')[1]->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7x', $document3->getAttribute('documents2')[1]->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8x', $document3->getAttribute('documents2')[1]->getAttribute('links')[3]);
        
    //     $this->assertIsInt($document3->getAttribute('integer'));
    //     $this->assertEquals(2, $document3->getAttribute('integer'));
    //     $this->assertIsInt($document3->getAttribute('integers')[0]);
    //     $this->assertIsInt($document3->getAttribute('integers')[1]);
    //     $this->assertIsInt($document3->getAttribute('integers')[2]);
    //     $this->assertEquals([6, 4, 5], $document3->getAttribute('integers'));
    //     $this->assertCount(3, $document3->getAttribute('integers'));

    //     $this->assertIsFloat($document3->getAttribute('float'));
    //     $this->assertEquals(3.22, $document3->getAttribute('float'));
    //     $this->assertIsFloat($document3->getAttribute('floats')[0]);
    //     $this->assertIsFloat($document3->getAttribute('floats')[1]);
    //     $this->assertIsFloat($document3->getAttribute('floats')[2]);
    //     $this->assertEquals([2.13, 5.33, 9.9999], $document3->getAttribute('floats'));
    //     $this->assertCount(3, $document3->getAttribute('floats'));

    //     $this->assertIsBool($document3->getAttribute('boolean'));
    //     $this->assertEquals(false, $document3->getAttribute('boolean'));
    //     $this->assertIsBool($document3->getAttribute('booleans')[0]);
    //     $this->assertIsBool($document3->getAttribute('booleans')[1]);
    //     $this->assertIsBool($document3->getAttribute('booleans')[2]);
    //     $this->assertEquals([false, true, false], $document3->getAttribute('booleans'));
    //     $this->assertCount(3, $document3->getAttribute('booleans'));

    //     $this->assertIsString($document3->getAttribute('email'));
    //     $this->assertEquals('testx@appwrite.io', $document3->getAttribute('email'));
    //     $this->assertIsString($document3->getAttribute('emails')[0]);
    //     $this->assertIsString($document3->getAttribute('emails')[1]);
    //     $this->assertIsString($document3->getAttribute('emails')[2]);
    //     $this->assertIsString($document3->getAttribute('emails')[3]);
    //     $this->assertEquals([
    //         'test4x@appwrite.io',
    //         'test3x@appwrite.io',
    //         'test2x@appwrite.io',
    //         'test1x@appwrite.io'
    //     ], $document3->getAttribute('emails'));
    //     $this->assertCount(4, $document3->getAttribute('emails'));

    //     $this->assertIsString($document3->getAttribute('url'));
    //     $this->assertEquals('http://example.com/welcomex', $document3->getAttribute('url'));
    //     $this->assertIsString($document3->getAttribute('urls')[0]);
    //     $this->assertIsString($document3->getAttribute('urls')[1]);
    //     $this->assertIsString($document3->getAttribute('urls')[2]);
    //     $this->assertEquals([
    //         'http://example.com/welcome-1x',
    //         'http://example.com/welcome-2x',
    //         'http://example.com/welcome-3x'
    //     ], $document3->getAttribute('urls'));
    //     $this->assertCount(3, $document3->getAttribute('urls'));

    //     $this->assertIsString($document3->getAttribute('ipv4'));
    //     $this->assertEquals('172.16.254.2', $document3->getAttribute('ipv4'));
    //     $this->assertIsString($document3->getAttribute('ipv4s')[0]);
    //     $this->assertIsString($document3->getAttribute('ipv4s')[1]);
    //     $this->assertEquals([
    //         '172.16.254.2',
    //         '172.16.254.6'
    //     ], $document3->getAttribute('ipv4s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv4s'));

    //     $this->assertIsString($document3->getAttribute('ipv6'));
    //     $this->assertEquals('2001:0db8:85a3:0000:0000:8a2e:0370:7335', $document3->getAttribute('ipv6'));
    //     $this->assertIsString($document3->getAttribute('ipv6s')[0]);
    //     $this->assertIsString($document3->getAttribute('ipv6s')[1]);
    //     $this->assertEquals([
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7335',
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7338'
    //     ], $document3->getAttribute('ipv6s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv6s'));

    //     $this->assertEquals('2001:0db8:85a3:0000:0000:8a2e:0370:7335', $document3->getAttribute('ipv6'));
    //     $this->assertEquals([
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7335',
    //         '2001:0db8:85a3:0000:0000:8a2e:0370:7338'
    //     ], $document3->getAttribute('ipv6s'));
    //     $this->assertCount(2, $document3->getAttribute('ipv6s'));

    //     $this->assertIsString($document3->getAttribute('key'));
    //     $this->assertCount(3, $document3->getAttribute('keys'));
    // }

    // public function testDeleteDocument()
    // {
    //     $collection1 = self::$database->createDocument(Database::COLLECTIONS, [
    //         '$collection' => Database::COLLECTIONS,
    //         '$permissions' => ['read' => ['*']],
    //         'name' => 'Create Documents',
    //         'rules' => [
    //             [
    //                 '$collection' => Database::COLLECTION_RULES,
    //                 '$permissions' => ['read' => ['*']],
    //                 'label' => 'Name',
    //                 'key' => 'name',
    //                 'type' => Database::VAR_STRING,
    //                 'default' => '',
    //                 'required' => true,
    //                 'array' => false,
    //             ],
    //             [
    //                 '$collection' => Database::COLLECTION_RULES,
    //                 '$permissions' => ['read' => ['*']],
    //                 'label' => 'Links',
    //                 'key' => 'links',
    //                 'type' => Database::VAR_STRING,
    //                 'default' => '',
    //                 'required' => true,
    //                 'array' => true,
    //             ],
    //         ]
    //     ]);

    //     $this->assertEquals(true, self::$database->createCollection($collection1->getId(), [], []));
        
    //     $document1 = self::$database->createDocument($collection1->getId(), [
    //         '$collection' => $collection1->getId(),
    //         '$permissions' => [
    //             'read' => ['*'],
    //             'write' => ['user:123'],
    //         ],
    //         'name' => 'Task #1️⃣',
    //         'links' => [
    //             'http://example.com/link-5',
    //             'http://example.com/link-6',
    //             'http://example.com/link-7',
    //             'http://example.com/link-8',
    //         ],
    //     ]);

    //     $this->assertNotEmpty($document1->getId());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertArrayHasKey('read', $document1->getPermissions());
    //     $this->assertArrayHasKey('write', $document1->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document1->getAttribute('name'));
    //     $this->assertCount(4, $document1->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document1->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document1->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document1->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document1->getAttribute('links')[3]);
        
    //     $document1 = self::$database->getDocument($collection1->getId(), $document1->getId());

    //     $this->assertNotEmpty($document1->getId());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertArrayHasKey('read', $document1->getPermissions());
    //     $this->assertArrayHasKey('write', $document1->getPermissions());
    //     $this->assertEquals('Task #1️⃣', $document1->getAttribute('name'));
    //     $this->assertCount(4, $document1->getAttribute('links'));
    //     $this->assertEquals('http://example.com/link-5', $document1->getAttribute('links')[0]);
    //     $this->assertEquals('http://example.com/link-6', $document1->getAttribute('links')[1]);
    //     $this->assertEquals('http://example.com/link-7', $document1->getAttribute('links')[2]);
    //     $this->assertEquals('http://example.com/link-8', $document1->getAttribute('links')[3]);

    //     self::$database->deleteDocument($collection1->getId(), $document1->getId());

    //     $document1 = self::$database->getDocument($collection1->getId(), $document1->getId());

    //     $this->assertTrue($document1->isEmpty());
    //     $this->assertEmpty($document1->getId());
    //     $this->assertEmpty($document1->getCollection());
    //     $this->assertIsArray($document1->getPermissions());
    //     $this->assertEmpty($document1->getPermissions());
    // }

    // public function testFind()
    // {
    //     $data = include __DIR__.'/../../resources/database/movies.php';

    //     $collections = $data['collections'];
    //     $movies = $data['movies'];

    //     foreach ($collections as $key => &$collection) {
    //         $collection = self::$database->createDocument(Database::COLLECTIONS, $collection);
    //         self::$database->createCollection($collection->getId(), [], []);
    //     }

    //     foreach ($movies as $key => &$movie) {
    //         $movie['$collection'] = $collection->getId();
    //         $movie['$permissions'] = [];
    //         $movie = self::$database->createDocument($collection->getId(), $movie);            
    //     }

    //     self::$database->find($collection->getId(), [
    //         'limit' => 5,
    //         'filters' => [
    //             'name=Hello World',
    //             'releaseYear=1999',
    //             'langauges=English',
    //         ],
    //     ]);
    //     $this->assertEquals('1', '1');
    // }

    public function testFindFirst()
    {
        $this->assertEquals('1', '1');
    }

    public function testFindLast()
    {
        $this->assertEquals('1', '1');
    }

    public function countTest()
    {
        $this->assertEquals('1', '1');
    }

    public function addFilterTest()
    {
        $this->assertEquals('1', '1');
    }

    public function encodeTest()
    {
        $this->assertEquals('1', '1');
    }

    public function decodeTest()
    {
        $this->assertEquals('1', '1');
    }

}
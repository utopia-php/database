<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Throwable;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Authorization;

trait DocumentTests
{
    public function testCreateDocument(): Document
    {
        static::getDatabase()->createCollection('documents');

        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'integer_signed', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'integer_unsigned', Database::VAR_INTEGER, 4, true, signed: false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'bigint_signed', Database::VAR_INTEGER, 8, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'bigint_unsigned', Database::VAR_INTEGER, 9, true, signed: false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'float_signed', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'float_unsigned', Database::VAR_FLOAT, 0, true, signed: false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'boolean', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'colors', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'empty', Database::VAR_STRING, 32, false, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents', 'with-dash', Database::VAR_STRING, 128, false, null));

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user(ID::custom('1'))),
                Permission::read(Role::user(ID::custom('2'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::INT_MAX,
            'integer_unsigned' => Database::INT_MAX,
            'bigint_signed' => -Database::BIG_INT_MAX,
            'bigint_unsigned' => Database::BIG_INT_MAX,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
            'with-dash' => 'Works',
        ]));

        $this->assertNotEmpty(true, $document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
        $this->assertIsInt($document->getAttribute('integer_signed'));
        $this->assertEquals(-Database::INT_MAX, $document->getAttribute('integer_signed'));
        $this->assertIsInt($document->getAttribute('integer_unsigned'));
        $this->assertEquals(Database::INT_MAX, $document->getAttribute('integer_unsigned'));
        $this->assertIsInt($document->getAttribute('bigint_signed'));
        $this->assertEquals(-Database::BIG_INT_MAX, $document->getAttribute('bigint_signed'));
        $this->assertIsInt($document->getAttribute('bigint_signed'));
        $this->assertEquals(Database::BIG_INT_MAX, $document->getAttribute('bigint_unsigned'));
        $this->assertIsFloat($document->getAttribute('float_signed'));
        $this->assertEquals(-5.55, $document->getAttribute('float_signed'));
        $this->assertIsFloat($document->getAttribute('float_unsigned'));
        $this->assertEquals(5.55, $document->getAttribute('float_unsigned'));
        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(true, $document->getAttribute('boolean'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));
        $this->assertEquals([], $document->getAttribute('empty'));
        $this->assertEquals('Works', $document->getAttribute('with-dash'));

        // Test create document with manual internal id
        $manualIdDocument = static::getDatabase()->createDocument('documents', new Document([
            '$id' => '56000',
            '$internalId' => '56000',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user(ID::custom('1'))),
                Permission::read(Role::user(ID::custom('2'))),
                Permission::create(Role::any()),
                Permission::create(Role::user(ID::custom('1x'))),
                Permission::create(Role::user(ID::custom('2x'))),
                Permission::update(Role::any()),
                Permission::update(Role::user(ID::custom('1x'))),
                Permission::update(Role::user(ID::custom('2x'))),
                Permission::delete(Role::any()),
                Permission::delete(Role::user(ID::custom('1x'))),
                Permission::delete(Role::user(ID::custom('2x'))),
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::INT_MAX,
            'integer_unsigned' => Database::INT_MAX,
            'bigint_signed' => -Database::BIG_INT_MAX,
            'bigint_unsigned' => Database::BIG_INT_MAX,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
            'with-dash' => 'Works',
        ]));

        $this->assertEquals('56000', $manualIdDocument->getInternalId());
        $this->assertNotEmpty(true, $manualIdDocument->getId());
        $this->assertIsString($manualIdDocument->getAttribute('string'));
        $this->assertEquals('text📝', $manualIdDocument->getAttribute('string')); // Also makes sure an emoji is working
        $this->assertIsInt($manualIdDocument->getAttribute('integer_signed'));
        $this->assertEquals(-Database::INT_MAX, $manualIdDocument->getAttribute('integer_signed'));
        $this->assertIsInt($manualIdDocument->getAttribute('integer_unsigned'));
        $this->assertEquals(Database::INT_MAX, $manualIdDocument->getAttribute('integer_unsigned'));
        $this->assertIsInt($manualIdDocument->getAttribute('bigint_signed'));
        $this->assertEquals(-Database::BIG_INT_MAX, $manualIdDocument->getAttribute('bigint_signed'));
        $this->assertIsInt($manualIdDocument->getAttribute('bigint_unsigned'));
        $this->assertEquals(Database::BIG_INT_MAX, $manualIdDocument->getAttribute('bigint_unsigned'));
        $this->assertIsFloat($manualIdDocument->getAttribute('float_signed'));
        $this->assertEquals(-5.55, $manualIdDocument->getAttribute('float_signed'));
        $this->assertIsFloat($manualIdDocument->getAttribute('float_unsigned'));
        $this->assertEquals(5.55, $manualIdDocument->getAttribute('float_unsigned'));
        $this->assertIsBool($manualIdDocument->getAttribute('boolean'));
        $this->assertEquals(true, $manualIdDocument->getAttribute('boolean'));
        $this->assertIsArray($manualIdDocument->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $manualIdDocument->getAttribute('colors'));
        $this->assertEquals([], $manualIdDocument->getAttribute('empty'));
        $this->assertEquals('Works', $manualIdDocument->getAttribute('with-dash'));

        $manualIdDocument = static::getDatabase()->getDocument('documents', '56000');

        $this->assertEquals('56000', $manualIdDocument->getInternalId());
        $this->assertNotEmpty(true, $manualIdDocument->getId());
        $this->assertIsString($manualIdDocument->getAttribute('string'));
        $this->assertEquals('text📝', $manualIdDocument->getAttribute('string')); // Also makes sure an emoji is working
        $this->assertIsInt($manualIdDocument->getAttribute('integer_signed'));
        $this->assertEquals(-Database::INT_MAX, $manualIdDocument->getAttribute('integer_signed'));
        $this->assertIsInt($manualIdDocument->getAttribute('integer_unsigned'));
        $this->assertEquals(Database::INT_MAX, $manualIdDocument->getAttribute('integer_unsigned'));
        $this->assertIsInt($manualIdDocument->getAttribute('bigint_signed'));
        $this->assertEquals(-Database::BIG_INT_MAX, $manualIdDocument->getAttribute('bigint_signed'));
        $this->assertIsInt($manualIdDocument->getAttribute('bigint_unsigned'));
        $this->assertEquals(Database::BIG_INT_MAX, $manualIdDocument->getAttribute('bigint_unsigned'));
        $this->assertIsFloat($manualIdDocument->getAttribute('float_signed'));
        $this->assertEquals(-5.55, $manualIdDocument->getAttribute('float_signed'));
        $this->assertIsFloat($manualIdDocument->getAttribute('float_unsigned'));
        $this->assertEquals(5.55, $manualIdDocument->getAttribute('float_unsigned'));
        $this->assertIsBool($manualIdDocument->getAttribute('boolean'));
        $this->assertEquals(true, $manualIdDocument->getAttribute('boolean'));
        $this->assertIsArray($manualIdDocument->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $manualIdDocument->getAttribute('colors'));
        $this->assertEquals([], $manualIdDocument->getAttribute('empty'));
        $this->assertEquals('Works', $manualIdDocument->getAttribute('with-dash'));

        try {
            static::getDatabase()->createDocument('documents', new Document([
                'string' => '',
                'integer_signed' => 0,
                'integer_unsigned' => 0,
                'bigint_signed' => 0,
                'bigint_unsigned' => 0,
                'float_signed' => 0,
                'float_unsigned' => -5.55,
                'boolean' => true,
                'colors' => [],
                'empty' => [],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof StructureException);
            $this->assertStringContainsString('Invalid document structure: Attribute "float_unsigned" has invalid type. Value must be a valid range between 0 and', $e->getMessage());
        }

        try {
            static::getDatabase()->createDocument('documents', new Document([
                'string' => '',
                'integer_signed' => 0,
                'integer_unsigned' => 0,
                'bigint_signed' => 0,
                'bigint_unsigned' => -10,
                'float_signed' => 0,
                'float_unsigned' => 0,
                'boolean' => true,
                'colors' => [],
                'empty' => [],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof StructureException);
            $this->assertEquals('Invalid document structure: Attribute "bigint_unsigned" has invalid type. Value must be a valid range between 0 and 9,223,372,036,854,775,807', $e->getMessage());
        }

        return $document;
    }
    /**
     * @return array<Document>
     */
    public function testCreateDocuments(): array
    {
        $count = 3;
        $collection = 'testCreateDocuments';

        static::getDatabase()->createCollection($collection);

        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'bigint', Database::VAR_INTEGER, 8, true));

        // Create an array of documents with random attributes. Don't use the createDocument function
        $documents = [];

        for ($i = 0; $i < $count; $i++) {
            $documents[] = new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'string' => 'text📝',
                'integer' => 5,
                'bigint' => Database::BIG_INT_MAX,
            ]);
        }

        $count = static::getDatabase()->createDocuments($collection, $documents, 3);

        $this->assertEquals($count, \count($documents));

        foreach ($documents as $document) {
            $this->assertNotEmpty(true, $document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(9223372036854775807, $document->getAttribute('bigint'));
        }

        return $documents;
    }

    public function testCreateDocumentsWithDifferentAttributes(): void
    {
        $collection = 'testDiffAttributes';

        static::getDatabase()->createCollection($collection);

        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'integer', Database::VAR_INTEGER, 0, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'bigint', Database::VAR_INTEGER, 8, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'string_default', Database::VAR_STRING, 128, false, 'default'));

        $documents = [
            new Document([
                '$id' => 'first',
                'string' => 'text📝',
                'integer' => 5,
                'string_default' => 'not_default',
            ]),
            new Document([
                '$id' => 'second',
                'string' => 'text📝',
            ]),
        ];

        $results = [];
        $count = static::getDatabase()->createDocuments($collection, $documents, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(2, $count);

        $this->assertEquals('text📝', $results[0]->getAttribute('string'));
        $this->assertEquals(5, $results[0]->getAttribute('integer'));
        $this->assertEquals('not_default', $results[0]->getAttribute('string_default'));
        $this->assertEquals('text📝', $results[1]->getAttribute('string'));
        $this->assertEquals(null, $results[1]->getAttribute('integer'));
        $this->assertEquals('default', $results[1]->getAttribute('string_default'));

        /**
         * Expect fail, mix of internalId and no internalId
         */
        $documents = [
            new Document([
                '$id' => 'third',
                '$internalId' => 'third',
                'string' => 'text📝',
            ]),
            new Document([
                '$id' => 'fourth',
                'string' => 'text📝',
            ]),
        ];

        try {
            static::getDatabase()->createDocuments($collection, $documents);
            $this->fail('Failed to throw exception');
        } catch (DatabaseException $e) {
        }

        $documents = array_reverse($documents);
        try {
            static::getDatabase()->createDocuments($collection, $documents);
            $this->fail('Failed to throw exception');
        } catch (DatabaseException $e) {
        }

        static::getDatabase()->deleteCollection($collection);
    }

    public function testCreateOrUpdateDocuments(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection(__FUNCTION__);
        static::getDatabase()->createAttribute(__FUNCTION__, 'string', Database::VAR_STRING, 128, true);
        static::getDatabase()->createAttribute(__FUNCTION__, 'integer', Database::VAR_INTEGER, 0, true);
        static::getDatabase()->createAttribute(__FUNCTION__, 'bigint', Database::VAR_INTEGER, 8, true);

        $documents = [
            new Document([
                '$id' => 'first',
                'string' => 'text📝',
                'integer' => 5,
                'bigint' => Database::BIG_INT_MAX,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]),
            new Document([
                '$id' => 'second',
                'string' => 'text📝',
                'integer' => 5,
                'bigint' => Database::BIG_INT_MAX,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]),
        ];

        $results = [];
        $count = static::getDatabase()->createOrUpdateDocuments(__FUNCTION__, $documents, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(2, $count);

        foreach ($results as $document) {
            $this->assertNotEmpty(true, $document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::BIG_INT_MAX, $document->getAttribute('bigint'));
        }

        $documents = static::getDatabase()->find(__FUNCTION__);

        $this->assertEquals(2, count($documents));

        foreach ($documents as $document) {
            $this->assertNotEmpty(true, $document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::BIG_INT_MAX, $document->getAttribute('bigint'));
        }

        $documents[0]->setAttribute('string', 'new text📝');
        $documents[0]->setAttribute('integer', 10);
        $documents[1]->setAttribute('string', 'new text📝');
        $documents[1]->setAttribute('integer', 10);

        $results = [];
        $count = static::getDatabase()->createOrUpdateDocuments(__FUNCTION__, $documents, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(2, $count);

        foreach ($results as $document) {
            $this->assertNotEmpty(true, $document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('new text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(10, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::BIG_INT_MAX, $document->getAttribute('bigint'));
        }

        $documents = static::getDatabase()->find(__FUNCTION__);

        $this->assertEquals(2, count($documents));

        foreach ($documents as $document) {
            $this->assertNotEmpty(true, $document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('new text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(10, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::BIG_INT_MAX, $document->getAttribute('bigint'));
        }
    }

    public function testCreateOrUpdateDocumentsInc(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection(__FUNCTION__);
        static::getDatabase()->createAttribute(__FUNCTION__, 'string', Database::VAR_STRING, 128, false);
        static::getDatabase()->createAttribute(__FUNCTION__, 'integer', Database::VAR_INTEGER, 0, false);

        $documents = [
            new Document([
                '$id' => 'first',
                'string' => 'text📝',
                'integer' => 5,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]),
            new Document([
                '$id' => 'second',
                'string' => 'text📝',
                'integer' => 5,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]),
        ];

        static::getDatabase()->createDocuments(__FUNCTION__, $documents);

        $documents[0]->setAttribute('integer', 1);
        $documents[1]->setAttribute('integer', 1);

        static::getDatabase()->createOrUpdateDocumentsWithIncrease(
            collection: __FUNCTION__,
            attribute:'integer',
            documents: $documents
        );

        $documents = static::getDatabase()->find(__FUNCTION__);

        foreach ($documents as $document) {
            $this->assertEquals(6, $document->getAttribute('integer'));
        }

        $documents[0]->setAttribute('integer', -1);
        $documents[1]->setAttribute('integer', -1);

        static::getDatabase()->createOrUpdateDocumentsWithIncrease(
            collection: __FUNCTION__,
            attribute:'integer',
            documents: $documents
        );

        $documents = static::getDatabase()->find(__FUNCTION__);

        foreach ($documents as $document) {
            $this->assertEquals(5, $document->getAttribute('integer'));
        }
    }

    public function testCreateOrUpdateDocumentsPermissions(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection(__FUNCTION__);
        static::getDatabase()->createAttribute(__FUNCTION__, 'string', Database::VAR_STRING, 128, true);

        $document = new Document([
            '$id' => 'first',
            'string' => 'text📝',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
            ],
        ]);

        static::getDatabase()->createOrUpdateDocuments(__FUNCTION__, [$document]);

        try {
            static::getDatabase()->createOrUpdateDocuments(__FUNCTION__, [$document->setAttribute('string', 'updated')]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }

        $document = new Document([
            '$id' => 'second',
            'string' => 'text📝',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]);

        static::getDatabase()->createOrUpdateDocuments(__FUNCTION__, [$document]);

        $results = [];
        $count = static::getDatabase()->createOrUpdateDocuments(
            __FUNCTION__,
            [$document->setAttribute('string', 'updated')],
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );

        $this->assertEquals(1, $count);
        $this->assertEquals('updated', $results[0]->getAttribute('string'));

        $document = new Document([
            '$id' => 'third',
            'string' => 'text📝',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]);

        static::getDatabase()->createOrUpdateDocuments(__FUNCTION__, [$document]);

        $newPermissions = [
            Permission::read(Role::any()),
            Permission::update(Role::user('user1')),
            Permission::delete(Role::user('user1')),
        ];

        $results = [];
        $count = static::getDatabase()->createOrUpdateDocuments(
            __FUNCTION__,
            [$document->setAttribute('$permissions', $newPermissions)],
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );

        $this->assertEquals(1, $count);
        $this->assertEquals($newPermissions, $results[0]->getPermissions());

        $document = static::getDatabase()->getDocument(__FUNCTION__, 'third');

        $this->assertEquals($newPermissions, $document->getPermissions());
    }

    public function testRespectNulls(): Document
    {
        static::getDatabase()->createCollection('documents_nulls');

        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'string', Database::VAR_STRING, 128, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'integer', Database::VAR_INTEGER, 0, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'bigint', Database::VAR_INTEGER, 8, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'float', Database::VAR_FLOAT, 0, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('documents_nulls', 'boolean', Database::VAR_BOOLEAN, 0, false));

        $document = static::getDatabase()->createDocument('documents_nulls', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
        ]));

        $this->assertNotEmpty(true, $document->getId());
        $this->assertNull($document->getAttribute('string'));
        $this->assertNull($document->getAttribute('integer'));
        $this->assertNull($document->getAttribute('bigint'));
        $this->assertNull($document->getAttribute('float'));
        $this->assertNull($document->getAttribute('boolean'));
        return $document;
    }

    public function testCreateDocumentDefaults(): void
    {
        static::getDatabase()->createCollection('defaults');

        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'string', Database::VAR_STRING, 128, false, 'default'));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'integer', Database::VAR_INTEGER, 0, false, 1));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'float', Database::VAR_FLOAT, 0, false, 1.5));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'boolean', Database::VAR_BOOLEAN, 0, false, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'colors', Database::VAR_STRING, 32, false, ['red', 'green', 'blue'], true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('defaults', 'datetime', Database::VAR_DATETIME, 0, false, '2000-06-12T14:12:55.000+00:00', true, false, null, [], ['datetime']));

        $document = static::getDatabase()->createDocument('defaults', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $document2 = static::getDatabase()->getDocument('defaults', $document->getId());
        $this->assertCount(4, $document2->getPermissions());
        $this->assertEquals('read("any")', $document2->getPermissions()[0]);
        $this->assertEquals('create("any")', $document2->getPermissions()[1]);
        $this->assertEquals('update("any")', $document2->getPermissions()[2]);
        $this->assertEquals('delete("any")', $document2->getPermissions()[3]);

        $this->assertNotEmpty(true, $document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('default', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer'));
        $this->assertEquals(1, $document->getAttribute('integer'));
        $this->assertIsFloat($document->getAttribute('float'));
        $this->assertEquals(1.5, $document->getAttribute('float'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertCount(3, $document->getAttribute('colors'));
        $this->assertEquals('red', $document->getAttribute('colors')[0]);
        $this->assertEquals('green', $document->getAttribute('colors')[1]);
        $this->assertEquals('blue', $document->getAttribute('colors')[2]);
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $document->getAttribute('datetime'));

        // cleanup collection
        static::getDatabase()->deleteCollection('defaults');
    }
    public function testIncreaseDecrease(): Document
    {
        $collection = 'increase_decrease';
        static::getDatabase()->createCollection($collection);

        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'increase', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'decrease', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'increase_text', Database::VAR_STRING, 255, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'increase_float', Database::VAR_FLOAT, 0, true));

        $document = static::getDatabase()->createDocument($collection, new Document([
            'increase' => 100,
            'decrease' => 100,
            'increase_float' => 100,
            'increase_text' => "some text",
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ]
        ]));

        $updatedAt = $document->getUpdatedAt();

        $this->assertEquals(true, static::getDatabase()->increaseDocumentAttribute($collection, $document->getId(), 'increase', 1, 101));

        $document = static::getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(101, $document->getAttribute('increase'));
        $this->assertNotEquals($updatedAt, $document->getUpdatedAt());

        $this->assertEquals(true, static::getDatabase()->decreaseDocumentAttribute($collection, $document->getId(), 'decrease', 1, 98));
        $document = static::getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(99, $document->getAttribute('decrease'));

        $this->assertEquals(true, static::getDatabase()->increaseDocumentAttribute($collection, $document->getId(), 'increase_float', 5.5, 110));
        $document = static::getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(105.5, $document->getAttribute('increase_float'));

        $this->assertEquals(true, static::getDatabase()->decreaseDocumentAttribute($collection, $document->getId(), 'increase_float', 1.1, 100));
        $document = static::getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(104.4, $document->getAttribute('increase_float'));

        return $document;
    }
    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseLimitMax(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(true, static::getDatabase()->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase', 10.5, 102.4));
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testDecreaseLimitMin(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(false, static::getDatabase()->decreaseDocumentAttribute('increase_decrease', $document->getId(), 'decrease', 10, 99));
    }


    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseTextAttribute(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(false, static::getDatabase()->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase_text'));
    }

    /**
      * @depends testCreateDocument
      */
    public function testGetDocument(Document $document): Document
    {
        $document = static::getDatabase()->getDocument('documents', $document->getId());

        $this->assertNotEmpty(true, $document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer_signed'));
        $this->assertEquals(-Database::INT_MAX, $document->getAttribute('integer_signed'));
        $this->assertIsFloat($document->getAttribute('float_signed'));
        $this->assertEquals(-5.55, $document->getAttribute('float_signed'));
        $this->assertIsFloat($document->getAttribute('float_unsigned'));
        $this->assertEquals(5.55, $document->getAttribute('float_unsigned'));
        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(true, $document->getAttribute('boolean'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));
        $this->assertEquals('Works', $document->getAttribute('with-dash'));

        return $document;
    }



    /**
     * @depends testCreateDocument
     */
    public function testGetDocumentSelect(Document $document): Document
    {
        $documentId = $document->getId();

        $document = static::getDatabase()->getDocument('documents', $documentId, [
            Query::select('string'),
            Query::select('integer_signed'),
        ]);
var_dump($document);
        $this->assertEmpty($document->getId());
        $this->assertFalse($document->isEmpty());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer_signed'));
        $this->assertEquals(-Database::INT_MAX, $document->getAttribute('integer_signed'));
        $this->assertArrayNotHasKey('float', $document->getAttributes());
        $this->assertArrayNotHasKey('boolean', $document->getAttributes());
        $this->assertArrayNotHasKey('colors', $document->getAttributes());
        $this->assertArrayNotHasKey('with-dash', $document->getAttributes());
        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        $document = static::getDatabase()->getDocument('documents', $documentId, [
            Query::select('string'),
            Query::select('integer_signed'),
            Query::select('$id'),
        ]);

        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        $document = static::getDatabase()->getDocument('documents', $documentId, [
            Query::select('string'),
            Query::select('integer_signed'),
            Query::select('$permissions'),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        $document = static::getDatabase()->getDocument('documents', $documentId, [
            Query::select('string'),
            Query::select('integer_signed'),
            Query::select('$internalId'),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        $document = static::getDatabase()->getDocument('documents', $documentId, [
            Query::select('string'),
            Query::select('integer_signed'),
            Query::select('$collection'),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        $document = static::getDatabase()->getDocument('documents', $documentId, [
            Query::select('string'),
            Query::select('integer_signed'),
            Query::select('$createdAt'),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        $document = static::getDatabase()->getDocument('documents', $documentId, [
            Query::select('string'),
            Query::select('integer_signed'),
            Query::select('$updatedAt'),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        return $document;
    }
    /**
     * @return array<string, mixed>
     */
    public function testFind(): array
    {
        Authorization::setRole(Role::any()->toString());

        static::getDatabase()->createCollection('movies', permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users())
        ]);

        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'director', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'price', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'active', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'genres', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'with-dash', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('movies', 'nullable', Database::VAR_STRING, 128, false));

        try {
            static::getDatabase()->createDocument('movies', new Document(['$id' => ['id_as_array']]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('$id must be of type string', $e->getMessage());
            $this->assertInstanceOf(StructureException::class, $e);
        }

        $document = static::getDatabase()->createDocument('movies', new Document([
            '$id' => ID::custom('frozen'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'genres' => ['animation', 'kids'],
            'with-dash' => 'Works'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Frozen II',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2019,
            'price' => 39.50,
            'active' => true,
            'genres' => ['animation', 'kids'],
            'with-dash' => 'Works'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Captain America: The First Avenger',
            'director' => 'Joe Johnston',
            'year' => 2011,
            'price' => 25.94,
            'active' => true,
            'genres' => ['science fiction', 'action', 'comics'],
            'with-dash' => 'Works2'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Captain Marvel',
            'director' => 'Anna Boden & Ryan Fleck',
            'year' => 2019,
            'price' => 25.99,
            'active' => true,
            'genres' => ['science fiction', 'action', 'comics'],
            'with-dash' => 'Works2'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Work in Progress',
            'director' => 'TBD',
            'year' => 2025,
            'price' => 0.0,
            'active' => false,
            'genres' => [],
            'with-dash' => 'Works3'
        ]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::user('x')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Work in Progress 2',
            'director' => 'TBD',
            'year' => 2026,
            'price' => 0.0,
            'active' => false,
            'genres' => [],
            'with-dash' => 'Works3',
            'nullable' => 'Not null'
        ]));

        return [
            '$internalId' => $document->getInternalId()
        ];
    }

    /**
    * @return void
    * @throws \Utopia\Database\Exception
    */
    public function testSelectInternalID(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::select('$internalId'),
            Query::select('$id'),
            Query::orderAsc(''),
            Query::limit(1),
        ]);

        $document = $documents[0];

        $this->assertArrayHasKey('$internalId', $document);
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);
        $this->assertCount(3, $document);

        $document = static::getDatabase()->getDocument('movies', $document->getId(), [
            Query::select('$internalId'),
        ]);

        $this->assertArrayHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);
        $this->assertCount(3, $document);
    }



    /**
    * @depends testFind
    */
    public function testFindOne(): void
    {
        $document = static::getDatabase()->findOne('movies', [
            Query::offset(2),
            Query::orderAsc('name')
        ]);

        $this->assertFalse($document->isEmpty());
        $this->assertEquals('Frozen', $document->getAttribute('name'));

        $document = static::getDatabase()->findOne('movies', [
            Query::offset(10)
        ]);
        $this->assertTrue($document->isEmpty());
    }

    public function testFindBasicChecks(): void
    {
        $documents = static::getDatabase()->find('movies');
        $movieDocuments = $documents;

        $this->assertEquals(5, count($documents));
        $this->assertNotEmpty($documents[0]->getId());
        $this->assertEquals('movies', $documents[0]->getCollection());
        $this->assertEquals(['any', 'user:1', 'user:2'], $documents[0]->getRead());
        $this->assertEquals(['any', 'user:1x', 'user:2x'], $documents[0]->getWrite());
        $this->assertEquals('Frozen', $documents[0]->getAttribute('name'));
        $this->assertEquals('Chris Buck & Jennifer Lee', $documents[0]->getAttribute('director'));
        $this->assertIsString($documents[0]->getAttribute('director'));
        $this->assertEquals(2013, $documents[0]->getAttribute('year'));
        $this->assertIsInt($documents[0]->getAttribute('year'));
        $this->assertEquals(39.50, $documents[0]->getAttribute('price'));
        $this->assertIsFloat($documents[0]->getAttribute('price'));
        $this->assertEquals(true, $documents[0]->getAttribute('active'));
        $this->assertIsBool($documents[0]->getAttribute('active'));
        $this->assertEquals(['animation', 'kids'], $documents[0]->getAttribute('genres'));
        $this->assertIsArray($documents[0]->getAttribute('genres'));
        $this->assertEquals('Works', $documents[0]->getAttribute('with-dash'));

        // Alphabetical order
        $sortedDocuments = $movieDocuments;
        \usort($sortedDocuments, function ($doc1, $doc2) {
            return strcmp($doc1['$id'], $doc2['$id']);
        });

        $firstDocumentId = $sortedDocuments[0]->getId();
        $lastDocumentId = $sortedDocuments[\count($sortedDocuments) - 1]->getId();

        /**
         * Check $id: Notice, this orders ID names alphabetically, not by internal numeric ID
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('$id'),
        ]);
        $this->assertEquals($lastDocumentId, $documents[0]->getId());
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderAsc('$id'),
        ]);
        $this->assertEquals($firstDocumentId, $documents[0]->getId());

        /**
         * Check internal numeric ID sorting
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);
        $this->assertEquals($movieDocuments[\count($movieDocuments) - 1]->getId(), $documents[0]->getId());
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderAsc(''),
        ]);
        $this->assertEquals($movieDocuments[0]->getId(), $documents[0]->getId());
    }

    public function testFindCheckPermissions(): void
    {
        /**
         * Check Permissions
         */
        Authorization::setRole('user:x');
        $documents = static::getDatabase()->find('movies');

        $this->assertEquals(6, count($documents));
    }

    public function testFindCheckInteger(): void
    {
        /**
         * Query with dash attribute
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('with-dash', ['Works']),
        ]);

        $this->assertEquals(2, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::equal('with-dash', ['Works2', 'Works3']),
        ]);

        $this->assertEquals(4, count($documents));

        /**
         * Check an Integer condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('year', [2019]),
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
    }

    public function testFindBoolean(): void
    {
        /**
         * Boolean condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('active', [true]),
        ]);

        $this->assertEquals(4, count($documents));
    }

    public function testFindStringQueryEqual(): void
    {
        /**
         * String condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['TBD']),
        ]);

        $this->assertEquals(2, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['']),
        ]);

        $this->assertEquals(0, count($documents));
    }


    public function testFindNotEqual(): void
    {
        /**
         * Not Equal query
         */
        $documents = static::getDatabase()->find('movies', [
            Query::notEqual('director', 'TBD'),
        ]);

        $this->assertGreaterThan(0, count($documents));

        foreach ($documents as $document) {
            $this->assertTrue($document['director'] !== 'TBD');
        }

        $documents = static::getDatabase()->find('movies', [
            Query::notEqual('director', ''),
        ]);

        $total = static::getDatabase()->count('movies');

        $this->assertEquals($total, count($documents));
    }

    public function testFindBetween(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::between('price', 25.94, 25.99),
        ]);
        $this->assertEquals(2, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::between('price', 30, 35),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::between('$createdAt', '1975-12-06', '2050-12-06'),
        ]);
        $this->assertEquals(6, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::between('$updatedAt', '1975-12-06T07:08:49.733+02:00', '2050-02-05T10:15:21.825+00:00'),
        ]);
        $this->assertEquals(6, count($documents));
    }

    public function testFindFloat(): void
    {
        /**
         * Float condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::lessThan('price', 26.00),
            Query::greaterThan('price', 25.98),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindContains(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForQueryContains()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $documents = static::getDatabase()->find('movies', [
            Query::contains('genres', ['comics'])
        ]);

        $this->assertEquals(2, count($documents));

        /**
         * Array contains OR condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::contains('genres', ['comics', 'kids']),
        ]);

        $this->assertEquals(4, count($documents));

        $documents = static::getDatabase()->find('movies', [
            Query::contains('genres', ['non-existent']),
        ]);

        $this->assertEquals(0, count($documents));

        try {
            static::getDatabase()->find('movies', [
                Query::contains('price', [10.5]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid query: Cannot query contains on attribute "price" because it is not an array or string.', $e->getMessage());
            $this->assertTrue($e instanceof DatabaseException);
        }
    }

    public function testFindFulltext(): void
    {
        /**
         * Fulltext search
         */
        if ($this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $success = static::getDatabase()->createIndex('movies', 'name', Database::INDEX_FULLTEXT, ['name']);
            $this->assertEquals(true, $success);

            $documents = static::getDatabase()->find('movies', [
                Query::search('name', 'captain'),
            ]);

            $this->assertEquals(2, count($documents));

            /**
             * Fulltext search (wildcard)
             */

            // TODO: Looks like the MongoDB implementation is a bit more complex, skipping that for now.
            // TODO: I think this needs a changes? how do we distinguish between regular full text and wildcard?

            if ($this->getDatabase()->getAdapter()->getSupportForFulltextWildCardIndex()) {
                $documents = static::getDatabase()->find('movies', [
                    Query::search('name', 'cap'),
                ]);

                $this->assertEquals(2, count($documents));
            }
        }

        $this->assertEquals(true, true); // Test must do an assertion
    }
    public function testFindFulltextSpecialChars(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'full_text';
        static::getDatabase()->createCollection($collection, permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users())
        ]);

        $this->assertTrue(static::getDatabase()->createAttribute($collection, 'ft', Database::VAR_STRING, 128, true));
        $this->assertTrue(static::getDatabase()->createIndex($collection, 'ft-index', Database::INDEX_FULLTEXT, ['ft']));

        static::getDatabase()->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'Alf: chapter_4@nasa.com'
        ]));

        $documents = static::getDatabase()->find($collection, [
            Query::search('ft', 'chapter_4'),
        ]);
        $this->assertEquals(1, count($documents));

        static::getDatabase()->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'al@ba.io +-*)(<>~'
        ]));

        $documents = static::getDatabase()->find($collection, [
            Query::search('ft', 'al@ba.io'), // === al ba io*
        ]);

        if (static::getDatabase()->getAdapter()->getSupportForFulltextWildcardIndex()) {
            $this->assertEquals(0, count($documents));
        } else {
            $this->assertEquals(1, count($documents));
        }

        static::getDatabase()->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'donald duck'
        ]));

        static::getDatabase()->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'donald trump'
        ]));

        $documents = static::getDatabase()->find($collection, [
            Query::search('ft', 'donald trump'),
            Query::orderAsc('ft'),
        ]);
        $this->assertEquals(2, count($documents));

        $documents = static::getDatabase()->find($collection, [
            Query::search('ft', '"donald trump"'), // Exact match
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindMultipleConditions(): void
    {
        /**
         * Multiple conditions
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['TBD']),
            Query::equal('year', [2026]),
        ]);

        $this->assertEquals(1, count($documents));

        /**
         * Multiple conditions and OR values
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('name', ['Frozen II', 'Captain Marvel']),
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
    }

    public function testFindByID(): void
    {
        /**
         * $id condition
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('$id', ['frozen']),
        ]);

        $this->assertEquals(1, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
    }
    /**
     * @depends testFind
     * @param array<string, mixed> $data
     * @return void
     * @throws \Utopia\Database\Exception
     */
    public function testFindByInternalID(array $data): void
    {
        /**
         * Test that internal ID queries are handled correctly
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('$internalId', [$data['$internalId']]),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindOrderBy(): void
    {
        /**
         * ORDER BY
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('name')
        ]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
        $this->assertEquals('Frozen II', $documents[1]['name']);
        $this->assertEquals('Captain Marvel', $documents[2]['name']);
        $this->assertEquals('Captain America: The First Avenger', $documents[3]['name']);
        $this->assertEquals('Work in Progress', $documents[4]['name']);
        $this->assertEquals('Work in Progress 2', $documents[5]['name']);
    }
    public function testFindOrderByNatural(): void
    {
        /**
         * ORDER BY natural
         */
        $base = array_reverse(static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]));
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals($base[0]['name'], $documents[0]['name']);
        $this->assertEquals($base[1]['name'], $documents[1]['name']);
        $this->assertEquals($base[2]['name'], $documents[2]['name']);
        $this->assertEquals($base[3]['name'], $documents[3]['name']);
        $this->assertEquals($base[4]['name'], $documents[4]['name']);
        $this->assertEquals($base[5]['name'], $documents[5]['name']);
    }
    public function testFindOrderByMultipleAttributes(): void
    {
        /**
         * ORDER BY - Multiple attributes
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderDesc('name')
        ]);

        $this->assertEquals(6, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Frozen', $documents[1]['name']);
        $this->assertEquals('Captain Marvel', $documents[2]['name']);
        $this->assertEquals('Captain America: The First Avenger', $documents[3]['name']);
        $this->assertEquals('Work in Progress 2', $documents[4]['name']);
        $this->assertEquals('Work in Progress', $documents[5]['name']);
    }

    public function testFindOrderByCursorAfter(): void
    {
        /**
         * ORDER BY - After
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }


    public function testFindOrderByCursorBefore(): void
    {
        /**
         * ORDER BY - Before
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByAfterNaturalOrder(): void
    {
        /**
         * ORDER BY - After by natural order
         */
        $movies = array_reverse(static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]));

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }
    public function testFindOrderByBeforeNaturalOrder(): void
    {
        /**
         * ORDER BY - Before by natural order
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderBySingleAttributeAfter(): void
    {
        /**
         * ORDER BY - Single Attribute After
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('year')
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[1])
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }


    public function testFindOrderBySingleAttributeBefore(): void
    {
        /**
         * ORDER BY - Single Attribute Before
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('year')
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByMultipleAttributeAfter(): void
    {
        /**
         * ORDER BY - Multiple Attribute After
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year')
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByMultipleAttributeBefore(): void
    {
        /**
         * ORDER BY - Multiple Attribute Before
         */
        $movies = static::getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year')
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[5])
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[4])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }
    public function testFindOrderByAndCursor(): void
    {
        /**
         * ORDER BY + CURSOR
         */
        $documentsTest = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
        ]);
        $documents = static::getDatabase()->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }
    public function testFindOrderByIdAndCursor(): void
    {
        /**
         * ORDER BY ID + CURSOR
         */
        $documentsTest = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$id'),
        ]);
        $documents = static::getDatabase()->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$id'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindOrderByCreateDateAndCursor(): void
    {
        /**
         * ORDER BY CREATE DATE + CURSOR
         */
        $documentsTest = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$createdAt'),
        ]);

        $documents = static::getDatabase()->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$createdAt'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindOrderByUpdateDateAndCursor(): void
    {
        /**
         * ORDER BY UPDATE DATE + CURSOR
         */
        $documentsTest = static::getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$updatedAt'),
        ]);
        $documents = static::getDatabase()->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$updatedAt'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindLimit(): void
    {
        /**
         * Limit
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(4),
            Query::offset(0),
            Query::orderAsc('name')
        ]);

        $this->assertEquals(4, count($documents));
        $this->assertEquals('Captain America: The First Avenger', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
        $this->assertEquals('Frozen', $documents[2]['name']);
        $this->assertEquals('Frozen II', $documents[3]['name']);
    }


    public function testFindLimitAndOffset(): void
    {
        /**
         * Limit + Offset
         */
        $documents = static::getDatabase()->find('movies', [
            Query::limit(4),
            Query::offset(2),
            Query::orderAsc('name')
        ]);

        $this->assertEquals(4, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
        $this->assertEquals('Frozen II', $documents[1]['name']);
        $this->assertEquals('Work in Progress', $documents[2]['name']);
        $this->assertEquals('Work in Progress 2', $documents[3]['name']);
    }

    public function testFindOrQueries(): void
    {
        /**
         * Test that OR queries are handled correctly
         */
        $documents = static::getDatabase()->find('movies', [
            Query::equal('director', ['TBD', 'Joe Johnston']),
            Query::equal('year', [2025]),
        ]);
        $this->assertEquals(1, count($documents));
    }

    /**
     * @depends testUpdateDocument
     */
    public function testFindEdgeCases(Document $document): void
    {
        $collection = 'edgeCases';

        static::getDatabase()->createCollection($collection);

        $this->assertEquals(true, static::getDatabase()->createAttribute($collection, 'value', Database::VAR_STRING, 256, true));

        $values = [
            'NormalString',
            '{"type":"json","somekey":"someval"}',
            '{NormalStringInBraces}',
            '"NormalStringInDoubleQuotes"',
            '{"NormalStringInDoubleQuotesAndBraces"}',
            "'NormalStringInSingleQuotes'",
            "{'NormalStringInSingleQuotesAndBraces'}",
            "SingleQuote'InMiddle",
            'DoubleQuote"InMiddle',
            'Slash/InMiddle',
            'Backslash\InMiddle',
            'Colon:InMiddle',
            '"quoted":"colon"'
        ];

        foreach ($values as $value) {
            static::getDatabase()->createDocument($collection, new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any())
                ],
                'value' => $value
            ]));
        }

        /**
         * Check Basic
         */
        $documents = static::getDatabase()->find($collection);

        $this->assertEquals(count($values), count($documents));
        $this->assertNotEmpty($documents[0]->getId());
        $this->assertEquals($collection, $documents[0]->getCollection());
        $this->assertEquals(['any'], $documents[0]->getRead());
        $this->assertEquals(['any'], $documents[0]->getUpdate());
        $this->assertEquals(['any'], $documents[0]->getDelete());
        $this->assertEquals($values[0], $documents[0]->getAttribute('value'));

        /**
         * Check `equals` query
         */
        foreach ($values as $value) {
            $documents = static::getDatabase()->find($collection, [
                Query::limit(25),
                Query::equal('value', [$value])
            ]);

            $this->assertEquals(1, count($documents));
            $this->assertEquals($value, $documents[0]->getAttribute('value'));
        }
    }

    public function testOrSingleQuery(): void
    {
        try {
            static::getDatabase()->find('movies', [
                Query::or([
                    Query::equal('active', [true])
                ])
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Invalid query: Or queries require at least two queries', $e->getMessage());
        }
    }

    public function testOrMultipleQueries(): void
    {
        $queries = [
            Query::or([
                Query::equal('active', [true]),
                Query::equal('name', ['Frozen II'])
            ])
        ];
        $this->assertCount(4, static::getDatabase()->find('movies', $queries));
        $this->assertEquals(4, static::getDatabase()->count('movies', $queries));

        $queries = [
            Query::equal('active', [true]),
            Query::or([
                Query::equal('name', ['Frozen']),
                Query::equal('name', ['Frozen II']),
                Query::equal('director', ['Joe Johnston'])
            ])
        ];

        $this->assertCount(3, static::getDatabase()->find('movies', $queries));
        $this->assertEquals(3, static::getDatabase()->count('movies', $queries));
    }

    public function testOrNested(): void
    {
        $queries = [
            Query::select('director'),
            Query::equal('director', ['Joe Johnston']),
            Query::or([
                Query::equal('name', ['Frozen']),
                Query::or([
                    Query::equal('active', [true]),
                    Query::equal('active', [false]),
                ])
            ])
        ];

        $documents = static::getDatabase()->find('movies', $queries);
        $this->assertCount(1, $documents);
        $this->assertArrayNotHasKey('name', $documents[0]);

        $count = static::getDatabase()->count('movies', $queries);
        $this->assertEquals(1, $count);
    }

    public function testAndSingleQuery(): void
    {
        try {
            static::getDatabase()->find('movies', [
                Query::and([
                    Query::equal('active', [true])
                ])
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Invalid query: And queries require at least two queries', $e->getMessage());
        }
    }

    public function testAndMultipleQueries(): void
    {
        $queries = [
            Query::and([
                Query::equal('active', [true]),
                Query::equal('name', ['Frozen II'])
            ])
        ];
        $this->assertCount(1, static::getDatabase()->find('movies', $queries));
        $this->assertEquals(1, static::getDatabase()->count('movies', $queries));
    }

    public function testAndNested(): void
    {
        $queries = [
            Query::or([
                Query::equal('active', [false]),
                Query::and([
                    Query::equal('active', [true]),
                    Query::equal('name', ['Frozen']),
                ])
            ])
        ];

        $documents = static::getDatabase()->find('movies', $queries);
        $this->assertCount(3, $documents);

        $count = static::getDatabase()->count('movies', $queries);
        $this->assertEquals(3, $count);
    }

    public function testNestedIDQueries(): void
    {
        Authorization::setRole(Role::any()->toString());

        static::getDatabase()->createCollection('movies_nested_id', permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users())
        ]);

        $this->assertEquals(true, static::getDatabase()->createAttribute('movies_nested_id', 'name', Database::VAR_STRING, 128, true));

        static::getDatabase()->createDocument('movies_nested_id', new Document([
            '$id' => ID::custom('1'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => '1',
        ]));

        static::getDatabase()->createDocument('movies_nested_id', new Document([
            '$id' => ID::custom('2'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => '2',
        ]));

        static::getDatabase()->createDocument('movies_nested_id', new Document([
            '$id' => ID::custom('3'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => '3',
        ]));

        $queries = [
            Query::or([
                Query::equal('$id', ["1"]),
                Query::equal('$id', ["2"])
            ])
        ];

        $documents = static::getDatabase()->find('movies_nested_id', $queries);
        $this->assertCount(2, $documents);

        // Make sure the query was not modified by reference
        $this->assertEquals($queries[0]->getValues()[0]->getAttribute(), '$id');

        $count = static::getDatabase()->count('movies_nested_id', $queries);
        $this->assertEquals(2, $count);
    }

    public function testFindNull(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::isNull('nullable'),
        ]);

        $this->assertEquals(5, count($documents));
    }

    public function testFindNotNull(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::isNotNull('nullable'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindStartsWith(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::startsWith('name', 'Work'),
        ]);

        $this->assertEquals(2, count($documents));

        if ($this->getDatabase()->getAdapter() instanceof SQL) {
            $documents = static::getDatabase()->find('movies', [
                Query::startsWith('name', '%ork'),
            ]);
        } else {
            $documents = static::getDatabase()->find('movies', [
                Query::startsWith('name', '.*ork'),
            ]);
        }

        $this->assertEquals(0, count($documents));
    }

    public function testFindStartsWithWords(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::startsWith('name', 'Work in Progress'),
        ]);

        $this->assertEquals(2, count($documents));
    }

    public function testFindEndsWith(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::endsWith('name', 'Marvel'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindSelect(): void
    {
        $documents = static::getDatabase()->find('movies', [
            Query::select('name'),
            Query::select('year')
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayNotHasKey('$id', $document);
            $this->assertArrayNotHasKey('$internalId', $document);
            $this->assertArrayNotHasKey('$collection', $document);
            $this->assertArrayNotHasKey('$createdAt', $document);
            $this->assertArrayNotHasKey('$updatedAt', $document);
            $this->assertArrayNotHasKey('$permissions', $document);
        }

        $documents = static::getDatabase()->find('movies', [
            Query::select('name'),
            Query::select('year'),
            Query::select('$id')
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayNotHasKey('$internalId', $document);
            $this->assertArrayNotHasKey('$collection', $document);
            $this->assertArrayNotHasKey('$createdAt', $document);
            $this->assertArrayNotHasKey('$updatedAt', $document);
            $this->assertArrayNotHasKey('$permissions', $document);
        }

        $documents = static::getDatabase()->find('movies', [
            Query::select('name'),
            Query::select('year'),
            Query::select('$internalId')
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayNotHasKey('$id', $document);
            $this->assertArrayHasKey('$internalId', $document);
            $this->assertArrayNotHasKey('$collection', $document);
            $this->assertArrayNotHasKey('$createdAt', $document);
            $this->assertArrayNotHasKey('$updatedAt', $document);
            $this->assertArrayNotHasKey('$permissions', $document);
        }

        $documents = static::getDatabase()->find('movies', [
            Query::select('name'),
            Query::select('year'),
            Query::select('$collection')
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayNotHasKey('$id', $document);
            $this->assertArrayNotHasKey('$internalId', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayNotHasKey('$createdAt', $document);
            $this->assertArrayNotHasKey('$updatedAt', $document);
            $this->assertArrayNotHasKey('$permissions', $document);
        }

        $documents = static::getDatabase()->find('movies', [
            Query::select('name'),
            Query::select('year'),
            Query::select('$createdAt')
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayNotHasKey('$id', $document);
            $this->assertArrayNotHasKey('$internalId', $document);
            $this->assertArrayNotHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayNotHasKey('$updatedAt', $document);
            $this->assertArrayNotHasKey('$permissions', $document);
        }

        $documents = static::getDatabase()->find('movies', [
            Query::select('name'),
            Query::select('year'),
            Query::select('$updatedAt')
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayNotHasKey('$id', $document);
            $this->assertArrayNotHasKey('$internalId', $document);
            $this->assertArrayNotHasKey('$collection', $document);
            $this->assertArrayNotHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayNotHasKey('$permissions', $document);
        }

        $documents = static::getDatabase()->find('movies', [
            Query::select('name'),
            Query::select('year'),
            Query::select('$permissions')
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayNotHasKey('$id', $document);
            $this->assertArrayNotHasKey('$internalId', $document);
            $this->assertArrayNotHasKey('$collection', $document);
            $this->assertArrayNotHasKey('$createdAt', $document);
            $this->assertArrayNotHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);
        }
    }

    /** @depends testFind */
    public function testForeach(): void
    {
        /**
         * Test, foreach goes through all the documents
         */
        $documents = [];
        static::getDatabase()->foreach('movies', queries: [Query::limit(2)], callback: function ($document) use (&$documents) {
            $documents[] = $document;
        });
        $this->assertEquals(6, count($documents));

        /**
         * Test, foreach with initial cursor
         */

        $first = $documents[0];
        $documents = [];
        static::getDatabase()->foreach('movies', queries: [Query::limit(2), Query::cursorAfter($first)], callback: function ($document) use (&$documents) {
            $documents[] = $document;
        });
        $this->assertEquals(5, count($documents));

        /**
         * Test, foreach with initial offset
         */

        $documents = [];
        static::getDatabase()->foreach('movies', queries: [Query::limit(2), Query::offset(2)], callback: function ($document) use (&$documents) {
            $documents[] = $document;
        });
        $this->assertEquals(4, count($documents));

        /**
         * Test, cursor before throws error
         */
        try {
            static::getDatabase()->foreach('movies', queries: [Query::cursorBefore($documents[0]), Query::offset(2)], callback: function ($document) use (&$documents) {
                $documents[] = $document;
            });

        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertEquals('Cursor ' . Database::CURSOR_BEFORE . ' not supported in this method.', $e->getMessage());
        }

    }

    /**
     * @depends testFind
     */
    public function testCount(): void
    {
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        $count = static::getDatabase()->count('movies', [Query::equal('year', [2019])]);

        $this->assertEquals(2, $count);
        $count = static::getDatabase()->count('movies', [Query::equal('with-dash', ['Works'])]);
        $this->assertEquals(2, $count);
        $count = static::getDatabase()->count('movies', [Query::equal('with-dash', ['Works2', 'Works3'])]);
        $this->assertEquals(4, $count);

        Authorization::unsetRole('user:x');
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(5, $count);

        Authorization::disable();
        $count = static::getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        Authorization::reset();

        Authorization::disable();
        $count = static::getDatabase()->count('movies', [], 3);
        $this->assertEquals(3, $count);
        Authorization::reset();

        /**
         * Test that OR queries are handled correctly
         */
        Authorization::disable();
        $count = static::getDatabase()->count('movies', [
            Query::equal('director', ['TBD', 'Joe Johnston']),
            Query::equal('year', [2025]),
        ]);
        $this->assertEquals(1, $count);
        Authorization::reset();
    }

    /**
     * @depends testFind
     */
    public function testSum(): void
    {
        Authorization::setRole('user:x');

        $sum = static::getDatabase()->sum('movies', 'year', [Query::equal('year', [2019]),]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = static::getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025 + 2026, $sum);
        $sum = static::getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = static::getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));

        $sum = static::getDatabase()->sum('movies', 'year', [Query::equal('year', [2019])], 1);
        $this->assertEquals(2019, $sum);

        Authorization::unsetRole('user:x');
        Authorization::unsetRole('userx');
        $sum = static::getDatabase()->sum('movies', 'year', [Query::equal('year', [2019]),]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = static::getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025, $sum);
        $sum = static::getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = static::getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
    }

    public function testEncodeDecode(): void
    {
        $collection = new Document([
            '$collection' => ID::custom(Database::METADATA),
            '$id' => ID::custom('users'),
            'name' => 'Users',
            'attributes' => [
                [
                    '$id' => ID::custom('name'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 256,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('email'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 1024,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('status'),
                    'type' => Database::VAR_INTEGER,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('password'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('passwordUpdate'),
                    'type' => Database::VAR_DATETIME,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['datetime'],
                ],
                [
                    '$id' => ID::custom('registration'),
                    'type' => Database::VAR_DATETIME,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['datetime'],
                ],
                [
                    '$id' => ID::custom('emailVerification'),
                    'type' => Database::VAR_BOOLEAN,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('reset'),
                    'type' => Database::VAR_BOOLEAN,
                    'format' => '',
                    'size' => 0,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('prefs'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json']
                ],
                [
                    '$id' => ID::custom('sessions'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => ID::custom('tokens'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => ID::custom('memberships'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 16384,
                    'signed' => true,
                    'required' => false,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => ID::custom('roles'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 128,
                    'signed' => true,
                    'required' => false,
                    'array' => true,
                    'filters' => [],
                ],
                [
                    '$id' => ID::custom('tags'),
                    'type' => Database::VAR_STRING,
                    'format' => '',
                    'size' => 128,
                    'signed' => true,
                    'required' => false,
                    'array' => true,
                    'filters' => ['json'],
                ],
            ],
            'indexes' => [
                [
                    '$id' => ID::custom('_key_email'),
                    'type' => Database::INDEX_UNIQUE,
                    'attributes' => ['email'],
                    'lengths' => [1024],
                    'orders' => [Database::ORDER_ASC],
                ]
            ],
        ]);

        $document = new Document([
            '$id' => ID::custom('608fdbe51361a'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::user('608fdbe51361a')),
                Permission::update(Role::user('608fdbe51361a')),
                Permission::delete(Role::user('608fdbe51361a')),
            ],
            'email' => 'test@example.com',
            'emailVerification' => false,
            'status' => 1,
            'password' => 'randomhash',
            'passwordUpdate' => '2000-06-12 14:12:55',
            'registration' => '1975-06-12 14:12:55+01:00',
            'reset' => false,
            'name' => 'My Name',
            'prefs' => new \stdClass(),
            'sessions' => [],
            'tokens' => [],
            'memberships' => [],
            'roles' => [
                'admin',
                'developer',
                'tester',
            ],
            'tags' => [
                ['$id' => '1', 'label' => 'x'],
                ['$id' => '2', 'label' => 'y'],
                ['$id' => '3', 'label' => 'z'],
            ],
        ]);

        $result = static::getDatabase()->encode($collection, $document);

        $this->assertEquals('608fdbe51361a', $result->getAttribute('$id'));
        $this->assertContains('read("any")', $result->getAttribute('$permissions'));
        $this->assertContains('read("any")', $result->getPermissions());
        $this->assertContains('any', $result->getRead());
        $this->assertContains(Permission::create(Role::user(ID::custom('608fdbe51361a'))), $result->getPermissions());
        $this->assertContains('user:608fdbe51361a', $result->getCreate());
        $this->assertContains('user:608fdbe51361a', $result->getWrite());
        $this->assertEquals('test@example.com', $result->getAttribute('email'));
        $this->assertEquals(false, $result->getAttribute('emailVerification'));
        $this->assertEquals(1, $result->getAttribute('status'));
        $this->assertEquals('randomhash', $result->getAttribute('password'));
        $this->assertEquals('2000-06-12 14:12:55.000', $result->getAttribute('passwordUpdate'));
        $this->assertEquals('1975-06-12 13:12:55.000', $result->getAttribute('registration'));
        $this->assertEquals(false, $result->getAttribute('reset'));
        $this->assertEquals('My Name', $result->getAttribute('name'));
        $this->assertEquals('{}', $result->getAttribute('prefs'));
        $this->assertEquals('[]', $result->getAttribute('sessions'));
        $this->assertEquals('[]', $result->getAttribute('tokens'));
        $this->assertEquals('[]', $result->getAttribute('memberships'));
        $this->assertEquals(['admin', 'developer', 'tester',], $result->getAttribute('roles'));
        $this->assertEquals(['{"$id":"1","label":"x"}', '{"$id":"2","label":"y"}', '{"$id":"3","label":"z"}',], $result->getAttribute('tags'));

        $context = new QueryContext();
        $context->add($collection);

        $result = static::getDatabase()->decode($context, $document);

        $this->assertEquals('608fdbe51361a', $result->getAttribute('$id'));
        $this->assertContains('read("any")', $result->getAttribute('$permissions'));
        $this->assertContains('read("any")', $result->getPermissions());
        $this->assertContains('any', $result->getRead());
        $this->assertContains(Permission::create(Role::user('608fdbe51361a')), $result->getPermissions());
        $this->assertContains('user:608fdbe51361a', $result->getCreate());
        $this->assertContains('user:608fdbe51361a', $result->getWrite());
        $this->assertEquals('test@example.com', $result->getAttribute('email'));
        $this->assertEquals(false, $result->getAttribute('emailVerification'));
        $this->assertEquals(1, $result->getAttribute('status'));
        $this->assertEquals('randomhash', $result->getAttribute('password'));
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $result->getAttribute('passwordUpdate'));
        $this->assertEquals('1975-06-12T13:12:55.000+00:00', $result->getAttribute('registration'));
        $this->assertEquals(false, $result->getAttribute('reset'));
        $this->assertEquals('My Name', $result->getAttribute('name'));
        $this->assertEquals([], $result->getAttribute('prefs'));
        $this->assertEquals([], $result->getAttribute('sessions'));
        $this->assertEquals([], $result->getAttribute('tokens'));
        $this->assertEquals([], $result->getAttribute('memberships'));
        $this->assertEquals(['admin', 'developer', 'tester',], $result->getAttribute('roles'));
        $this->assertEquals([
            new Document(['$id' => '1', 'label' => 'x']),
            new Document(['$id' => '2', 'label' => 'y']),
            new Document(['$id' => '3', 'label' => 'z']),
        ], $result->getAttribute('tags'));
    }
    /**
     * @depends testGetDocument
     */
    public function testUpdateDocument(Document $document): Document
    {
        $document
            ->setAttribute('string', 'text📝 updated')
            ->setAttribute('integer_signed', -6)
            ->setAttribute('integer_unsigned', 6)
            ->setAttribute('float_signed', -5.56)
            ->setAttribute('float_unsigned', 5.56)
            ->setAttribute('boolean', false)
            ->setAttribute('colors', 'red', Document::SET_TYPE_APPEND)
            ->setAttribute('with-dash', 'Works');

        $new = $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);

        $this->assertNotEmpty(true, $new->getId());
        $this->assertIsString($new->getAttribute('string'));
        $this->assertEquals('text📝 updated', $new->getAttribute('string'));
        $this->assertIsInt($new->getAttribute('integer_signed'));
        $this->assertEquals(-6, $new->getAttribute('integer_signed'));
        $this->assertIsInt($new->getAttribute('integer_unsigned'));
        $this->assertEquals(6, $new->getAttribute('integer_unsigned'));
        $this->assertIsFloat($new->getAttribute('float_signed'));
        $this->assertEquals(-5.56, $new->getAttribute('float_signed'));
        $this->assertIsFloat($new->getAttribute('float_unsigned'));
        $this->assertEquals(5.56, $new->getAttribute('float_unsigned'));
        $this->assertIsBool($new->getAttribute('boolean'));
        $this->assertEquals(false, $new->getAttribute('boolean'));
        $this->assertIsArray($new->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue', 'red'], $new->getAttribute('colors'));
        $this->assertEquals('Works', $new->getAttribute('with-dash'));

        $oldPermissions = $document->getPermissions();

        $new
            ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::update(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::delete(Role::guests()), Document::SET_TYPE_APPEND);

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertContains('guests', $new->getRead());
        $this->assertContains('guests', $new->getWrite());
        $this->assertContains('guests', $new->getCreate());
        $this->assertContains('guests', $new->getUpdate());
        $this->assertContains('guests', $new->getDelete());

        $new->setAttribute('$permissions', $oldPermissions);

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertNotContains('guests', $new->getRead());
        $this->assertNotContains('guests', $new->getWrite());
        $this->assertNotContains('guests', $new->getCreate());
        $this->assertNotContains('guests', $new->getUpdate());
        $this->assertNotContains('guests', $new->getDelete());

        // Test change document ID
        $id = $new->getId();
        $newId = 'new-id';
        $new->setAttribute('$id', $newId);
        $new = $this->getDatabase()->updateDocument($new->getCollection(), $id, $new);
        $this->assertEquals($newId, $new->getId());

        // Reset ID
        $new->setAttribute('$id', $id);
        $new = $this->getDatabase()->updateDocument($new->getCollection(), $newId, $new);
        $this->assertEquals($id, $new->getId());

        return $document;
    }

    /**
     * @depends testUpdateDocument
     */
    public function testUpdateDocumentConflict(Document $document): void
    {
        $document->setAttribute('integer_signed', 7);
        $result = $this->getDatabase()->withRequestTimestamp(new \DateTime(), function () use ($document) {
            return $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);
        });
        $this->assertEquals(7, $result->getAttribute('integer_signed'));

        $oneHourAgo = (new \DateTime())->sub(new \DateInterval('PT1H'));
        $document->setAttribute('integer_signed', 8);
        try {
            $this->getDatabase()->withRequestTimestamp($oneHourAgo, function () use ($document) {
                return $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);
            });
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof ConflictException);
            $this->assertEquals('Document was updated after the request timestamp', $e->getMessage());
        }
    }

    /**
     * @depends testUpdateDocument
     */
    public function testDeleteDocumentConflict(Document $document): void
    {
        $oneHourAgo = (new \DateTime())->sub(new \DateInterval('PT1H'));
        $this->expectException(ConflictException::class);
        $this->getDatabase()->withRequestTimestamp($oneHourAgo, function () use ($document) {
            return $this->getDatabase()->deleteDocument($document->getCollection(), $document->getId());
        });
    }

    /**
     * @depends testGetDocument
     */
    public function testUpdateDocumentDuplicatePermissions(Document $document): Document
    {
        $new = $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);

        $new
            ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::read(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND)
            ->setAttribute('$permissions', Permission::create(Role::guests()), Document::SET_TYPE_APPEND);

        $this->getDatabase()->updateDocument($new->getCollection(), $new->getId(), $new);

        $new = $this->getDatabase()->getDocument($new->getCollection(), $new->getId());

        $this->assertContains('guests', $new->getRead());
        $this->assertContains('guests', $new->getCreate());

        return $document;
    }

    /**
     * @depends testUpdateDocument
     */
    public function testDeleteDocument(Document $document): void
    {
        $result = $this->getDatabase()->deleteDocument($document->getCollection(), $document->getId());
        $document = $this->getDatabase()->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $result);
        $this->assertEquals(true, $document->isEmpty());
    }

    public function testUpdateDocuments(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'testUpdateDocuments';
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        static::getDatabase()->createCollection($collection, attributes: [
            new Document([
                '$id' => ID::custom('string'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('integer'),
                'type' => Database::VAR_INTEGER,
                'format' => '',
                'size' => 10000,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ], documentSecurity: false);

        for ($i = 0; $i < 10; $i++) {
            static::getDatabase()->createDocument($collection, new Document([
                '$id' => 'doc' . $i,
                'string' => 'text📝 ' . $i,
                'integer' => $i
            ]));
        }

        // Test Update half of the documents
        $results = [];
        $count = static::getDatabase()->updateDocuments($collection, new Document([
            'string' => 'text📝 updated',
        ]), [
            Query::greaterThanEqual('integer', 5),
        ], onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(5, $count);

        foreach ($results as $document) {
            $this->assertEquals('text📝 updated', $document->getAttribute('string'));
        }

        $updatedDocuments = static::getDatabase()->find($collection, [
            Query::greaterThanEqual('integer', 5),
        ]);

        $this->assertCount(5, $updatedDocuments);

        foreach ($updatedDocuments as $document) {
            $this->assertEquals('text📝 updated', $document->getAttribute('string'));
            $this->assertGreaterThanOrEqual(5, $document->getAttribute('integer'));
        }

        $controlDocuments = static::getDatabase()->find($collection, [
            Query::lessThan('integer', 5),
        ]);

        $this->assertEquals(count($controlDocuments), 5);

        foreach ($controlDocuments as $document) {
            $this->assertNotEquals('text📝 updated', $document->getAttribute('string'));
        }

        // Test Update all documents
        $this->assertEquals(10, static::getDatabase()->updateDocuments($collection, new Document([
            'string' => 'text📝 updated all',
        ])));

        $updatedDocuments = static::getDatabase()->find($collection);

        $this->assertEquals(count($updatedDocuments), 10);

        foreach ($updatedDocuments as $document) {
            $this->assertEquals('text📝 updated all', $document->getAttribute('string'));
        }

        // TEST: Can't delete documents in the past
        $oneHourAgo = (new \DateTime())->sub(new \DateInterval('PT1H'));

        try {
            $this->getDatabase()->withRequestTimestamp($oneHourAgo, function () use ($collection) {
                static::getDatabase()->updateDocuments($collection, new Document([
                    'string' => 'text📝 updated all',
                ]));
            });
            $this->fail('Failed to throw exception');
        } catch (ConflictException $e) {
            $this->assertEquals('Document was updated after the request timestamp', $e->getMessage());
        }

        // Check collection level permissions
        static::getDatabase()->updateCollection($collection, permissions: [
            Permission::read(Role::user('asd')),
            Permission::create(Role::user('asd')),
            Permission::update(Role::user('asd')),
            Permission::delete(Role::user('asd')),
        ], documentSecurity: false);

        try {
            static::getDatabase()->updateDocuments($collection, new Document([
                'string' => 'text📝 updated all',
            ]));
            $this->fail('Failed to throw exception');
        } catch (AuthorizationException $e) {
            $this->assertStringStartsWith('Missing "update" permission for role "user:asd".', $e->getMessage());
        }

        // Check document level permissions
        static::getDatabase()->updateCollection($collection, permissions: [], documentSecurity: true);

        Authorization::skip(function () use ($collection) {
            static::getDatabase()->updateDocument($collection, 'doc0', new Document([
                'string' => 'text📝 updated all',
                '$permissions' => [
                    Permission::read(Role::user('asd')),
                    Permission::create(Role::user('asd')),
                    Permission::update(Role::user('asd')),
                    Permission::delete(Role::user('asd')),
                ],
            ]));
        });

        Authorization::setRole(Role::user('asd')->toString());

        static::getDatabase()->updateDocuments($collection, new Document([
            'string' => 'permission text',
        ]));

        $documents = static::getDatabase()->find($collection, [
            Query::equal('string', ['permission text']),
        ]);

        $this->assertCount(1, $documents);

        Authorization::skip(function () use ($collection) {
            $unmodifiedDocuments = static::getDatabase()->find($collection, [
                Query::equal('string', ['text📝 updated all']),
            ]);

            $this->assertCount(9, $unmodifiedDocuments);
        });

        Authorization::skip(function () use ($collection) {
            static::getDatabase()->updateDocuments($collection, new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]));
        });

        // Test we can update more documents than batchSize
        $this->assertEquals(10, static::getDatabase()->updateDocuments($collection, new Document([
            'string' => 'batchSize Test'
        ]), batchSize: 2));

        $documents = static::getDatabase()->find($collection);

        foreach ($documents as $document) {
            $this->assertEquals('batchSize Test', $document->getAttribute('string'));
        }

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());
    }
    /**
     * @depends testCreateDocument
     */
    public function testReadPermissionsSuccess(Document $document): Document
    {
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::INT_MAX,
            'integer_unsigned' => Database::INT_MAX,
            'bigint_signed' => -Database::BIG_INT_MAX,
            'bigint_unsigned' => Database::BIG_INT_MAX,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $this->assertEquals(false, $document->isEmpty());

        Authorization::cleanRoles();

        $document = static::getDatabase()->getDocument($document->getCollection(), $document->getId());
        $this->assertEquals(true, $document->isEmpty());

        Authorization::setRole(Role::any()->toString());

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testWritePermissionsSuccess(Document $document): void
    {
        Authorization::cleanRoles();

        $this->expectException(AuthorizationException::class);
        static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::INT_MAX,
            'integer_unsigned' => Database::INT_MAX,
            'bigint_signed' => -Database::BIG_INT_MAX,
            'bigint_unsigned' => Database::BIG_INT_MAX,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));
    }

    /**
     * @depends testCreateDocument
     */
    public function testWritePermissionsUpdateFailure(Document $document): Document
    {
        $this->expectException(AuthorizationException::class);

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $document = static::getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::INT_MAX,
            'integer_unsigned' => Database::INT_MAX,
            'bigint_signed' => -Database::BIG_INT_MAX,
            'bigint_unsigned' => Database::BIG_INT_MAX,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        Authorization::cleanRoles();

        $document = static::getDatabase()->updateDocument('documents', $document->getId(), new Document([
            '$id' => ID::custom($document->getId()),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer_signed' => 6,
            'bigint_signed' => -Database::BIG_INT_MAX,
            'float_signed' => -Database::DOUBLE_MAX,
            'float_unsigned' => Database::DOUBLE_MAX,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        return $document;
    }

    /**
     * @depends testFind
     */
    public function testUniqueIndexDuplicate(): void
    {
        $this->expectException(DuplicateException::class);

        $this->assertEquals(true, static::getDatabase()->createIndex('movies', 'uniqueIndex', Database::INDEX_UNIQUE, ['name'], [128], [Database::ORDER_ASC]));

        static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'genres' => ['animation', 'kids'],
            'with-dash' => 'Works4'
        ]));
    }
    /**
     * @depends testUniqueIndexDuplicate
     */
    public function testUniqueIndexDuplicateUpdate(): void
    {
        Authorization::setRole(Role::users()->toString());
        // create document then update to conflict with index
        $document = static::getDatabase()->createDocument('movies', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('1')),
                Permission::read(Role::user('2')),
                Permission::create(Role::any()),
                Permission::create(Role::user('1x')),
                Permission::create(Role::user('2x')),
                Permission::update(Role::any()),
                Permission::update(Role::user('1x')),
                Permission::update(Role::user('2x')),
                Permission::delete(Role::any()),
                Permission::delete(Role::user('1x')),
                Permission::delete(Role::user('2x')),
            ],
            'name' => 'Frozen 5',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'genres' => ['animation', 'kids'],
            'with-dash' => 'Works4'
        ]));

        $this->expectException(DuplicateException::class);

        static::getDatabase()->updateDocument('movies', $document->getId(), $document->setAttribute('name', 'Frozen'));
    }

    public function propagateBulkDocuments(string $collection, int $amount = 10, bool $documentSecurity = false): void
    {
        for ($i = 0; $i < $amount; $i++) {
            static::getDatabase()->createDocument($collection, new Document(
                array_merge([
                    '$id' => 'doc' . $i,
                    'text' => 'value' . $i,
                    'integer' => $i
                ], $documentSecurity ? [
                    '$permissions' => [
                        Permission::create(Role::any()),
                        Permission::read(Role::any()),
                    ],
                ] : [])
            ));
        }
    }

    public function testDeleteBulkDocuments(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection(
            'bulk_delete',
            attributes: [
                new Document([
                    '$id' => 'text',
                    'type' => Database::VAR_STRING,
                    'size' => 100,
                    'required' => true,
                ]),
                new Document([
                    '$id' => 'integer',
                    'type' => Database::VAR_INTEGER,
                    'size' => 10,
                    'required' => true,
                ])
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::delete(Role::any())
            ],
            documentSecurity: false
        );

        $this->propagateBulkDocuments('bulk_delete');

        $docs = static::getDatabase()->find('bulk_delete');
        $this->assertCount(10, $docs);

        /**
         * Test Short select query, test pagination as well, Add order to select
         */
        $mandatory = ['$internalId', '$id', '$collection', '$permissions', '$updatedAt'];

        $count = static::getDatabase()->deleteDocuments(
            collection: 'bulk_delete',
            queries: [
                Query::select('$createdAt'),
                ...array_map(fn ($f) => Query::select($f), $mandatory),
                Query::cursorAfter($docs[6]),
                Query::greaterThan('$createdAt', '2000-01-01'),
                Query::orderAsc('$createdAt'),
                Query::orderAsc(),
                Query::limit(2),
            ],
            batchSize: 1
        );

        $this->assertEquals(2, $count);

        // TEST: Bulk Delete All Documents
        $this->assertEquals(8, static::getDatabase()->deleteDocuments('bulk_delete'));

        $docs = static::getDatabase()->find('bulk_delete');
        $this->assertCount(0, $docs);

        // TEST: Bulk delete documents with queries.
        $this->propagateBulkDocuments('bulk_delete');

        $results = [];
        $count = static::getDatabase()->deleteDocuments('bulk_delete', [
            Query::greaterThanEqual('integer', 5)
        ], onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(5, $count);

        foreach ($results as $document) {
            $this->assertGreaterThanOrEqual(5, $document->getAttribute('integer'));
        }

        $docs = static::getDatabase()->find('bulk_delete');
        $this->assertEquals(5, \count($docs));

        // TEST (FAIL): Can't delete documents in the past
        $oneHourAgo = (new \DateTime())->sub(new \DateInterval('PT1H'));

        try {
            $this->getDatabase()->withRequestTimestamp($oneHourAgo, function () {
                return $this->getDatabase()->deleteDocuments('bulk_delete');
            });
            $this->fail('Failed to throw exception');
        } catch (ConflictException $e) {
            $this->assertEquals('Document was updated after the request timestamp', $e->getMessage());
        }

        // TEST (FAIL): Bulk delete all documents with invalid collection permission
        static::getDatabase()->updateCollection('bulk_delete', [], false);
        try {
            static::getDatabase()->deleteDocuments('bulk_delete');
            $this->fail('Bulk deleted documents with invalid collection permission');
        } catch (\Utopia\Database\Exception\Authorization) {
        }

        static::getDatabase()->updateCollection('bulk_delete', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::delete(Role::any())
        ], false);

        $this->assertEquals(5, static::getDatabase()->deleteDocuments('bulk_delete'));
        $this->assertEquals(0, \count($this->getDatabase()->find('bulk_delete')));

        // TEST: Make sure we can't delete documents we don't have permissions for
        static::getDatabase()->updateCollection('bulk_delete', [
            Permission::create(Role::any()),
        ], true);
        $this->propagateBulkDocuments('bulk_delete', documentSecurity: true);

        $this->assertEquals(0, static::getDatabase()->deleteDocuments('bulk_delete'));

        $documents = Authorization::skip(function () {
            return static::getDatabase()->find('bulk_delete');
        });

        $this->assertEquals(10, \count($documents));

        static::getDatabase()->updateCollection('bulk_delete', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::delete(Role::any())
        ], false);

        static::getDatabase()->deleteDocuments('bulk_delete');

        $this->assertEquals(0, \count($this->getDatabase()->find('bulk_delete')));

        // Teardown
        static::getDatabase()->deleteCollection('bulk_delete');
    }

    public function testDeleteBulkDocumentsQueries(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection(
            'bulk_delete_queries',
            attributes: [
                new Document([
                    '$id' => 'text',
                    'type' => Database::VAR_STRING,
                    'size' => 100,
                    'required' => true,
                ]),
                new Document([
                    '$id' => 'integer',
                    'type' => Database::VAR_INTEGER,
                    'size' => 10,
                    'required' => true,
                ])
            ],
            documentSecurity: false,
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::delete(Role::any())
            ]
        );

        // Test limit
        $this->propagateBulkDocuments('bulk_delete_queries');

        $this->assertEquals(5, static::getDatabase()->deleteDocuments('bulk_delete_queries', [Query::limit(5)]));
        $this->assertEquals(5, \count(static::getDatabase()->find('bulk_delete_queries')));

        $this->assertEquals(5, static::getDatabase()->deleteDocuments('bulk_delete_queries', [Query::limit(5)]));
        $this->assertEquals(0, \count(static::getDatabase()->find('bulk_delete_queries')));

        // Test Limit more than batchSize
        $this->propagateBulkDocuments('bulk_delete_queries', Database::DELETE_BATCH_SIZE * 2);
        $this->assertEquals(Database::DELETE_BATCH_SIZE * 2, \count(static::getDatabase()->find('bulk_delete_queries', [Query::limit(Database::DELETE_BATCH_SIZE * 2)])));
        $this->assertEquals(Database::DELETE_BATCH_SIZE + 2, static::getDatabase()->deleteDocuments('bulk_delete_queries', [Query::limit(Database::DELETE_BATCH_SIZE + 2)]));
        $this->assertEquals(Database::DELETE_BATCH_SIZE - 2, \count(static::getDatabase()->find('bulk_delete_queries', [Query::limit(Database::DELETE_BATCH_SIZE * 2)])));
        $this->assertEquals(Database::DELETE_BATCH_SIZE - 2, $this->getDatabase()->deleteDocuments('bulk_delete_queries'));

        // Test Offset
        $this->propagateBulkDocuments('bulk_delete_queries', 100);
        $this->assertEquals(50, static::getDatabase()->deleteDocuments('bulk_delete_queries', [Query::offset(50)]));

        $docs = static::getDatabase()->find('bulk_delete_queries', [Query::limit(100)]);
        $this->assertEquals(50, \count($docs));

        $lastDoc = \end($docs);
        $this->assertNotEmpty($lastDoc);
        $this->assertEquals('doc49', $lastDoc->getId());
        $this->assertEquals(50, static::getDatabase()->deleteDocuments('bulk_delete_queries'));

        static::getDatabase()->deleteCollection('bulk_delete_queries');
    }

    public function testUpdateDocumentsQueries(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'testUpdateDocumentsQueries';

        static::getDatabase()->createCollection($collection, attributes: [
            new Document([
                '$id' => ID::custom('text'),
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
            ]),
            new Document([
                '$id' => ID::custom('integer'),
                'type' => Database::VAR_INTEGER,
                'size' => 64,
                'required' => true,
            ]),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ], documentSecurity: true);

        // Test limit
        $this->propagateBulkDocuments($collection, 100);

        $this->assertEquals(10, static::getDatabase()->updateDocuments($collection, new Document([
            'text' => 'text📝 updated',
        ]), [Query::limit(10)]));

        $this->assertEquals(10, \count(static::getDatabase()->find($collection, [Query::equal('text', ['text📝 updated'])])));
        $this->assertEquals(100, static::getDatabase()->deleteDocuments($collection));
        $this->assertEquals(0, \count(static::getDatabase()->find($collection)));

        // Test Offset
        $this->propagateBulkDocuments($collection, 100);
        $this->assertEquals(50, static::getDatabase()->updateDocuments($collection, new Document([
            'text' => 'text📝 updated',
        ]), [
            Query::offset(50),
        ]));

        $docs = static::getDatabase()->find($collection, [Query::equal('text', ['text📝 updated']), Query::limit(100)]);
        $this->assertCount(50, $docs);

        $lastDoc = end($docs);
        $this->assertNotEmpty($lastDoc);
        $this->assertEquals('doc99', $lastDoc->getId());

        $this->assertEquals(100, static::getDatabase()->deleteDocuments($collection));
    }

    /**
     * @depends testCreateDocument
     */
    public function testFulltextIndexWithInteger(): void
    {
        $this->expectException(Exception::class);

        if (!$this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $this->expectExceptionMessage('Fulltext index is not supported');
        } else {
            $this->expectExceptionMessage('Attribute "integer_signed" cannot be part of a FULLTEXT index, must be of type string');
        }

        static::getDatabase()->createIndex('documents', 'fulltext_integer', Database::INDEX_FULLTEXT, ['string','integer_signed']);
    }

    public function testEnableDisableValidation(): void
    {
        $database = static::getDatabase();

        $database->createCollection('validation', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $database->createAttribute(
            'validation',
            'name',
            Database::VAR_STRING,
            10,
            false
        );

        $database->createDocument('validation', new Document([
            '$id' => 'docwithmorethan36charsasitsidentifier',
            'name' => 'value1',
        ]));

        try {
            $database->find('validation', queries: [
                Query::equal('$id', ['docwithmorethan36charsasitsidentifier']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
        }

        $database->disableValidation();

        $database->find('validation', queries: [
            Query::equal('$id', ['docwithmorethan36charsasitsidentifier']),
        ]);

        $database->enableValidation();

        try {
            $database->find('validation', queries: [
                Query::equal('$id', ['docwithmorethan36charsasitsidentifier']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
        }

        $database->skipValidation(function () use ($database) {
            $database->find('validation', queries: [
                Query::equal('$id', ['docwithmorethan36charsasitsidentifier']),
            ]);
        });

        $database->enableValidation();
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionDuplicate(Document $document): void
    {
        $document->setAttribute('$id', 'duplicated');
        static::getDatabase()->createDocument($document->getCollection(), $document);
        $this->expectException(DuplicateException::class);
        static::getDatabase()->createDocument($document->getCollection(), $document);
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionCaseInsensitiveDuplicate(Document $document): Document
    {
        $document->setAttribute('$id', 'caseSensitive');
        $document->setAttribute('$internalId', '200');
        static::getDatabase()->createDocument($document->getCollection(), $document);

        $document->setAttribute('$id', 'CaseSensitive');

        $this->expectException(DuplicateException::class);
        static::getDatabase()->createDocument($document->getCollection(), $document);

        return $document;
    }

    public function testEmptyTenant(): void
    {
        if (static::getDatabase()->getAdapter()->getSharedTables()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $documents = static::getDatabase()->find(
            'documents',
            [Query::notEqual('$id', '56000')] // Mongo bug with Integer UID
        );

        $document = $documents[0];
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);

        $document = static::getDatabase()->getDocument('documents', $document->getId());
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);

        $document = static::getDatabase()->updateDocument('documents', $document->getId(), $document);
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);
    }

    public function testEmptyOperatorValues(): void
    {
        try {
            static::getDatabase()->findOne('documents', [
                Query::equal('string', []),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals('Invalid query: Equal queries require at least one value.', $e->getMessage());
        }

        try {
            static::getDatabase()->findOne('documents', [
                Query::contains('string', []),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals('Invalid query: Contains queries require at least one value.', $e->getMessage());
        }
    }
}

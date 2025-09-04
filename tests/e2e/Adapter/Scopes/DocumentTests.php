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
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

trait DocumentTests
{
    public function testCreateDocument(): Document
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection('documents');

        $this->assertEquals(true, $database->createAttribute('documents', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $database->createAttribute('documents', 'integer_signed', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $database->createAttribute('documents', 'integer_unsigned', Database::VAR_INTEGER, 4, true, signed: false));
        $this->assertEquals(true, $database->createAttribute('documents', 'bigint_signed', Database::VAR_INTEGER, 8, true));
        $this->assertEquals(true, $database->createAttribute('documents', 'bigint_unsigned', Database::VAR_INTEGER, 9, true, signed: false));
        $this->assertEquals(true, $database->createAttribute('documents', 'float_signed', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, $database->createAttribute('documents', 'float_unsigned', Database::VAR_FLOAT, 0, true, signed: false));
        $this->assertEquals(true, $database->createAttribute('documents', 'boolean', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $database->createAttribute('documents', 'colors', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, $database->createAttribute('documents', 'empty', Database::VAR_STRING, 32, false, null, true, true));
        $this->assertEquals(true, $database->createAttribute('documents', 'with-dash', Database::VAR_STRING, 128, false, null));
        $this->assertEquals(true, $database->createAttribute('documents', 'id', Database::VAR_ID, 0, false, null));

        $document = $database->createDocument('documents', new Document([
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
            'id' => '1000000',
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
        $this->assertIsString($document->getAttribute('id'));
        $this->assertEquals('1000000', $document->getAttribute('id'));

        // Test create document with manual internal id
        $manualIdDocument = $database->createDocument('documents', new Document([
            '$id' => '56000',
            '$sequence' => '56000',
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

        $this->assertEquals('56000', $manualIdDocument->getSequence());
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
        $this->assertEquals(null, $manualIdDocument->getAttribute('id'));

        $manualIdDocument = $database->getDocument('documents', '56000');

        $this->assertEquals('56000', $manualIdDocument->getSequence());
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
            $database->createDocument('documents', new Document([
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
            $database->createDocument('documents', new Document([
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

        try {
            $database->createDocument('documents', new Document([
                '$sequence' => '0',
                '$permissions' => [],
                'string' => '',
                'integer_signed' => 1,
                'integer_unsigned' => 1,
                'bigint_signed' => 1,
                'bigint_unsigned' => 1,
                'float_signed' => 1,
                'float_unsigned' => 1,
                'boolean' => true,
                'colors' => [],
                'empty' => [],
                'with-dash' => '',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof StructureException);
            $this->assertEquals('Invalid document structure: Attribute "$sequence" has invalid type. Invalid sequence value', $e->getMessage());
        }

        /**
         * Insert ID attribute with NULL
         */

        $documentIdNull = $database->createDocument('documents', new Document([
            'id' => null,
            '$permissions' => [Permission::read(Role::any())],
            'string' => '',
            'integer_signed' => 1,
            'integer_unsigned' => 1,
            'bigint_signed' => 1,
            'bigint_unsigned' => 1,
            'float_signed' => 1,
            'float_unsigned' => 1,
            'boolean' => true,
            'colors' => [],
            'empty' => [],
            'with-dash' => '',
        ]));
        $this->assertNotEmpty(true, $documentIdNull->getSequence());
        $this->assertNull($documentIdNull->getAttribute('id'));

        $documentIdNull = $database->getDocument('documents', $documentIdNull->getId());
        $this->assertNotEmpty(true, $documentIdNull->getId());
        $this->assertNull($documentIdNull->getAttribute('id'));

        $documentIdNull = $database->findOne('documents', [
            query::isNull('id')
        ]);
        $this->assertNotEmpty(true, $documentIdNull->getId());
        $this->assertNull($documentIdNull->getAttribute('id'));

        /**
         * Insert ID attribute with '0'
         */
        $documentId0 = $database->createDocument('documents', new Document([
            'id' => '0',
            '$permissions' => [Permission::read(Role::any())],
            'string' => '',
            'integer_signed' => 1,
            'integer_unsigned' => 1,
            'bigint_signed' => 1,
            'bigint_unsigned' => 1,
            'float_signed' => 1,
            'float_unsigned' => 1,
            'boolean' => true,
            'colors' => [],
            'empty' => [],
            'with-dash' => '',
        ]));
        $this->assertNotEmpty(true, $documentId0->getSequence());
        $this->assertIsString($documentId0->getAttribute('id'));
        $this->assertEquals('0', $documentId0->getAttribute('id'));

        $documentId0 = $database->getDocument('documents', $documentId0->getId());
        $this->assertNotEmpty(true, $documentId0->getSequence());
        $this->assertIsString($documentId0->getAttribute('id'));
        $this->assertEquals('0', $documentId0->getAttribute('id'));

        $documentId0 = $database->findOne('documents', [
            query::equal('id', ['0'])
        ]);
        $this->assertNotEmpty(true, $documentId0->getSequence());
        $this->assertIsString($documentId0->getAttribute('id'));
        $this->assertEquals('0', $documentId0->getAttribute('id'));


        return $document;
    }

    public function testCreateDocuments(): void
    {
        $count = 3;
        $collection = 'testCreateDocuments';

        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection($collection);

        $this->assertEquals(true, $database->createAttribute($collection, 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $database->createAttribute($collection, 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $database->createAttribute($collection, 'bigint', Database::VAR_INTEGER, 8, true));

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

        $results = [];

        $count = $database->createDocuments($collection, $documents, 3, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals($count, \count($results));

        foreach ($results as $document) {
            $this->assertNotEmpty(true, $document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(9223372036854775807, $document->getAttribute('bigint'));
        }

        $documents = $database->find($collection, [
            Query::orderAsc()
        ]);

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
    }

    public function testCreateDocumentsWithAutoIncrement(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection(__FUNCTION__);

        $this->assertEquals(true, $database->createAttribute(__FUNCTION__, 'string', Database::VAR_STRING, 128, true));

        /** @var array<Document> $documents */
        $documents = [];
        $count = 10;
        $sequence = 1_000_000;

        for ($i = $sequence; $i <= ($sequence + $count); $i++) {
            $documents[] = new Document([
                '$sequence' => (string)$i,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'string' => 'text',
            ]);
        }

        $count = $database->createDocuments(__FUNCTION__, $documents, 6);
        $this->assertEquals($count, \count($documents));

        $documents = $database->find(__FUNCTION__, [
            Query::orderAsc()
        ]);
        foreach ($documents as $index => $document) {
            $this->assertEquals($sequence + $index, $document->getSequence());
            $this->assertNotEmpty(true, $document->getId());
            $this->assertEquals('text', $document->getAttribute('string'));
        }
    }

    public function testCreateDocumentsWithDifferentAttributes(): void
    {
        $collection = 'testDiffAttributes';

        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection($collection);

        $this->assertEquals(true, $database->createAttribute($collection, 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $database->createAttribute($collection, 'integer', Database::VAR_INTEGER, 0, false));
        $this->assertEquals(true, $database->createAttribute($collection, 'bigint', Database::VAR_INTEGER, 8, false));
        $this->assertEquals(true, $database->createAttribute($collection, 'string_default', Database::VAR_STRING, 128, false, 'default'));

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
        $count = $database->createDocuments($collection, $documents, onNext: function ($doc) use (&$results) {
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
         * Expect fail, mix of sequence and no sequence
         */
        $documents = [
            new Document([
                '$id' => 'third',
                '$sequence' => 'third',
                'string' => 'text📝',
            ]),
            new Document([
                '$id' => 'fourth',
                'string' => 'text📝',
            ]),
        ];

        try {
            $database->createDocuments($collection, $documents);
            $this->fail('Failed to throw exception');
        } catch (DatabaseException $e) {
        }

        $documents = array_reverse($documents);
        try {
            $database->createDocuments($collection, $documents);
            $this->fail('Failed to throw exception');
        } catch (DatabaseException $e) {
        }

        $database->deleteCollection($collection);
    }

    public function testSkipPermissions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection(__FUNCTION__);
        $database->createAttribute(__FUNCTION__, 'number', Database::VAR_INTEGER, 0, false);

        $data = [];
        for ($i = 1; $i <= 10; $i++) {
            $data[] = [
                '$id' => "$i",
                'number' => $i,
            ];
        }

        $documents = array_map(fn ($d) => new Document($d), $data);

        $results = [];
        $count = $database->createDocuments(__FUNCTION__, $documents, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals($count, \count($results));
        $this->assertEquals(10, \count($results));

        /**
         * Update 1 row
         */
        $data[\array_key_last($data)]['number'] = 100;

        /**
         * Add 1 row
         */
        $data[] = [
            '$id' => "101",
            'number' => 101,
        ];

        $documents = array_map(fn ($d) => new Document($d), $data);

        Authorization::disable();

        $results = [];
        $count = $database->upsertDocuments(
            __FUNCTION__,
            $documents,
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );

        Authorization::reset();

        $this->assertEquals(2, \count($results));
        $this->assertEquals(2, $count);

        foreach ($results as $result) {
            $this->assertArrayHasKey('$permissions', $result);
            $this->assertEquals([], $result->getAttribute('$permissions'));
        }
    }

    public function testUpsertDocuments(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection(__FUNCTION__);
        $database->createAttribute(__FUNCTION__, 'string', Database::VAR_STRING, 128, true);
        $database->createAttribute(__FUNCTION__, 'integer', Database::VAR_INTEGER, 0, true);
        $database->createAttribute(__FUNCTION__, 'bigint', Database::VAR_INTEGER, 8, true);

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
        $count = $database->upsertDocuments(
            __FUNCTION__,
            $documents,
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );

        $this->assertEquals(2, $count);

        $createdAt = [];
        foreach ($results as $index => $document) {
            $createdAt[$index] = $document->getCreatedAt();
            $this->assertNotEmpty(true, $document->getId());
            $this->assertNotEmpty(true, $document->getSequence());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::BIG_INT_MAX, $document->getAttribute('bigint'));
        }

        $documents = $database->find(__FUNCTION__);

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
        $count = $database->upsertDocuments(__FUNCTION__, $documents, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(2, $count);

        foreach ($results as $document) {
            $this->assertNotEmpty(true, $document->getId());
            $this->assertNotEmpty(true, $document->getSequence());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('new text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(10, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::BIG_INT_MAX, $document->getAttribute('bigint'));
        }

        $documents = $database->find(__FUNCTION__);

        $this->assertEquals(2, count($documents));

        foreach ($documents as $index => $document) {
            $this->assertEquals($createdAt[$index], $document->getCreatedAt());
            $this->assertNotEmpty(true, $document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('new text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(10, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::BIG_INT_MAX, $document->getAttribute('bigint'));
        }
    }

    public function testUpsertDocumentsInc(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection(__FUNCTION__);
        $database->createAttribute(__FUNCTION__, 'string', Database::VAR_STRING, 128, false);
        $database->createAttribute(__FUNCTION__, 'integer', Database::VAR_INTEGER, 0, false);

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

        $database->createDocuments(__FUNCTION__, $documents);

        $documents[0]->setAttribute('integer', 1);
        $documents[1]->setAttribute('integer', 1);

        $database->upsertDocumentsWithIncrease(
            collection: __FUNCTION__,
            attribute: 'integer',
            documents: $documents
        );

        $documents = $database->find(__FUNCTION__);

        foreach ($documents as $document) {
            $this->assertEquals(6, $document->getAttribute('integer'));
        }

        $documents[0]->setAttribute('integer', -1);
        $documents[1]->setAttribute('integer', -1);

        $database->upsertDocumentsWithIncrease(
            collection: __FUNCTION__,
            attribute: 'integer',
            documents: $documents
        );

        $documents = $database->find(__FUNCTION__);

        foreach ($documents as $document) {
            $this->assertEquals(5, $document->getAttribute('integer'));
        }
    }

    public function testUpsertDocumentsPermissions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection(__FUNCTION__);
        $database->createAttribute(__FUNCTION__, 'string', Database::VAR_STRING, 128, true);

        $document = new Document([
            '$id' => 'first',
            'string' => 'text📝',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
            ],
        ]);

        $database->upsertDocuments(__FUNCTION__, [$document]);

        try {
            $database->upsertDocuments(__FUNCTION__, [$document->setAttribute('string', 'updated')]);
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

        $database->upsertDocuments(__FUNCTION__, [$document]);

        $results = [];
        $count = $database->upsertDocuments(
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

        $database->upsertDocuments(__FUNCTION__, [$document]);

        $newPermissions = [
            Permission::read(Role::any()),
            Permission::update(Role::user('user1')),
            Permission::delete(Role::user('user1')),
        ];

        $results = [];
        $count = $database->upsertDocuments(
            __FUNCTION__,
            [$document->setAttribute('$permissions', $newPermissions)],
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );

        $this->assertEquals(1, $count);
        $this->assertEquals($newPermissions, $results[0]->getPermissions());

        $document = $database->getDocument(__FUNCTION__, 'third');

        $this->assertEquals($newPermissions, $document->getPermissions());
    }

    public function testUpsertDocumentsAttributeMismatch(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection(__FUNCTION__, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ], documentSecurity: false);
        $database->createAttribute(__FUNCTION__, 'first', Database::VAR_STRING, 128, true);
        $database->createAttribute(__FUNCTION__, 'last', Database::VAR_STRING, 128, false);

        $existingDocument = $database->createDocument(__FUNCTION__, new Document([
            '$id' => 'first',
            'first' => 'first',
            'last' => 'last',
        ]));

        $newDocument = new Document([
            '$id' => 'second',
            'first' => 'second',
        ]);

        // Ensure missing optionals on new document is allowed
        $docs = $database->upsertDocuments(__FUNCTION__, [
            $existingDocument->setAttribute('first', 'updated'),
            $newDocument,
        ]);

        $this->assertEquals(2, $docs);
        $this->assertEquals('updated', $existingDocument->getAttribute('first'));
        $this->assertEquals('last', $existingDocument->getAttribute('last'));
        $this->assertEquals('second', $newDocument->getAttribute('first'));
        $this->assertEquals('', $newDocument->getAttribute('last'));

        try {
            $database->upsertDocuments(__FUNCTION__, [
                $existingDocument->removeAttribute('first'),
                $newDocument
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof StructureException, $e->getMessage());
        }

        // Ensure missing optionals on existing document is allowed
        $docs = $database->upsertDocuments(__FUNCTION__, [
            $existingDocument
                ->setAttribute('first', 'first')
                ->removeAttribute('last'),
            $newDocument
                ->setAttribute('last', 'last')
        ]);

        $this->assertEquals(2, $docs);
        $this->assertEquals('first', $existingDocument->getAttribute('first'));
        $this->assertEquals('last', $existingDocument->getAttribute('last'));
        $this->assertEquals('second', $newDocument->getAttribute('first'));
        $this->assertEquals('last', $newDocument->getAttribute('last'));

        // Ensure set null on existing document is allowed
        $docs = $database->upsertDocuments(__FUNCTION__, [
            $existingDocument
                ->setAttribute('first', 'first')
                ->setAttribute('last', null),
            $newDocument
                ->setAttribute('last', 'last')
        ]);

        $this->assertEquals(1, $docs);
        $this->assertEquals('first', $existingDocument->getAttribute('first'));
        $this->assertEquals(null, $existingDocument->getAttribute('last'));
        $this->assertEquals('second', $newDocument->getAttribute('first'));
        $this->assertEquals('last', $newDocument->getAttribute('last'));

        $doc3 = new Document([
            '$id' => 'third',
            'last' => 'last',
            'first' => 'third',
        ]);

        $doc4 = new Document([
            '$id' => 'fourth',
            'first' => 'fourth',
            'last' => 'last',
        ]);

        // Ensure mismatch of attribute orders is allowed
        $docs = $database->upsertDocuments(__FUNCTION__, [
            $doc3,
            $doc4
        ]);

        $this->assertEquals(2, $docs);
        $this->assertEquals('third', $doc3->getAttribute('first'));
        $this->assertEquals('last', $doc3->getAttribute('last'));
        $this->assertEquals('fourth', $doc4->getAttribute('first'));
        $this->assertEquals('last', $doc4->getAttribute('last'));

        $doc3 = $database->getDocument(__FUNCTION__, 'third');
        $doc4 = $database->getDocument(__FUNCTION__, 'fourth');

        $this->assertEquals('third', $doc3->getAttribute('first'));
        $this->assertEquals('last', $doc3->getAttribute('last'));
        $this->assertEquals('fourth', $doc4->getAttribute('first'));
        $this->assertEquals('last', $doc4->getAttribute('last'));
    }

    public function testUpsertDocumentsNoop(): void
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
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]);

        $count = static::getDatabase()->upsertDocuments(__FUNCTION__, [$document]);
        $this->assertEquals(1, $count);

        // No changes, should return 0
        $count = static::getDatabase()->upsertDocuments(__FUNCTION__, [$document]);
        $this->assertEquals(0, $count);
    }

    public function testUpsertDuplicateIds(): void
    {
        $db = static::getDatabase();
        if (!$db->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $db->createCollection(__FUNCTION__);
        $db->createAttribute(__FUNCTION__, 'num', Database::VAR_INTEGER, 0, true);

        $doc1 = new Document(['$id' => 'dup', 'num' => 1]);
        $doc2 = new Document(['$id' => 'dup', 'num' => 2]);

        try {
            $db->upsertDocuments(__FUNCTION__, [$doc1, $doc2]);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(DuplicateException::class, $e, $e->getMessage());
        }
    }

    public function testUpsertMixedPermissionDelta(): void
    {
        $db = static::getDatabase();
        if (!$db->getAdapter()->getSupportForUpserts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $db->createCollection(__FUNCTION__);
        $db->createAttribute(__FUNCTION__, 'v', Database::VAR_INTEGER, 0, true);

        $d1 = $db->createDocument(__FUNCTION__, new Document([
            '$id' => 'a',
            'v' => 0,
            '$permissions' => [
                Permission::update(Role::any())
            ]
        ]));
        $d2 = $db->createDocument(__FUNCTION__, new Document([
            '$id' => 'b',
            'v' => 0,
            '$permissions' => [
                Permission::update(Role::any())
            ]
        ]));

        // d1 adds write, d2 removes update
        $d1->setAttribute('$permissions', [
            Permission::read(Role::any()),
            Permission::update(Role::any())
        ]);
        $d2->setAttribute('$permissions', [
            Permission::read(Role::any())
        ]);

        $db->upsertDocuments(__FUNCTION__, [$d1, $d2]);

        $this->assertEquals([
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ], $db->getDocument(__FUNCTION__, 'a')->getPermissions());

        $this->assertEquals([
            Permission::read(Role::any()),
        ], $db->getDocument(__FUNCTION__, 'b')->getPermissions());
    }

    public function testRespectNulls(): Document
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection('documents_nulls');

        $this->assertEquals(true, $database->createAttribute('documents_nulls', 'string', Database::VAR_STRING, 128, false));
        $this->assertEquals(true, $database->createAttribute('documents_nulls', 'integer', Database::VAR_INTEGER, 0, false));
        $this->assertEquals(true, $database->createAttribute('documents_nulls', 'bigint', Database::VAR_INTEGER, 8, false));
        $this->assertEquals(true, $database->createAttribute('documents_nulls', 'float', Database::VAR_FLOAT, 0, false));
        $this->assertEquals(true, $database->createAttribute('documents_nulls', 'boolean', Database::VAR_BOOLEAN, 0, false));

        $document = $database->createDocument('documents_nulls', new Document([
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
        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection('defaults');

        $this->assertEquals(true, $database->createAttribute('defaults', 'string', Database::VAR_STRING, 128, false, 'default'));
        $this->assertEquals(true, $database->createAttribute('defaults', 'integer', Database::VAR_INTEGER, 0, false, 1));
        $this->assertEquals(true, $database->createAttribute('defaults', 'float', Database::VAR_FLOAT, 0, false, 1.5));
        $this->assertEquals(true, $database->createAttribute('defaults', 'boolean', Database::VAR_BOOLEAN, 0, false, true));
        $this->assertEquals(true, $database->createAttribute('defaults', 'colors', Database::VAR_STRING, 32, false, ['red', 'green', 'blue'], true, true));
        $this->assertEquals(true, $database->createAttribute('defaults', 'datetime', Database::VAR_DATETIME, 0, false, '2000-06-12T14:12:55.000+00:00', true, false, null, [], ['datetime']));

        $document = $database->createDocument('defaults', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $document2 = $database->getDocument('defaults', $document->getId());
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
        $database->deleteCollection('defaults');
    }

    public function testIncreaseDecrease(): Document
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collection = 'increase_decrease';
        $database->createCollection($collection);

        $this->assertEquals(true, $database->createAttribute($collection, 'increase', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $database->createAttribute($collection, 'decrease', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $database->createAttribute($collection, 'increase_text', Database::VAR_STRING, 255, true));
        $this->assertEquals(true, $database->createAttribute($collection, 'increase_float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, $database->createAttribute($collection, 'sizes', Database::VAR_INTEGER, 8, required: false, array: true));

        $document = $database->createDocument($collection, new Document([
            'increase' => 100,
            'decrease' => 100,
            'increase_float' => 100,
            'increase_text' => 'some text',
            'sizes' => [10, 20, 30],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ]
        ]));

        $updatedAt = $document->getUpdatedAt();

        $doc = $database->increaseDocumentAttribute($collection, $document->getId(), 'increase', 1, 101);
        $this->assertEquals(101, $doc->getAttribute('increase'));

        $document = $database->getDocument($collection, $document->getId());
        $this->assertEquals(101, $document->getAttribute('increase'));
        $this->assertNotEquals($updatedAt, $document->getUpdatedAt());

        $doc = $database->decreaseDocumentAttribute($collection, $document->getId(), 'decrease', 1, 98);
        $this->assertEquals(99, $doc->getAttribute('decrease'));
        $document = $database->getDocument($collection, $document->getId());
        $this->assertEquals(99, $document->getAttribute('decrease'));

        $doc = $database->increaseDocumentAttribute($collection, $document->getId(), 'increase_float', 5.5, 110);
        $this->assertEquals(105.5, $doc->getAttribute('increase_float'));
        $document = $database->getDocument($collection, $document->getId());
        $this->assertEquals(105.5, $document->getAttribute('increase_float'));

        $doc = $database->decreaseDocumentAttribute($collection, $document->getId(), 'increase_float', 1.1, 100);
        $this->assertEquals(104.4, $doc->getAttribute('increase_float'));
        $document = $database->getDocument($collection, $document->getId());
        $this->assertEquals(104.4, $document->getAttribute('increase_float'));

        return $document;
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseLimitMax(Document $document): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $this->expectException(Exception::class);
        $this->assertEquals(true, $database->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase', 10.5, 102.4));
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testDecreaseLimitMin(Document $document): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        try {
            $database->decreaseDocumentAttribute(
                'increase_decrease',
                $document->getId(),
                'decrease',
                10,
                99
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        }

        try {
            $database->decreaseDocumentAttribute(
                'increase_decrease',
                $document->getId(),
                'decrease',
                1000,
                0
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
        }
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseTextAttribute(Document $document): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        try {
            $this->assertEquals(false, $database->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase_text'));
            $this->fail('Expected TypeException not thrown');
        } catch (Exception $e) {
            $this->assertInstanceOf(TypeException::class, $e, $e->getMessage());
        }
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseArrayAttribute(Document $document): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        try {
            $this->assertEquals(false, $database->increaseDocumentAttribute('increase_decrease', $document->getId(), 'sizes'));
            $this->fail('Expected TypeException not thrown');
        } catch (Exception $e) {
            $this->assertInstanceOf(TypeException::class, $e);
        }
    }

    /**
      * @depends testCreateDocument
      */
    public function testGetDocument(Document $document): Document
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->getDocument('documents', $document->getId());

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

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed']),
        ]);

        $this->assertFalse($document->isEmpty());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer_signed'));
        $this->assertEquals(-Database::INT_MAX, $document->getAttribute('integer_signed'));
        $this->assertArrayNotHasKey('float', $document->getAttributes());
        $this->assertArrayNotHasKey('boolean', $document->getAttributes());
        $this->assertArrayNotHasKey('colors', $document->getAttributes());
        $this->assertArrayNotHasKey('with-dash', $document->getAttributes());
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayHasKey('$sequence', $document);
        $this->assertArrayHasKey('$createdAt', $document);
        $this->assertArrayHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        $document = $database->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed', '$id']),
        ]);

        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayHasKey('$sequence', $document);
        $this->assertArrayHasKey('$createdAt', $document);
        $this->assertArrayHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);
        $this->assertArrayHasKey('string', $document);
        $this->assertArrayHasKey('integer_signed', $document);
        $this->assertArrayNotHasKey('float', $document);

        return $document;
    }
    /**
     * @return array<string, mixed>
     */
    public function testFind(): array
    {
        Authorization::setRole(Role::any()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection('movies', permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users())
        ]);

        $this->assertEquals(true, $database->createAttribute('movies', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $database->createAttribute('movies', 'director', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $database->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $database->createAttribute('movies', 'price', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, $database->createAttribute('movies', 'active', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $database->createAttribute('movies', 'genres', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, $database->createAttribute('movies', 'with-dash', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $database->createAttribute('movies', 'nullable', Database::VAR_STRING, 128, false));

        try {
            $database->createDocument('movies', new Document(['$id' => ['id_as_array']]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('$id must be of type string', $e->getMessage());
            $this->assertInstanceOf(StructureException::class, $e);
        }

        $document = $database->createDocument('movies', new Document([
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

        $database->createDocument('movies', new Document([
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

        $database->createDocument('movies', new Document([
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

        $database->createDocument('movies', new Document([
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

        $database->createDocument('movies', new Document([
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

        $database->createDocument('movies', new Document([
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
            '$sequence' => $document->getSequence()
        ];
    }

    /**
    * @depends testFind
    */
    public function testFindOne(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->findOne('movies', [
            Query::offset(2),
            Query::orderAsc('name')
        ]);

        $this->assertFalse($document->isEmpty());
        $this->assertEquals('Frozen', $document->getAttribute('name'));

        $document = $database->findOne('movies', [
            Query::offset(10)
        ]);
        $this->assertTrue($document->isEmpty());
    }

    public function testFindBasicChecks(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find('movies');
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
        $documents = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('$id'),
        ]);
        $this->assertEquals($lastDocumentId, $documents[0]->getId());
        $documents = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderAsc('$id'),
        ]);
        $this->assertEquals($firstDocumentId, $documents[0]->getId());

        /**
         * Check internal numeric ID sorting
         */
        $documents = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);
        $this->assertEquals($movieDocuments[\count($movieDocuments) - 1]->getId(), $documents[0]->getId());
        $documents = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderAsc(''),
        ]);
        $this->assertEquals($movieDocuments[0]->getId(), $documents[0]->getId());
    }

    public function testFindCheckPermissions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Check Permissions
         */
        Authorization::setRole('user:x');
        $documents = $database->find('movies');

        $this->assertEquals(6, count($documents));
    }

    public function testFindCheckInteger(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Query with dash attribute
         */
        $documents = $database->find('movies', [
            Query::equal('with-dash', ['Works']),
        ]);

        $this->assertEquals(2, count($documents));

        $documents = $database->find('movies', [
            Query::equal('with-dash', ['Works2', 'Works3']),
        ]);

        $this->assertEquals(4, count($documents));

        /**
         * Check an Integer condition
         */
        $documents = $database->find('movies', [
            Query::equal('year', [2019]),
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
    }

    public function testFindBoolean(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Boolean condition
         */
        $documents = $database->find('movies', [
            Query::equal('active', [true]),
        ]);

        $this->assertEquals(4, count($documents));
    }

    public function testFindStringQueryEqual(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * String condition
         */
        $documents = $database->find('movies', [
            Query::equal('director', ['TBD']),
        ]);

        $this->assertEquals(2, count($documents));

        $documents = $database->find('movies', [
            Query::equal('director', ['']),
        ]);

        $this->assertEquals(0, count($documents));
    }


    public function testFindNotEqual(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Not Equal query
         */
        $documents = $database->find('movies', [
            Query::notEqual('director', 'TBD'),
        ]);

        $this->assertGreaterThan(0, count($documents));

        foreach ($documents as $document) {
            $this->assertTrue($document['director'] !== 'TBD');
        }

        $documents = $database->find('movies', [
            Query::notEqual('director', ''),
        ]);

        $total = $database->count('movies');

        $this->assertEquals($total, count($documents));
    }

    public function testFindBetween(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find('movies', [
            Query::between('price', 25.94, 25.99),
        ]);
        $this->assertEquals(2, count($documents));

        $documents = $database->find('movies', [
            Query::between('price', 30, 35),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = $database->find('movies', [
            Query::between('$createdAt', '1975-12-06', '2050-12-06'),
        ]);
        $this->assertEquals(6, count($documents));

        $documents = $database->find('movies', [
            Query::between('$updatedAt', '1975-12-06T07:08:49.733+02:00', '2050-02-05T10:15:21.825+00:00'),
        ]);
        $this->assertEquals(6, count($documents));
    }

    public function testFindFloat(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Float condition
         */
        $documents = $database->find('movies', [
            Query::lessThan('price', 26.00),
            Query::greaterThan('price', 25.98),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindContains(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForQueryContains()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $documents = $database->find('movies', [
            Query::contains('genres', ['comics'])
        ]);

        $this->assertEquals(2, count($documents));

        /**
         * Array contains OR condition
         */
        $documents = $database->find('movies', [
            Query::contains('genres', ['comics', 'kids']),
        ]);

        $this->assertEquals(4, count($documents));

        $documents = $database->find('movies', [
            Query::contains('genres', ['non-existent']),
        ]);

        $this->assertEquals(0, count($documents));

        try {
            $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Fulltext search
         */
        if ($this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $success = $database->createIndex('movies', 'name', Database::INDEX_FULLTEXT, ['name']);
            $this->assertEquals(true, $success);

            $documents = $database->find('movies', [
                Query::search('name', 'captain'),
            ]);

            $this->assertEquals(2, count($documents));

            /**
             * Fulltext search (wildcard)
             */

            // TODO: Looks like the MongoDB implementation is a bit more complex, skipping that for now.
            // TODO: I think this needs a changes? how do we distinguish between regular full text and wildcard?

            if ($this->getDatabase()->getAdapter()->getSupportForFulltextWildCardIndex()) {
                $documents = $database->find('movies', [
                    Query::search('name', 'cap'),
                ]);

                $this->assertEquals(2, count($documents));
            }
        }

        $this->assertEquals(true, true); // Test must do an assertion
    }
    public function testFindFulltextSpecialChars(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForFulltextIndex()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'full_text';
        $database->createCollection($collection, permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users())
        ]);

        $this->assertTrue($database->createAttribute($collection, 'ft', Database::VAR_STRING, 128, true));
        $this->assertTrue($database->createIndex($collection, 'ft-index', Database::INDEX_FULLTEXT, ['ft']));

        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'Alf: chapter_4@nasa.com'
        ]));

        $documents = $database->find($collection, [
            Query::search('ft', 'chapter_4'),
        ]);
        $this->assertEquals(1, count($documents));

        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'al@ba.io +-*)(<>~'
        ]));

        $documents = $database->find($collection, [
            Query::search('ft', 'al@ba.io'), // === al ba io*
        ]);

        if ($database->getAdapter()->getSupportForFulltextWildcardIndex()) {
            $this->assertEquals(0, count($documents));
        } else {
            $this->assertEquals(1, count($documents));
        }

        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'donald duck'
        ]));

        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'donald trump'
        ]));

        $documents = $database->find($collection, [
            Query::search('ft', 'donald trump'),
            Query::orderAsc('ft'),
        ]);
        $this->assertEquals(2, count($documents));

        $documents = $database->find($collection, [
            Query::search('ft', '"donald trump"'), // Exact match
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindMultipleConditions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Multiple conditions
         */
        $documents = $database->find('movies', [
            Query::equal('director', ['TBD']),
            Query::equal('year', [2026]),
        ]);

        $this->assertEquals(1, count($documents));

        /**
         * Multiple conditions and OR values
         */
        $documents = $database->find('movies', [
            Query::equal('name', ['Frozen II', 'Captain Marvel']),
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals('Frozen II', $documents[0]['name']);
        $this->assertEquals('Captain Marvel', $documents[1]['name']);
    }

    public function testFindByID(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * $id condition
         */
        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Test that internal ID queries are handled correctly
         */
        $documents = $database->find('movies', [
            Query::equal('$sequence', [$data['$sequence']]),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindOrderBy(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY
         */
        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY natural
         */
        $base = array_reverse($database->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]));
        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - Multiple attributes
         */
        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - After
         */
        $movies = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));

        /**
         * Multiple order by, Test tie-break on year 2019
         */
        $movies = $database->find('movies', [
            Query::orderAsc('year'),
            Query::orderAsc('price'),
        ]);

        $this->assertEquals(6, count($movies));

        $this->assertEquals($movies[0]['name'], 'Captain America: The First Avenger');
        $this->assertEquals($movies[0]['year'], 2011);
        $this->assertEquals($movies[0]['price'], 25.94);

        $this->assertEquals($movies[1]['name'], 'Frozen');
        $this->assertEquals($movies[1]['year'], 2013);
        $this->assertEquals($movies[1]['price'], 39.5);

        $this->assertEquals($movies[2]['name'], 'Captain Marvel');
        $this->assertEquals($movies[2]['year'], 2019);
        $this->assertEquals($movies[2]['price'], 25.99);

        $this->assertEquals($movies[3]['name'], 'Frozen II');
        $this->assertEquals($movies[3]['year'], 2019);
        $this->assertEquals($movies[3]['price'], 39.5);

        $this->assertEquals($movies[4]['name'], 'Work in Progress');
        $this->assertEquals($movies[4]['year'], 2025);
        $this->assertEquals($movies[4]['price'], 0);

        $this->assertEquals($movies[5]['name'], 'Work in Progress 2');
        $this->assertEquals($movies[5]['year'], 2026);
        $this->assertEquals($movies[5]['price'], 0);

        $pos = 2;
        $documents = $database->find('movies', [
            Query::orderAsc('year'),
            Query::orderAsc('price'),
            Query::cursorAfter($movies[$pos])
        ]);

        $this->assertEquals(3, count($documents));

        foreach ($documents as $i => $document) {
            $this->assertEquals($document['name'], $movies[$i + 1 + $pos]['name']);
            $this->assertEquals($document['price'], $movies[$i + 1 + $pos]['price']);
            $this->assertEquals($document['year'], $movies[$i + 1 + $pos]['year']);
        }
    }


    public function testFindOrderByCursorBefore(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - Before
         */
        $movies = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByAfterNaturalOrder(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - After by natural order
         */
        $movies = array_reverse($database->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]));

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }
    public function testFindOrderByBeforeNaturalOrder(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - Before by natural order
         */
        $movies = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderBySingleAttributeAfter(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - Single Attribute After
         */
        $movies = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('year')
        ]);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[1])
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[5])
        ]);
        $this->assertEmpty(count($documents));
    }


    public function testFindOrderBySingleAttributeBefore(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - Single Attribute Before
         */
        $movies = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('year')
        ]);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[0])
        ]);
        $this->assertEmpty(count($documents));
    }

    public function testFindOrderByMultipleAttributeAfter(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - Multiple Attribute After
         */
        $movies = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year')
        ]);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY - Multiple Attribute Before
         */
        $movies = $database->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year')
        ]);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[5])
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[4])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY + CURSOR
         */
        $documentsTest = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
        ]);
        $documents = $database->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }
    public function testFindOrderByIdAndCursor(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY ID + CURSOR
         */
        $documentsTest = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$id'),
        ]);
        $documents = $database->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$id'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindOrderByCreateDateAndCursor(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY CREATE DATE + CURSOR
         */
        $documentsTest = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$createdAt'),
        ]);

        $documents = $database->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$createdAt'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindOrderByUpdateDateAndCursor(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * ORDER BY UPDATE DATE + CURSOR
         */
        $documentsTest = $database->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$updatedAt'),
        ]);
        $documents = $database->find('movies', [
            Query::limit(1),
            Query::offset(0),
            Query::orderDesc('$updatedAt'),
            Query::cursorAfter($documentsTest[0])
        ]);

        $this->assertEquals($documentsTest[1]['$id'], $documents[0]['$id']);
    }

    public function testFindCreatedBefore(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Test Query::createdBefore wrapper
         */
        $futureDate = '2050-01-01T00:00:00.000Z';
        $pastDate = '1900-01-01T00:00:00.000Z';

        $documents = $database->find('movies', [
            Query::createdBefore($futureDate),
            Query::limit(1)
        ]);

        $this->assertGreaterThan(0, count($documents));

        $documents = $database->find('movies', [
            Query::createdBefore($pastDate),
            Query::limit(1)
        ]);

        $this->assertEquals(0, count($documents));
    }

    public function testFindCreatedAfter(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Test Query::createdAfter wrapper
         */
        $futureDate = '2050-01-01T00:00:00.000Z';
        $pastDate = '1900-01-01T00:00:00.000Z';

        $documents = $database->find('movies', [
            Query::createdAfter($pastDate),
            Query::limit(1)
        ]);

        $this->assertGreaterThan(0, count($documents));

        $documents = $database->find('movies', [
            Query::createdAfter($futureDate),
            Query::limit(1)
        ]);

        $this->assertEquals(0, count($documents));
    }

    public function testFindUpdatedBefore(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Test Query::updatedBefore wrapper
         */
        $futureDate = '2050-01-01T00:00:00.000Z';
        $pastDate = '1900-01-01T00:00:00.000Z';

        $documents = $database->find('movies', [
            Query::updatedBefore($futureDate),
            Query::limit(1)
        ]);

        $this->assertGreaterThan(0, count($documents));

        $documents = $database->find('movies', [
            Query::updatedBefore($pastDate),
            Query::limit(1)
        ]);

        $this->assertEquals(0, count($documents));
    }

    public function testFindUpdatedAfter(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Test Query::updatedAfter wrapper
         */
        $futureDate = '2050-01-01T00:00:00.000Z';
        $pastDate = '1900-01-01T00:00:00.000Z';

        $documents = $database->find('movies', [
            Query::updatedAfter($pastDate),
            Query::limit(1)
        ]);

        $this->assertGreaterThan(0, count($documents));

        $documents = $database->find('movies', [
            Query::updatedAfter($futureDate),
            Query::limit(1)
        ]);

        $this->assertEquals(0, count($documents));
    }

    public function testFindLimit(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Limit
         */
        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Limit + Offset
         */
        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Test that OR queries are handled correctly
         */
        $documents = $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        $collection = 'edgeCases';

        $database->createCollection($collection);

        $this->assertEquals(true, $database->createAttribute($collection, 'value', Database::VAR_STRING, 256, true));

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
            $database->createDocument($collection, new Document([
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
        $documents = $database->find($collection);

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
            $documents = $database->find($collection, [
                Query::limit(25),
                Query::equal('value', [$value])
            ]);

            $this->assertEquals(1, count($documents));
            $this->assertEquals($value, $documents[0]->getAttribute('value'));
        }
    }

    public function testOrSingleQuery(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        try {
            $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        $queries = [
            Query::or([
                Query::equal('active', [true]),
                Query::equal('name', ['Frozen II'])
            ])
        ];
        $this->assertCount(4, $database->find('movies', $queries));
        $this->assertEquals(4, $database->count('movies', $queries));

        $queries = [
            Query::equal('active', [true]),
            Query::or([
                Query::equal('name', ['Frozen']),
                Query::equal('name', ['Frozen II']),
                Query::equal('director', ['Joe Johnston'])
            ])
        ];

        $this->assertCount(3, $database->find('movies', $queries));
        $this->assertEquals(3, $database->count('movies', $queries));
    }

    public function testOrNested(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $queries = [
            Query::select(['director']),
            Query::equal('director', ['Joe Johnston']),
            Query::or([
                Query::equal('name', ['Frozen']),
                Query::or([
                    Query::equal('active', [true]),
                    Query::equal('active', [false]),
                ])
            ])
        ];

        $documents = $database->find('movies', $queries);
        $this->assertCount(1, $documents);
        $this->assertArrayNotHasKey('name', $documents[0]);

        $count = $database->count('movies', $queries);
        $this->assertEquals(1, $count);
    }

    public function testAndSingleQuery(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        try {
            $database->find('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        $queries = [
            Query::and([
                Query::equal('active', [true]),
                Query::equal('name', ['Frozen II'])
            ])
        ];
        $this->assertCount(1, $database->find('movies', $queries));
        $this->assertEquals(1, $database->count('movies', $queries));
    }

    public function testAndNested(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $queries = [
            Query::or([
                Query::equal('active', [false]),
                Query::and([
                    Query::equal('active', [true]),
                    Query::equal('name', ['Frozen']),
                ])
            ])
        ];

        $documents = $database->find('movies', $queries);
        $this->assertCount(3, $documents);

        $count = $database->count('movies', $queries);
        $this->assertEquals(3, $count);
    }

    public function testNestedIDQueries(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        Authorization::setRole(Role::any()->toString());

        $database->createCollection('movies_nested_id', permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users())
        ]);

        $this->assertEquals(true, $database->createAttribute('movies_nested_id', 'name', Database::VAR_STRING, 128, true));

        $database->createDocument('movies_nested_id', new Document([
            '$id' => ID::custom('1'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => '1',
        ]));

        $database->createDocument('movies_nested_id', new Document([
            '$id' => ID::custom('2'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => '2',
        ]));

        $database->createDocument('movies_nested_id', new Document([
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

        $documents = $database->find('movies_nested_id', $queries);
        $this->assertCount(2, $documents);

        // Make sure the query was not modified by reference
        $this->assertEquals($queries[0]->getValues()[0]->getAttribute(), '$id');

        $count = $database->count('movies_nested_id', $queries);
        $this->assertEquals(2, $count);
    }

    public function testFindNull(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find('movies', [
            Query::isNull('nullable'),
        ]);

        $this->assertEquals(5, count($documents));
    }

    public function testFindNotNull(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find('movies', [
            Query::isNotNull('nullable'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindStartsWith(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find('movies', [
            Query::startsWith('name', 'Work'),
        ]);

        $this->assertEquals(2, count($documents));

        if ($this->getDatabase()->getAdapter() instanceof SQL) {
            $documents = $database->find('movies', [
                Query::startsWith('name', '%ork'),
            ]);
        } else {
            $documents = $database->find('movies', [
                Query::startsWith('name', '.*ork'),
            ]);
        }

        $this->assertEquals(0, count($documents));
    }

    public function testFindStartsWithWords(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find('movies', [
            Query::startsWith('name', 'Work in Progress'),
        ]);

        $this->assertEquals(2, count($documents));
    }

    public function testFindEndsWith(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find('movies', [
            Query::endsWith('name', 'Marvel'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindNotContains(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForQueryContains()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Test notContains with array attributes - should return documents that don't contain specified genres
        $documents = $database->find('movies', [
            Query::notContains('genres', ['comics'])
        ]);

        $this->assertEquals(4, count($documents)); // All movies except the 2 with 'comics' genre

        // Test notContains with multiple values (AND logic - exclude documents containing ANY of these)
        $documents = $database->find('movies', [
            Query::notContains('genres', ['comics', 'kids']),
        ]);

        $this->assertEquals(2, count($documents)); // Movies that have neither 'comics' nor 'kids'

        // Test notContains with non-existent genre - should return all documents
        $documents = $database->find('movies', [
            Query::notContains('genres', ['non-existent']),
        ]);

        $this->assertEquals(6, count($documents));

        // Test notContains with string attribute (substring search)
        $documents = $database->find('movies', [
            Query::notContains('name', ['Captain'])
        ]);
        $this->assertEquals(4, count($documents)); // All movies except those containing 'Captain'

        // Test notContains combined with other queries (AND logic)
        $documents = $database->find('movies', [
            Query::notContains('genres', ['comics']),
            Query::greaterThan('year', 2000)
        ]);
        $this->assertLessThanOrEqual(4, count($documents)); // Subset of movies without 'comics' and after 2000

        // Test notContains with case sensitivity
        $documents = $database->find('movies', [
            Query::notContains('genres', ['COMICS']) // Different case
        ]);
        $this->assertEquals(6, count($documents)); // All movies since case doesn't match

        // Test error handling for invalid attribute type
        try {
            $database->find('movies', [
                Query::notContains('price', [10.5]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid query: Cannot query notContains on attribute "price" because it is not an array or string.', $e->getMessage());
            $this->assertTrue($e instanceof DatabaseException);
        }
    }

    public function testFindNotSearch(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Only test if fulltext search is supported
        if ($this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            // Ensure fulltext index exists (may already exist from previous tests)
            try {
                $database->createIndex('movies', 'name', Database::INDEX_FULLTEXT, ['name']);
            } catch (Throwable $e) {
                // Index may already exist, ignore duplicate error
                if (!str_contains($e->getMessage(), 'already exists')) {
                    throw $e;
                }
            }

            // Test notSearch - should return documents that don't match the search term
            $documents = $database->find('movies', [
                Query::notSearch('name', 'captain'),
            ]);

            $this->assertEquals(4, count($documents)); // All movies except the 2 with 'captain' in name

            // Test notSearch with term that doesn't exist - should return all documents
            $documents = $database->find('movies', [
                Query::notSearch('name', 'nonexistent'),
            ]);

            $this->assertEquals(6, count($documents));

            // Test notSearch with partial term
            if ($this->getDatabase()->getAdapter()->getSupportForFulltextWildCardIndex()) {
                $documents = $database->find('movies', [
                    Query::notSearch('name', 'cap'),
                ]);

                $this->assertEquals(4, count($documents)); // All movies except those matching 'cap'
            }

            // Test notSearch with empty string - should return all documents
            $documents = $database->find('movies', [
                Query::notSearch('name', ''),
            ]);
            $this->assertEquals(6, count($documents)); // All movies since empty search matches nothing

            // Test notSearch combined with other filters
            $documents = $database->find('movies', [
                Query::notSearch('name', 'captain'),
                Query::lessThan('year', 2010)
            ]);
            $this->assertLessThanOrEqual(4, count($documents)); // Subset of non-captain movies before 2010

            // Test notSearch with special characters
            $documents = $database->find('movies', [
                Query::notSearch('name', '@#$%'),
            ]);
            $this->assertEquals(6, count($documents)); // All movies since special chars don't match
        }

        $this->assertEquals(true, true); // Test must do an assertion
    }

    public function testFindNotStartsWith(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Test notStartsWith - should return documents that don't start with 'Work'
        $documents = $database->find('movies', [
            Query::notStartsWith('name', 'Work'),
        ]);

        $this->assertEquals(4, count($documents)); // All movies except the 2 starting with 'Work'

        // Test notStartsWith with non-existent prefix - should return all documents
        $documents = $database->find('movies', [
            Query::notStartsWith('name', 'NonExistent'),
        ]);

        $this->assertEquals(6, count($documents));

        // Test notStartsWith with wildcard characters (should treat them literally)
        if ($this->getDatabase()->getAdapter() instanceof SQL) {
            $documents = $database->find('movies', [
                Query::notStartsWith('name', '%ork'),
            ]);
        } else {
            $documents = $database->find('movies', [
                Query::notStartsWith('name', '.*ork'),
            ]);
        }

        $this->assertEquals(6, count($documents)); // Should return all since no movie starts with these patterns

        // Test notStartsWith with empty string - should return no documents (all strings start with empty)
        $documents = $database->find('movies', [
            Query::notStartsWith('name', ''),
        ]);
        $this->assertEquals(0, count($documents)); // No documents since all strings start with empty string

        // Test notStartsWith with single character
        $documents = $database->find('movies', [
            Query::notStartsWith('name', 'C'),
        ]);
        $this->assertGreaterThanOrEqual(4, count($documents)); // Movies not starting with 'C'

        // Test notStartsWith with case sensitivity (may be case-insensitive depending on DB)
        $documents = $database->find('movies', [
            Query::notStartsWith('name', 'work'), // lowercase vs 'Work'
        ]);
        $this->assertGreaterThanOrEqual(4, count($documents)); // May match case-insensitively

        // Test notStartsWith combined with other queries
        $documents = $database->find('movies', [
            Query::notStartsWith('name', 'Work'),
            Query::equal('year', [2006])
        ]);
        $this->assertLessThanOrEqual(4, count($documents)); // Subset of non-Work movies from 2006
    }

    public function testFindNotEndsWith(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Test notEndsWith - should return documents that don't end with 'Marvel'
        $documents = $database->find('movies', [
            Query::notEndsWith('name', 'Marvel'),
        ]);

        $this->assertEquals(5, count($documents)); // All movies except the 1 ending with 'Marvel'

        // Test notEndsWith with non-existent suffix - should return all documents
        $documents = $database->find('movies', [
            Query::notEndsWith('name', 'NonExistent'),
        ]);

        $this->assertEquals(6, count($documents));

        // Test notEndsWith with partial suffix
        $documents = $database->find('movies', [
            Query::notEndsWith('name', 'vel'),
        ]);

        $this->assertEquals(5, count($documents)); // All movies except the 1 ending with 'vel' (from 'Marvel')

        // Test notEndsWith with empty string - should return no documents (all strings end with empty)
        $documents = $database->find('movies', [
            Query::notEndsWith('name', ''),
        ]);
        $this->assertEquals(0, count($documents)); // No documents since all strings end with empty string

        // Test notEndsWith with single character
        $documents = $database->find('movies', [
            Query::notEndsWith('name', 'l'),
        ]);
        $this->assertGreaterThanOrEqual(5, count($documents)); // Movies not ending with 'l'

        // Test notEndsWith with case sensitivity (may be case-insensitive depending on DB)
        $documents = $database->find('movies', [
            Query::notEndsWith('name', 'marvel'), // lowercase vs 'Marvel'
        ]);
        $this->assertGreaterThanOrEqual(5, count($documents)); // May match case-insensitively

        // Test notEndsWith combined with limit
        $documents = $database->find('movies', [
            Query::notEndsWith('name', 'Marvel'),
            Query::limit(3)
        ]);
        $this->assertEquals(3, count($documents)); // Limited to 3 results
        $this->assertLessThanOrEqual(5, count($documents)); // But still excluding Marvel movies
    }

    public function testFindNotBetween(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Test notBetween with price range - should return documents outside the range
        $documents = $database->find('movies', [
            Query::notBetween('price', 25.94, 25.99),
        ]);
        $this->assertEquals(4, count($documents)); // All movies except the 2 in the price range

        // Test notBetween with range that includes no documents - should return all documents
        $documents = $database->find('movies', [
            Query::notBetween('price', 30, 35),
        ]);
        $this->assertEquals(6, count($documents));

        // Test notBetween with date range
        $documents = $database->find('movies', [
            Query::notBetween('$createdAt', '1975-12-06', '2050-12-06'),
        ]);
        $this->assertEquals(0, count($documents)); // No movies outside this wide date range

        // Test notBetween with narrower date range
        $documents = $database->find('movies', [
            Query::notBetween('$createdAt', '2000-01-01', '2001-01-01'),
        ]);
        $this->assertEquals(6, count($documents)); // All movies should be outside this narrow range

        // Test notBetween with updated date range
        $documents = $database->find('movies', [
            Query::notBetween('$updatedAt', '2000-01-01T00:00:00.000+00:00', '2001-01-01T00:00:00.000+00:00'),
        ]);
        $this->assertEquals(6, count($documents)); // All movies should be outside this narrow range

        // Test notBetween with year range (integer values)
        $documents = $database->find('movies', [
            Query::notBetween('year', 2005, 2007),
        ]);
        $this->assertLessThanOrEqual(6, count($documents)); // Movies outside 2005-2007 range

        // Test notBetween with reversed range (start > end) - should still work
        $documents = $database->find('movies', [
            Query::notBetween('price', 25.99, 25.94), // Note: reversed order
        ]);
        $this->assertGreaterThanOrEqual(4, count($documents)); // Should handle reversed range gracefully

        // Test notBetween with same start and end values
        $documents = $database->find('movies', [
            Query::notBetween('year', 2006, 2006),
        ]);
        $this->assertGreaterThanOrEqual(5, count($documents)); // All movies except those from exactly 2006

        // Test notBetween combined with other filters
        $documents = $database->find('movies', [
            Query::notBetween('price', 25.94, 25.99),
            Query::orderDesc('year'),
            Query::limit(2)
        ]);
        $this->assertEquals(2, count($documents)); // Limited results, ordered, excluding price range

        // Test notBetween with extreme ranges
        $documents = $database->find('movies', [
            Query::notBetween('year', -1000, 1000), // Very wide range
        ]);
        $this->assertLessThanOrEqual(6, count($documents)); // Movies outside this range

        // Test notBetween with float precision
        $documents = $database->find('movies', [
            Query::notBetween('price', 25.945, 25.955), // Very narrow range
        ]);
        $this->assertGreaterThanOrEqual(4, count($documents)); // Most movies should be outside this narrow range
    }

    public function testFindSelect(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find('movies', [
            Query::select(['name', 'year'])
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayHasKey('$sequence', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);
        }

        $documents = $database->find('movies', [
            Query::select(['name', 'year', '$id'])
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayHasKey('$sequence', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);
        }

        $documents = $database->find('movies', [
            Query::select(['name', 'year', '$sequence'])
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayHasKey('$sequence', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);
        }

        $documents = $database->find('movies', [
            Query::select(['name', 'year', '$collection'])
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayHasKey('$sequence', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);
        }

        $documents = $database->find('movies', [
            Query::select(['name', 'year', '$createdAt'])
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayHasKey('$sequence', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);
        }

        $documents = $database->find('movies', [
            Query::select(['name', 'year', '$updatedAt'])
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayHasKey('$sequence', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);
        }

        $documents = $database->find('movies', [
            Query::select(['name', 'year', '$permissions'])
        ]);

        foreach ($documents as $document) {
            $this->assertArrayHasKey('name', $document);
            $this->assertArrayHasKey('year', $document);
            $this->assertArrayNotHasKey('director', $document);
            $this->assertArrayNotHasKey('price', $document);
            $this->assertArrayNotHasKey('active', $document);
            $this->assertArrayHasKey('$id', $document);
            $this->assertArrayHasKey('$sequence', $document);
            $this->assertArrayHasKey('$collection', $document);
            $this->assertArrayHasKey('$createdAt', $document);
            $this->assertArrayHasKey('$updatedAt', $document);
            $this->assertArrayHasKey('$permissions', $document);
        }
    }

    /** @depends testFind */
    public function testForeach(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        /**
         * Test, foreach goes through all the documents
         */
        $documents = [];
        $database->foreach('movies', queries: [Query::limit(2)], callback: function ($document) use (&$documents) {
            $documents[] = $document;
        });
        $this->assertEquals(6, count($documents));

        /**
         * Test, foreach with initial cursor
         */

        $first = $documents[0];
        $documents = [];
        $database->foreach('movies', queries: [Query::limit(2), Query::cursorAfter($first)], callback: function ($document) use (&$documents) {
            $documents[] = $document;
        });
        $this->assertEquals(5, count($documents));

        /**
         * Test, foreach with initial offset
         */

        $documents = [];
        $database->foreach('movies', queries: [Query::limit(2), Query::offset(2)], callback: function ($document) use (&$documents) {
            $documents[] = $document;
        });
        $this->assertEquals(4, count($documents));

        /**
         * Test, cursor before throws error
         */
        try {
            $database->foreach('movies', queries: [Query::cursorBefore($documents[0]), Query::offset(2)], callback: function ($document) use (&$documents) {
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
        /** @var Database $database */
        $database = static::getDatabase();

        $count = $database->count('movies');
        $this->assertEquals(6, $count);
        $count = $database->count('movies', [Query::equal('year', [2019])]);

        $this->assertEquals(2, $count);
        $count = $database->count('movies', [Query::equal('with-dash', ['Works'])]);
        $this->assertEquals(2, $count);
        $count = $database->count('movies', [Query::equal('with-dash', ['Works2', 'Works3'])]);
        $this->assertEquals(4, $count);

        Authorization::unsetRole('user:x');
        $count = $database->count('movies');
        $this->assertEquals(5, $count);

        Authorization::disable();
        $count = $database->count('movies');
        $this->assertEquals(6, $count);
        Authorization::reset();

        Authorization::disable();
        $count = $database->count('movies', [], 3);
        $this->assertEquals(3, $count);
        Authorization::reset();

        /**
         * Test that OR queries are handled correctly
         */
        Authorization::disable();
        $count = $database->count('movies', [
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
        /** @var Database $database */
        $database = static::getDatabase();

        Authorization::setRole('user:x');

        $sum = $database->sum('movies', 'year', [Query::equal('year', [2019]),]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = $database->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025 + 2026, $sum);
        $sum = $database->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = $database->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));

        $sum = $database->sum('movies', 'year', [Query::equal('year', [2019])], 1);
        $this->assertEquals(2019, $sum);

        Authorization::unsetRole('user:x');
        Authorization::unsetRole('userx');
        $sum = $database->sum('movies', 'year', [Query::equal('year', [2019]),]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = $database->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025, $sum);
        $sum = $database->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = $database->sum('movies', 'price', [Query::equal('year', [2019]),]);
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

        /** @var Database $database */
        $database = static::getDatabase();

        $result = $database->encode($collection, $document);

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

        $result = $database->decode($collection, $document);

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
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'testUpdateDocuments';
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $database->createCollection($collection, attributes: [
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
            $database->createDocument($collection, new Document([
                '$id' => 'doc' . $i,
                'string' => 'text📝 ' . $i,
                'integer' => $i
            ]));
        }

        // Test Update half of the documents
        $results = [];
        $count = $database->updateDocuments($collection, new Document([
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

        $updatedDocuments = $database->find($collection, [
            Query::greaterThanEqual('integer', 5),
        ]);

        $this->assertCount(5, $updatedDocuments);

        foreach ($updatedDocuments as $document) {
            $this->assertEquals('text📝 updated', $document->getAttribute('string'));
            $this->assertGreaterThanOrEqual(5, $document->getAttribute('integer'));
        }

        $controlDocuments = $database->find($collection, [
            Query::lessThan('integer', 5),
        ]);

        $this->assertEquals(count($controlDocuments), 5);

        foreach ($controlDocuments as $document) {
            $this->assertNotEquals('text📝 updated', $document->getAttribute('string'));
        }

        // Test Update all documents
        $this->assertEquals(10, $database->updateDocuments($collection, new Document([
            'string' => 'text📝 updated all',
        ])));

        $updatedDocuments = $database->find($collection);

        $this->assertEquals(count($updatedDocuments), 10);

        foreach ($updatedDocuments as $document) {
            $this->assertEquals('text📝 updated all', $document->getAttribute('string'));
        }

        // TEST: Can't delete documents in the past
        $oneHourAgo = (new \DateTime())->sub(new \DateInterval('PT1H'));

        try {
            $this->getDatabase()->withRequestTimestamp($oneHourAgo, function () use ($collection, $database) {
                $database->updateDocuments($collection, new Document([
                    'string' => 'text📝 updated all',
                ]));
            });
            $this->fail('Failed to throw exception');
        } catch (ConflictException $e) {
            $this->assertEquals('Document was updated after the request timestamp', $e->getMessage());
        }

        // Check collection level permissions
        $database->updateCollection($collection, permissions: [
            Permission::read(Role::user('asd')),
            Permission::create(Role::user('asd')),
            Permission::update(Role::user('asd')),
            Permission::delete(Role::user('asd')),
        ], documentSecurity: false);

        try {
            $database->updateDocuments($collection, new Document([
                'string' => 'text📝 updated all',
            ]));
            $this->fail('Failed to throw exception');
        } catch (AuthorizationException $e) {
            $this->assertStringStartsWith('Missing "update" permission for role "user:asd".', $e->getMessage());
        }

        // Check document level permissions
        $database->updateCollection($collection, permissions: [], documentSecurity: true);

        Authorization::skip(function () use ($collection, $database) {
            $database->updateDocument($collection, 'doc0', new Document([
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

        $database->updateDocuments($collection, new Document([
            'string' => 'permission text',
        ]));

        $documents = $database->find($collection, [
            Query::equal('string', ['permission text']),
        ]);

        $this->assertCount(1, $documents);

        Authorization::skip(function () use ($collection, $database) {
            $unmodifiedDocuments = $database->find($collection, [
                Query::equal('string', ['text📝 updated all']),
            ]);

            $this->assertCount(9, $unmodifiedDocuments);
        });

        Authorization::skip(function () use ($collection, $database) {
            $database->updateDocuments($collection, new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]));
        });

        // Test we can update more documents than batchSize
        $this->assertEquals(10, $database->updateDocuments($collection, new Document([
            'string' => 'batchSize Test'
        ]), batchSize: 2));

        $documents = $database->find($collection);

        foreach ($documents as $document) {
            $this->assertEquals('batchSize Test', $document->getAttribute('string'));
        }

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());
    }

    public function testUpdateDocumentsWithCallbackSupport(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'update_callback';
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $database->createCollection($collection, attributes: [
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
            $database->createDocument($collection, new Document([
                '$id' => 'doc' . $i,
                'string' => 'text📝 ' . $i,
                'integer' => $i
            ]));
        }
        // Test onNext is throwing the error without the onError
        // a non existent document to test the error thrown
        try {
            $results = [];
            $count = $database->updateDocuments($collection, new Document([
                'string' => 'text📝 updated',
            ]), [
                Query::greaterThanEqual('integer', 100),
            ], onNext: function ($doc) use (&$results) {
                $results[] = $doc;
                throw new Exception("Error thrown to test that update doesn't stop and error is caught");
            });
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals("Error thrown to test that update doesn't stop and error is caught", $e->getMessage());
        }

        // Test Update half of the documents
        $results = [];
        $count = $database->updateDocuments($collection, new Document([
            'string' => 'text📝 updated',
        ]), [
            Query::greaterThanEqual('integer', 5),
        ], onNext: function ($doc) use (&$results) {
            $results[] = $doc;
            throw new Exception("Error thrown to test that update doesn't stop and error is caught");
        }, onError:function ($e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals("Error thrown to test that update doesn't stop and error is caught", $e->getMessage());
        });

        $this->assertEquals(5, $count);

        foreach ($results as $document) {
            $this->assertEquals('text📝 updated', $document->getAttribute('string'));
        }

        $updatedDocuments = $database->find($collection, [
            Query::greaterThanEqual('integer', 5),
        ]);

        $this->assertCount(5, $updatedDocuments);
    }

    /**
     * @depends testCreateDocument
     */
    public function testReadPermissionsSuccess(Document $document): Document
    {
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->createDocument('documents', new Document([
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

        $document = $database->getDocument($document->getCollection(), $document->getId());
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

        /** @var Database $database */
        $database = static::getDatabase();

        $this->expectException(AuthorizationException::class);
        $database->createDocument('documents', new Document([
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

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->createDocument('documents', new Document([
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

        $document = $database->updateDocument('documents', $document->getId(), new Document([
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

        /** @var Database $database */
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createIndex('movies', 'uniqueIndex', Database::INDEX_UNIQUE, ['name'], [128], [Database::ORDER_ASC]));

        $database->createDocument('movies', new Document([
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
        /** @var Database $database */
        $database = static::getDatabase();

        Authorization::setRole(Role::users()->toString());
        // create document then update to conflict with index
        $document = $database->createDocument('movies', new Document([
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

        $database->updateDocument('movies', $document->getId(), $document->setAttribute('name', 'Frozen'));
    }

    public function propagateBulkDocuments(string $collection, int $amount = 10, bool $documentSecurity = false): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        for ($i = 0; $i < $amount; $i++) {
            $database->createDocument($collection, new Document(
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
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection(
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

        $docs = $database->find('bulk_delete');
        $this->assertCount(10, $docs);

        /**
         * Test Short select query, test pagination as well, Add order to select
         */
        $selects = ['$sequence', '$id', '$collection', '$permissions', '$updatedAt'];

        $count = $database->deleteDocuments(
            collection: 'bulk_delete',
            queries: [
                Query::select([...$selects, '$createdAt']),
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
        $this->assertEquals(8, $database->deleteDocuments('bulk_delete'));

        $docs = $database->find('bulk_delete');
        $this->assertCount(0, $docs);

        // TEST: Bulk delete documents with queries.
        $this->propagateBulkDocuments('bulk_delete');

        $results = [];
        $count = $database->deleteDocuments('bulk_delete', [
            Query::greaterThanEqual('integer', 5)
        ], onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(5, $count);

        foreach ($results as $document) {
            $this->assertGreaterThanOrEqual(5, $document->getAttribute('integer'));
        }

        $docs = $database->find('bulk_delete');
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
        $database->updateCollection('bulk_delete', [], false);
        try {
            $database->deleteDocuments('bulk_delete');
            $this->fail('Bulk deleted documents with invalid collection permission');
        } catch (\Utopia\Database\Exception\Authorization) {
        }

        $database->updateCollection('bulk_delete', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::delete(Role::any())
        ], false);

        $this->assertEquals(5, $database->deleteDocuments('bulk_delete'));
        $this->assertEquals(0, \count($this->getDatabase()->find('bulk_delete')));

        // TEST: Make sure we can't delete documents we don't have permissions for
        $database->updateCollection('bulk_delete', [
            Permission::create(Role::any()),
        ], true);
        $this->propagateBulkDocuments('bulk_delete', documentSecurity: true);

        $this->assertEquals(0, $database->deleteDocuments('bulk_delete'));

        $documents = Authorization::skip(function () use ($database) {
            return $database->find('bulk_delete');
        });

        $this->assertEquals(10, \count($documents));

        $database->updateCollection('bulk_delete', [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::delete(Role::any())
        ], false);

        $database->deleteDocuments('bulk_delete');

        $this->assertEquals(0, \count($this->getDatabase()->find('bulk_delete')));

        // Teardown
        $database->deleteCollection('bulk_delete');
    }

    public function testDeleteBulkDocumentsQueries(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection(
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

        $this->assertEquals(5, $database->deleteDocuments('bulk_delete_queries', [Query::limit(5)]));
        $this->assertEquals(5, \count($database->find('bulk_delete_queries')));

        $this->assertEquals(5, $database->deleteDocuments('bulk_delete_queries', [Query::limit(5)]));
        $this->assertEquals(0, \count($database->find('bulk_delete_queries')));

        // Test Limit more than batchSize
        $this->propagateBulkDocuments('bulk_delete_queries', Database::DELETE_BATCH_SIZE * 2);
        $this->assertEquals(Database::DELETE_BATCH_SIZE * 2, \count($database->find('bulk_delete_queries', [Query::limit(Database::DELETE_BATCH_SIZE * 2)])));
        $this->assertEquals(Database::DELETE_BATCH_SIZE + 2, $database->deleteDocuments('bulk_delete_queries', [Query::limit(Database::DELETE_BATCH_SIZE + 2)]));
        $this->assertEquals(Database::DELETE_BATCH_SIZE - 2, \count($database->find('bulk_delete_queries', [Query::limit(Database::DELETE_BATCH_SIZE * 2)])));
        $this->assertEquals(Database::DELETE_BATCH_SIZE - 2, $this->getDatabase()->deleteDocuments('bulk_delete_queries'));

        // Test Offset
        $this->propagateBulkDocuments('bulk_delete_queries', 100);
        $this->assertEquals(50, $database->deleteDocuments('bulk_delete_queries', [Query::offset(50)]));

        $docs = $database->find('bulk_delete_queries', [Query::limit(100)]);
        $this->assertEquals(50, \count($docs));

        $lastDoc = \end($docs);
        $this->assertNotEmpty($lastDoc);
        $this->assertEquals('doc49', $lastDoc->getId());
        $this->assertEquals(50, $database->deleteDocuments('bulk_delete_queries'));

        $database->deleteCollection('bulk_delete_queries');
    }

    public function testDeleteBulkDocumentsWithCallbackSupport(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection(
            'bulk_delete_with_callback',
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

        $this->propagateBulkDocuments('bulk_delete_with_callback');

        $docs = $database->find('bulk_delete_with_callback');
        $this->assertCount(10, $docs);

        /**
         * Test Short select query, test pagination as well, Add order to select
         */
        $selects = ['$sequence', '$id', '$collection', '$permissions', '$updatedAt'];

        try {
            // a non existent document to test the error thrown
            $database->deleteDocuments(
                collection: 'bulk_delete_with_callback',
                queries: [
                    Query::select([...$selects, '$createdAt']),
                    Query::lessThan('$createdAt', '1800-01-01'),
                    Query::orderAsc('$createdAt'),
                    Query::orderAsc(),
                    Query::limit(1),
                ],
                batchSize: 1,
                onNext: function () {
                    throw new Exception("Error thrown to test that deletion doesn't stop and error is caught");
                }
            );
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals("Error thrown to test that deletion doesn't stop and error is caught", $e->getMessage());
        }

        $docs = $database->find('bulk_delete_with_callback');
        $this->assertCount(10, $docs);

        $count = $database->deleteDocuments(
            collection: 'bulk_delete_with_callback',
            queries: [
                Query::select([...$selects, '$createdAt']),
                Query::cursorAfter($docs[6]),
                Query::greaterThan('$createdAt', '2000-01-01'),
                Query::orderAsc('$createdAt'),
                Query::orderAsc(),
                Query::limit(2),
            ],
            batchSize: 1,
            onNext: function () {
                // simulating error throwing but should not stop deletion
                throw new Exception("Error thrown to test that deletion doesn't stop and error is caught");
            },
            onError:function ($e) {
                $this->assertInstanceOf(Exception::class, $e);
                $this->assertEquals("Error thrown to test that deletion doesn't stop and error is caught", $e->getMessage());
            }
        );

        $this->assertEquals(2, $count);

        // TEST: Bulk Delete All Documents without passing callbacks
        $this->assertEquals(8, $database->deleteDocuments('bulk_delete_with_callback'));

        $docs = $database->find('bulk_delete_with_callback');
        $this->assertCount(0, $docs);

        // TEST: Bulk delete documents with queries with callbacks
        $this->propagateBulkDocuments('bulk_delete_with_callback');

        $results = [];
        $count = $database->deleteDocuments('bulk_delete_with_callback', [
            Query::greaterThanEqual('integer', 5)
        ], onNext: function ($doc) use (&$results) {
            $results[] = $doc;
            throw new Exception("Error thrown to test that deletion doesn't stop and error is caught");
        }, onError:function ($e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals("Error thrown to test that deletion doesn't stop and error is caught", $e->getMessage());
        });

        $this->assertEquals(5, $count);

        foreach ($results as $document) {
            $this->assertGreaterThanOrEqual(5, $document->getAttribute('integer'));
        }

        $docs = $database->find('bulk_delete_with_callback');
        $this->assertEquals(5, \count($docs));

        // Teardown
        $database->deleteCollection('bulk_delete_with_callback');
    }

    public function testUpdateDocumentsQueries(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'testUpdateDocumentsQueries';

        $database->createCollection($collection, attributes: [
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

        $this->assertEquals(10, $database->updateDocuments($collection, new Document([
            'text' => 'text📝 updated',
        ]), [Query::limit(10)]));

        $this->assertEquals(10, \count($database->find($collection, [Query::equal('text', ['text📝 updated'])])));
        $this->assertEquals(100, $database->deleteDocuments($collection));
        $this->assertEquals(0, \count($database->find($collection)));

        // Test Offset
        $this->propagateBulkDocuments($collection, 100);
        $this->assertEquals(50, $database->updateDocuments($collection, new Document([
            'text' => 'text📝 updated',
        ]), [
            Query::offset(50),
        ]));

        $docs = $database->find($collection, [Query::equal('text', ['text📝 updated']), Query::limit(100)]);
        $this->assertCount(50, $docs);

        $lastDoc = end($docs);
        $this->assertNotEmpty($lastDoc);
        $this->assertEquals('doc99', $lastDoc->getId());

        $this->assertEquals(100, $database->deleteDocuments($collection));
    }

    /**
     * @depends testCreateDocument
     */
    public function testFulltextIndexWithInteger(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $this->expectException(Exception::class);

        if (!$this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $this->expectExceptionMessage('Fulltext index is not supported');
        } else {
            $this->expectExceptionMessage('Attribute "integer_signed" cannot be part of a FULLTEXT index, must be of type string');
        }

        $database->createIndex('documents', 'fulltext_integer', Database::INDEX_FULLTEXT, ['string','integer_signed']);
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
        /** @var Database $database */
        $database = static::getDatabase();

        $document->setAttribute('$id', 'duplicated');
        $database->createDocument($document->getCollection(), $document);
        $this->expectException(DuplicateException::class);
        $database->createDocument($document->getCollection(), $document);
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionCaseInsensitiveDuplicate(Document $document): Document
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $document->setAttribute('$id', 'caseSensitive');
        $document->setAttribute('$sequence', '200');
        $database->createDocument($document->getCollection(), $document);

        $document->setAttribute('$id', 'CaseSensitive');

        $this->expectException(DuplicateException::class);
        $database->createDocument($document->getCollection(), $document);

        return $document;
    }

    public function testEmptyTenant(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if ($database->getAdapter()->getSharedTables()) {
            $documents = $database->find(
                'documents',
                [Query::select(['*'])] // Mongo bug with Integer UID
            );

            $document = $documents[0];
            $doc = $database->getDocument($document->getCollection(), $document->getId());
            $this->assertEquals($document->getTenant(), $doc->getTenant());
            return;
        }

        $documents = $database->find(
            'documents',
            [Query::notEqual('$id', '56000')] // Mongo bug with Integer UID
        );

        $document = $documents[0];
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);

        $document = $database->getDocument('documents', $document->getId());
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);

        $document = $database->updateDocument('documents', $document->getId(), $document);
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);
    }

    public function testEmptyOperatorValues(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        try {
            $database->findOne('documents', [
                Query::equal('string', []),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals('Invalid query: Equal queries require at least one value.', $e->getMessage());
        }

        try {
            $database->findOne('documents', [
                Query::contains('string', []),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals('Invalid query: Contains queries require at least one value.', $e->getMessage());
        }
    }

    public function testDateTimeDocument(): void
    {
        /**
         * @var Database $database
         */
        $database = static::getDatabase();
        $collection = 'create_modify_dates';
        $database->createCollection($collection);
        $this->assertEquals(true, $database->createAttribute($collection, 'string', Database::VAR_STRING, 128, false));
        $this->assertEquals(true, $database->createAttribute($collection, 'datetime', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']));

        $date = '2000-01-01T10:00:00.000+00:00';
        // test - default behaviour of external datetime attribute not changed
        $doc = $database->createDocument($collection, new Document([
            '$id' => 'doc1',
            '$permissions' => [Permission::read(Role::any()),Permission::write(Role::any()),Permission::update(Role::any())],
            'datetime' => ''
        ]));
        $this->assertNotEmpty($doc->getAttribute('datetime'));
        $this->assertNotEmpty($doc->getAttribute('$createdAt'));
        $this->assertNotEmpty($doc->getAttribute('$updatedAt'));

        $doc = $database->getDocument($collection, 'doc1');
        $this->assertNotEmpty($doc->getAttribute('datetime'));
        $this->assertNotEmpty($doc->getAttribute('$createdAt'));
        $this->assertNotEmpty($doc->getAttribute('$updatedAt'));

        $database->setPreserveDates(true);
        // test - modifying $createdAt and $updatedAt
        $doc = $database->createDocument($collection, new Document([
            '$id' => 'doc2',
            '$permissions' => [Permission::read(Role::any()),Permission::write(Role::any()),Permission::update(Role::any())],
            '$createdAt' => $date
        ]));

        $this->assertEquals($doc->getAttribute('$createdAt'), $date);
        $this->assertNotEmpty($doc->getAttribute('$updatedAt'));
        $this->assertNotEquals($doc->getAttribute('$updatedAt'), $date);

        $doc = $database->getDocument($collection, 'doc2');

        $this->assertEquals($doc->getAttribute('$createdAt'), $date);
        $this->assertNotEmpty($doc->getAttribute('$updatedAt'));
        $this->assertNotEquals($doc->getAttribute('$updatedAt'), $date);

        $database->setPreserveDates(false);
        $database->deleteCollection($collection);
    }

    public function testSingleDocumentDateOperations(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        $collection = 'normal_date_operations';
        $database->createCollection($collection);
        $this->assertEquals(true, $database->createAttribute($collection, 'string', Database::VAR_STRING, 128, false));

        $database->setPreserveDates(true);

        $createDate = '2000-01-01T10:00:00.000+00:00';
        $updateDate = '2000-02-01T15:30:00.000+00:00';
        $date1 = '2000-01-01T10:00:00.000+00:00';
        $date2 = '2000-02-01T15:30:00.000+00:00';
        $date3 = '2000-03-01T20:45:00.000+00:00';
        // Test 1: Create with custom createdAt, then update with custom updatedAt
        $doc = $database->createDocument($collection, new Document([
            '$id' => 'doc1',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())],
            'string' => 'initial',
            '$createdAt' => $createDate
        ]));

        $this->assertEquals($createDate, $doc->getAttribute('$createdAt'));
        $this->assertNotEquals($createDate, $doc->getAttribute('$updatedAt'));

        // Update with custom updatedAt
        $doc->setAttribute('string', 'updated');
        $doc->setAttribute('$updatedAt', $updateDate);
        $updatedDoc = $database->updateDocument($collection, 'doc1', $doc);

        $this->assertEquals($createDate, $updatedDoc->getAttribute('$createdAt'));
        $this->assertEquals($updateDate, $updatedDoc->getAttribute('$updatedAt'));

        // Test 2: Create with both custom dates
        $doc2 = $database->createDocument($collection, new Document([
            '$id' => 'doc2',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())],
            'string' => 'both_dates',
            '$createdAt' => $createDate,
            '$updatedAt' => $updateDate
        ]));

        $this->assertEquals($createDate, $doc2->getAttribute('$createdAt'));
        $this->assertEquals($updateDate, $doc2->getAttribute('$updatedAt'));

        // Test 3: Create without dates, then update with custom dates
        $doc3 = $database->createDocument($collection, new Document([
            '$id' => 'doc3',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())],
            'string' => 'no_dates'
        ]));


        $doc3->setAttribute('string', 'updated_no_dates');
        $doc3->setAttribute('$createdAt', $createDate);
        $doc3->setAttribute('$updatedAt', $updateDate);
        $updatedDoc3 = $database->updateDocument($collection, 'doc3', $doc3);

        $this->assertEquals($createDate, $updatedDoc3->getAttribute('$createdAt'));
        $this->assertEquals($updateDate, $updatedDoc3->getAttribute('$updatedAt'));

        // Test 4: Update only createdAt
        $doc4 = $database->createDocument($collection, new Document([
            '$id' => 'doc4',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())],
            'string' => 'initial'
        ]));

        $originalCreatedAt4 = $doc4->getAttribute('$createdAt');
        $originalUpdatedAt4 = $doc4->getAttribute('$updatedAt');

        $doc4->setAttribute('$updatedAt', null);
        $doc4->setAttribute('$createdAt', null);
        $updatedDoc4 = $database->updateDocument($collection, 'doc4', document: $doc4);

        $this->assertEquals($originalCreatedAt4, $updatedDoc4->getAttribute('$createdAt'));
        $this->assertNotEquals($originalUpdatedAt4, $updatedDoc4->getAttribute('$updatedAt'));

        // Test 5: Update only updatedAt
        $updatedDoc4->setAttribute('$updatedAt', $updateDate);
        $updatedDoc4->setAttribute('$createdAt', $createDate);
        $finalDoc4 = $database->updateDocument($collection, 'doc4', $updatedDoc4);

        $this->assertEquals($createDate, $finalDoc4->getAttribute('$createdAt'));
        $this->assertEquals($updateDate, $finalDoc4->getAttribute('$updatedAt'));

        // Test 6: Create with updatedAt, update with createdAt
        $doc5 = $database->createDocument($collection, new Document([
            '$id' => 'doc5',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())],
            'string' => 'doc5',
            '$updatedAt' => $date2
        ]));

        $this->assertNotEquals($date2, $doc5->getAttribute('$createdAt'));
        $this->assertEquals($date2, $doc5->getAttribute('$updatedAt'));

        $doc5->setAttribute('string', 'doc5_updated');
        $doc5->setAttribute('$createdAt', $date1);
        $updatedDoc5 = $database->updateDocument($collection, 'doc5', $doc5);

        $this->assertEquals($date1, $updatedDoc5->getAttribute('$createdAt'));
        $this->assertEquals($date2, $updatedDoc5->getAttribute('$updatedAt'));

        // Test 7: Create with both dates, update with different dates
        $doc6 = $database->createDocument($collection, new Document([
            '$id' => 'doc6',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())],
            'string' => 'doc6',
            '$createdAt' => $date1,
            '$updatedAt' => $date2
        ]));

        $this->assertEquals($date1, $doc6->getAttribute('$createdAt'));
        $this->assertEquals($date2, $doc6->getAttribute('$updatedAt'));

        $doc6->setAttribute('string', 'doc6_updated');
        $doc6->setAttribute('$createdAt', $date3);
        $doc6->setAttribute('$updatedAt', $date3);
        $updatedDoc6 = $database->updateDocument($collection, 'doc6', $doc6);

        $this->assertEquals($date3, $updatedDoc6->getAttribute('$createdAt'));
        $this->assertEquals($date3, $updatedDoc6->getAttribute('$updatedAt'));

        // Test 8: Preserve dates disabled
        $database->setPreserveDates(false);

        $customDate = '2000-01-01T10:00:00.000+00:00';

        $doc7 = $database->createDocument($collection, new Document([
            '$id' => 'doc7',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())],
            'string' => 'doc7',
            '$createdAt' => $customDate,
            '$updatedAt' => $customDate
        ]));

        $this->assertNotEquals($customDate, $doc7->getAttribute('$createdAt'));
        $this->assertNotEquals($customDate, $doc7->getAttribute('$updatedAt'));

        // Update with custom dates should also be ignored
        $doc7->setAttribute('string', 'updated');
        $doc7->setAttribute('$createdAt', $customDate);
        $doc7->setAttribute('$updatedAt', $customDate);
        $updatedDoc7 = $database->updateDocument($collection, 'doc7', $doc7);

        $this->assertNotEquals($customDate, $updatedDoc7->getAttribute('$createdAt'));
        $this->assertNotEquals($customDate, $updatedDoc7->getAttribute('$updatedAt'));

        // Test checking updatedAt updates even old document exists
        $database->setPreserveDates(true);
        $doc11 = $database->createDocument($collection, new Document([
            '$id' => 'doc11',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())],
            'string' => 'no_dates',
            '$createdAt' => $customDate
        ]));

        $newUpdatedAt = $doc11->getUpdatedAt();

        $newDoc11 = new Document([
            'string' => 'no_dates_update',
        ]);
        $updatedDoc7 = $database->updateDocument($collection, 'doc11', $newDoc11);
        $this->assertNotEquals($newUpdatedAt, $updatedDoc7->getAttribute('$updatedAt'));

        $database->setPreserveDates(false);
        $database->deleteCollection($collection);
    }

    public function testBulkDocumentDateOperations(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        $collection = 'bulk_date_operations';
        $database->createCollection($collection);
        $this->assertEquals(true, $database->createAttribute($collection, 'string', Database::VAR_STRING, 128, false));

        $database->setPreserveDates(true);

        $createDate = '2000-01-01T10:00:00.000+00:00';
        $updateDate = '2000-02-01T15:30:00.000+00:00';
        $permissions = [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())];

        // Test 1: Bulk create with different date configurations
        $documents = [
            new Document([
                '$id' => 'doc1',
                '$permissions' => $permissions,
                'string' => 'doc1',
                '$createdAt' => $createDate
            ]),
            new Document([
                '$id' => 'doc2',
                '$permissions' => $permissions,
                'string' => 'doc2',
                '$updatedAt' => $updateDate
            ]),
            new Document([
                '$id' => 'doc3',
                '$permissions' => $permissions,
                'string' => 'doc3',
                '$createdAt' => $createDate,
                '$updatedAt' => $updateDate
            ]),
            new Document([
                '$id' => 'doc4',
                '$permissions' => $permissions,
                'string' => 'doc4'
            ]),
            new Document([
                '$id' => 'doc5',
                '$permissions' => $permissions,
                'string' => 'doc5',
                '$createdAt' => null
            ]),
            new Document([
                '$id' => 'doc6',
                '$permissions' => $permissions,
                'string' => 'doc6',
                '$updatedAt' => null
            ])
        ];

        $database->createDocuments($collection, $documents);

        // Verify initial state
        foreach (['doc1', 'doc3'] as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertEquals($createDate, $doc->getAttribute('$createdAt'), "createdAt mismatch for $id");
        }

        foreach (['doc2', 'doc3'] as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertEquals($updateDate, $doc->getAttribute('$updatedAt'), "updatedAt mismatch for $id");
        }

        foreach (['doc4', 'doc5', 'doc6'] as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertNotEmpty($doc->getAttribute('$createdAt'), "createdAt missing for $id");
            $this->assertNotEmpty($doc->getAttribute('$updatedAt'), "updatedAt missing for $id");
        }

        // Test 2: Bulk update with custom dates
        $updateDoc = new Document([
            'string' => 'updated',
            '$createdAt' => $createDate,
            '$updatedAt' => $updateDate
        ]);
        $ids = [];
        foreach ($documents as $doc) {
            $ids[] = $doc->getId();
        }
        $count = $database->updateDocuments($collection, $updateDoc, [
            Query::equal('$id', $ids)
        ]);
        $this->assertEquals(6, $count);

        foreach (['doc1', 'doc3'] as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertEquals($createDate, $doc->getAttribute('$createdAt'), "createdAt mismatch for $id");
            $this->assertEquals($updateDate, $doc->getAttribute('$updatedAt'), "updatedAt mismatch for $id");
            $this->assertEquals('updated', $doc->getAttribute('string'), "string mismatch for $id");
        }

        foreach (['doc2', 'doc4','doc5','doc6'] as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertEquals($updateDate, $doc->getAttribute('$updatedAt'), "updatedAt mismatch for $id");
            $this->assertEquals('updated', $doc->getAttribute('string'), "string mismatch for $id");
        }

        // Test 3: Bulk update with preserve dates disabled
        $database->setPreserveDates(false);

        $customDate = 'should be ignored anyways so no error';
        $updateDocDisabled = new Document([
            'string' => 'disabled_update',
            '$createdAt' => $customDate,
            '$updatedAt' => $customDate
        ]);

        $countDisabled = $database->updateDocuments($collection, $updateDocDisabled);
        $this->assertEquals(6, $countDisabled);

        // Test 4: Bulk update with preserve dates re-enabled
        $database->setPreserveDates(true);

        $newDate = '2000-03-01T20:45:00.000+00:00';
        $updateDocEnabled = new Document([
            'string' => 'enabled_update',
            '$createdAt' => $newDate,
            '$updatedAt' => $newDate
        ]);

        $countEnabled = $database->updateDocuments($collection, $updateDocEnabled);
        $this->assertEquals(6, $countEnabled);

        $database->setPreserveDates(false);
        $database->deleteCollection($collection);
    }

    public function testUpsertDateOperations(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForUpserts()) {
            return;
        }

        $collection = 'upsert_date_operations';
        $database->createCollection($collection);
        $this->assertEquals(true, $database->createAttribute($collection, 'string', Database::VAR_STRING, 128, false));

        $database->setPreserveDates(true);

        $createDate = '2000-01-01T10:00:00.000+00:00';
        $updateDate = '2000-02-01T15:30:00.000+00:00';
        $date1 = '2000-01-01T10:00:00.000+00:00';
        $date2 = '2000-02-01T15:30:00.000+00:00';
        $date3 = '2000-03-01T20:45:00.000+00:00';
        $permissions = [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())];

        // Test 1: Upsert new document with custom createdAt
        $upsertResults = [];
        $database->upsertDocuments($collection, [
            new Document([
                '$id' => 'upsert1',
                '$permissions' => $permissions,
                'string' => 'upsert1_initial',
                '$createdAt' => $createDate
            ])
        ], onNext: function ($doc) use (&$upsertResults) {
            $upsertResults[] = $doc;
        });
        $upsertDoc1 = $upsertResults[0];

        $this->assertEquals($createDate, $upsertDoc1->getAttribute('$createdAt'));
        $this->assertNotEquals($createDate, $upsertDoc1->getAttribute('$updatedAt'));

        // Test 2: Upsert existing document with custom updatedAt
        $upsertDoc1->setAttribute('string', 'upsert1_updated');
        $upsertDoc1->setAttribute('$updatedAt', $updateDate);
        $updatedUpsertResults = [];
        $database->upsertDocuments($collection, [$upsertDoc1], onNext: function ($doc) use (&$updatedUpsertResults) {
            $updatedUpsertResults[] = $doc;
        });
        $updatedUpsertDoc1 = $updatedUpsertResults[0];

        $this->assertEquals($createDate, $updatedUpsertDoc1->getAttribute('$createdAt'));
        $this->assertEquals($updateDate, $updatedUpsertDoc1->getAttribute('$updatedAt'));

        // Test 3: Upsert new document with both custom dates
        $upsertResults2 = [];
        $database->upsertDocuments($collection, [
            new Document([
                '$id' => 'upsert2',
                '$permissions' => $permissions,
                'string' => 'upsert2_both_dates',
                '$createdAt' => $createDate,
                '$updatedAt' => $updateDate
            ])
        ], onNext: function ($doc) use (&$upsertResults2) {
            $upsertResults2[] = $doc;
        });
        $upsertDoc2 = $upsertResults2[0];

        $this->assertEquals($createDate, $upsertDoc2->getAttribute('$createdAt'));
        $this->assertEquals($updateDate, $upsertDoc2->getAttribute('$updatedAt'));

        // Test 4: Upsert existing document with different dates
        $upsertDoc2->setAttribute('string', 'upsert2_updated');
        $upsertDoc2->setAttribute('$createdAt', $date3);
        $upsertDoc2->setAttribute('$updatedAt', $date3);
        $updatedUpsertResults2 = [];
        $database->upsertDocuments($collection, [$upsertDoc2], onNext: function ($doc) use (&$updatedUpsertResults2) {
            $updatedUpsertResults2[] = $doc;
        });
        $updatedUpsertDoc2 = $updatedUpsertResults2[0];

        $this->assertEquals($date3, $updatedUpsertDoc2->getAttribute('$createdAt'));
        $this->assertEquals($date3, $updatedUpsertDoc2->getAttribute('$updatedAt'));

        // Test 5: Upsert with preserve dates disabled
        $database->setPreserveDates(false);

        $customDate = '2000-01-01T10:00:00.000+00:00';
        $upsertResults3 = [];
        $database->upsertDocuments($collection, [
            new Document([
                '$id' => 'upsert3',
                '$permissions' => $permissions,
                'string' => 'upsert3_disabled',
                '$createdAt' => $customDate,
                '$updatedAt' => $customDate
            ])
        ], onNext: function ($doc) use (&$upsertResults3) {
            $upsertResults3[] = $doc;
        });
        $upsertDoc3 = $upsertResults3[0];

        $this->assertNotEquals($customDate, $upsertDoc3->getAttribute('$createdAt'));
        $this->assertNotEquals($customDate, $upsertDoc3->getAttribute('$updatedAt'));

        // Update with custom dates should also be ignored
        $upsertDoc3->setAttribute('string', 'upsert3_updated');
        $upsertDoc3->setAttribute('$createdAt', $customDate);
        $upsertDoc3->setAttribute('$updatedAt', $customDate);
        $updatedUpsertResults3 = [];
        $database->upsertDocuments($collection, [$upsertDoc3], onNext: function ($doc) use (&$updatedUpsertResults3) {
            $updatedUpsertResults3[] = $doc;
        });
        $updatedUpsertDoc3 = $updatedUpsertResults3[0];

        $this->assertNotEquals($customDate, $updatedUpsertDoc3->getAttribute('$createdAt'));
        $this->assertNotEquals($customDate, $updatedUpsertDoc3->getAttribute('$updatedAt'));

        // Test 6: Bulk upsert operations with custom dates
        $database->setPreserveDates(true);

        // Test 7: Bulk upsert with different date configurations
        $upsertDocuments = [
            new Document([
                '$id' => 'bulk_upsert1',
                '$permissions' => $permissions,
                'string' => 'bulk_upsert1_initial',
                '$createdAt' => $createDate
            ]),
            new Document([
                '$id' => 'bulk_upsert2',
                '$permissions' => $permissions,
                'string' => 'bulk_upsert2_initial',
                '$updatedAt' => $updateDate
            ]),
            new Document([
                '$id' => 'bulk_upsert3',
                '$permissions' => $permissions,
                'string' => 'bulk_upsert3_initial',
                '$createdAt' => $createDate,
                '$updatedAt' => $updateDate
            ]),
            new Document([
                '$id' => 'bulk_upsert4',
                '$permissions' => $permissions,
                'string' => 'bulk_upsert4_initial'
            ])
        ];

        $bulkUpsertResults = [];
        $database->upsertDocuments($collection, $upsertDocuments, onNext: function ($doc) use (&$bulkUpsertResults) {
            $bulkUpsertResults[] = $doc;
        });

        // Test 8: Verify initial bulk upsert state
        foreach (['bulk_upsert1', 'bulk_upsert3'] as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertEquals($createDate, $doc->getAttribute('$createdAt'), "createdAt mismatch for $id");
        }

        foreach (['bulk_upsert2', 'bulk_upsert3'] as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertEquals($updateDate, $doc->getAttribute('$updatedAt'), "updatedAt mismatch for $id");
        }

        foreach (['bulk_upsert4'] as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertNotEmpty($doc->getAttribute('$createdAt'), "createdAt missing for $id");
            $this->assertNotEmpty($doc->getAttribute('$updatedAt'), "updatedAt missing for $id");
        }

        // Test 9: Bulk upsert update with custom dates using updateDocuments
        $newDate = '2000-04-01T12:00:00.000+00:00';
        $updateUpsertDoc = new Document([
            'string' => 'bulk_upsert_updated',
            '$createdAt' => $newDate,
            '$updatedAt' => $newDate
        ]);

        $upsertIds = [];
        foreach ($upsertDocuments as $doc) {
            $upsertIds[] = $doc->getId();
        }

        $database->updateDocuments($collection, $updateUpsertDoc, [
            Query::equal('$id', $upsertIds)
        ]);

        foreach ($upsertIds as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertEquals($newDate, $doc->getAttribute('$createdAt'), "createdAt mismatch for $id");
            $this->assertEquals($newDate, $doc->getAttribute('$updatedAt'), "updatedAt mismatch for $id");
            $this->assertEquals('bulk_upsert_updated', $doc->getAttribute('string'), "string mismatch for $id");
        }

        // Test 10: checking by passing null to each
        $updateUpsertDoc = new Document([
            'string' => 'bulk_upsert_updated',
            '$createdAt' => null,
            '$updatedAt' => null
        ]);

        $upsertIds = [];
        foreach ($upsertDocuments as $doc) {
            $upsertIds[] = $doc->getId();
        }

        $database->updateDocuments($collection, $updateUpsertDoc, [
            Query::equal('$id', $upsertIds)
        ]);

        foreach ($upsertIds as $id) {
            $doc = $database->getDocument($collection, $id);
            $this->assertNotEmpty($doc->getAttribute('$createdAt'), "createdAt mismatch for $id");
            $this->assertNotEmpty($doc->getAttribute('$updatedAt'), "updatedAt mismatch for $id");
        }

        // Test 11: Bulk upsert operations with upsertDocuments
        $upsertUpdateDocuments = [];
        foreach ($upsertDocuments as $doc) {
            $updatedDoc = clone $doc;
            $updatedDoc->setAttribute('string', 'bulk_upsert_updated_via_upsert');
            $updatedDoc->setAttribute('$createdAt', $newDate);
            $updatedDoc->setAttribute('$updatedAt', $newDate);
            $upsertUpdateDocuments[] = $updatedDoc;
        }

        $upsertUpdateResults = [];
        $countUpsertUpdate = $database->upsertDocuments($collection, $upsertUpdateDocuments, onNext: function ($doc) use (&$upsertUpdateResults) {
            $upsertUpdateResults[] = $doc;
        });
        $this->assertEquals(4, $countUpsertUpdate);

        foreach ($upsertUpdateResults as $doc) {
            $this->assertEquals($newDate, $doc->getAttribute('$createdAt'), "createdAt mismatch for upsert update");
            $this->assertEquals($newDate, $doc->getAttribute('$updatedAt'), "updatedAt mismatch for upsert update");
            $this->assertEquals('bulk_upsert_updated_via_upsert', $doc->getAttribute('string'), "string mismatch for upsert update");
        }

        // Test 12: Bulk upsert with preserve dates disabled
        $database->setPreserveDates(false);

        $customDate = 'should be ignored anyways so no error';
        $upsertDisabledDocuments = [];
        foreach ($upsertDocuments as $doc) {
            $disabledDoc = clone $doc;
            $disabledDoc->setAttribute('string', 'bulk_upsert_disabled');
            $disabledDoc->setAttribute('$createdAt', $customDate);
            $disabledDoc->setAttribute('$updatedAt', $customDate);
            $upsertDisabledDocuments[] = $disabledDoc;
        }

        $upsertDisabledResults = [];
        $countUpsertDisabled = $database->upsertDocuments($collection, $upsertDisabledDocuments, onNext: function ($doc) use (&$upsertDisabledResults) {
            $upsertDisabledResults[] = $doc;
        });
        $this->assertEquals(4, $countUpsertDisabled);

        foreach ($upsertDisabledResults as $doc) {
            $this->assertNotEquals($customDate, $doc->getAttribute('$createdAt'), "createdAt should not be custom date when disabled");
            $this->assertNotEquals($customDate, $doc->getAttribute('$updatedAt'), "updatedAt should not be custom date when disabled");
            $this->assertEquals('bulk_upsert_disabled', $doc->getAttribute('string'), "string mismatch for disabled upsert");
        }

        $database->setPreserveDates(false);
        $database->deleteCollection($collection);
    }

    public function testUpdateDocumentsCount(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForUpserts()) {
            return;
        }

        $collectionName = "update_count";
        $database->createCollection($collectionName);

        $database->createAttribute($collectionName, 'key', Database::VAR_STRING, 60, false);
        $database->createAttribute($collectionName, 'value', Database::VAR_STRING, 60, false);

        $permissions = [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())];

        $docs =  [
            new Document([
                '$id' => 'bulk_upsert1',
                '$permissions' => $permissions,
                'key' => 'bulk_upsert1_initial',
            ]),
            new Document([
                '$id' => 'bulk_upsert2',
                '$permissions' => $permissions,
                'key' => 'bulk_upsert2_initial',
            ]),
            new Document([
                '$id' => 'bulk_upsert3',
                '$permissions' => $permissions,
                'key' => 'bulk_upsert3_initial',
            ]),
            new Document([
                '$id' => 'bulk_upsert4',
                '$permissions' => $permissions,
                'key' => 'bulk_upsert4_initial'
            ])
        ];
        $upsertUpdateResults = [];
        $count = $database->upsertDocuments($collectionName, $docs, onNext: function ($doc) use (&$upsertUpdateResults) {
            $upsertUpdateResults[] = $doc;
        });
        $this->assertCount(4, $upsertUpdateResults);
        $this->assertEquals(4, $count);

        $updates = new Document(['value' => 'test']);
        $newDocs = [];
        $count = $database->updateDocuments($collectionName, $updates, onNext:function ($doc) use (&$newDocs) {
            $newDocs[] = $doc;
        });

        $this->assertCount(4, $newDocs);
        $this->assertEquals(4, $count);

        $database->deleteCollection($collectionName);
    }

    public function testCreateUpdateDocumentsMismatch(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // with different set of attributes
        $colName = "docs_with_diff";
        $database->createCollection($colName);
        $database->createAttribute($colName, 'key', Database::VAR_STRING, 50, true);
        $database->createAttribute($colName, 'value', Database::VAR_STRING, 50, false, 'value');
        $permissions = [Permission::read(Role::any()), Permission::write(Role::any()),Permission::update(Role::any())];
        $docs =  [
            new Document([
                '$id' => 'doc1',
                'key' => 'doc1',
            ]),
            new Document([
                '$id' => 'doc2',
                'key' => 'doc2',
                'value' => 'test',
            ]),
            new Document([
                '$id' => 'doc3',
                '$permissions' => $permissions,
                'key' => 'doc3'
            ]),
        ];
        $this->assertEquals(3, $database->createDocuments($colName, $docs));
        // we should get only one document as read permission provided to the last document only
        $addedDocs = $database->find($colName);
        $this->assertCount(1, $addedDocs);
        $doc = $addedDocs[0];
        $this->assertEquals('doc3', $doc->getId());
        $this->assertNotEmpty($doc->getPermissions());
        $this->assertCount(3, $doc->getPermissions());

        $database->createDocument($colName, new Document([
            '$id' => 'doc4',
            '$permissions' => $permissions,
            'key' => 'doc4'
        ]));

        $this->assertEquals(2, $database->updateDocuments($colName, new Document(['key' => 'new doc'])));
        $doc = $database->getDocument($colName, 'doc4');
        $this->assertEquals('doc4', $doc->getId());
        $this->assertEquals('value', $doc->getAttribute('value'));

        $addedDocs = $database->find($colName);
        $this->assertCount(2, $addedDocs);
        foreach ($addedDocs as $doc) {
            $this->assertNotEmpty($doc->getPermissions());
            $this->assertCount(3, $doc->getPermissions());
            $this->assertEquals('value', $doc->getAttribute('value'));
        }
        $database->deleteCollection($colName);
    }
}

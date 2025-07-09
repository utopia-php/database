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
        $count = $database->createOrUpdateDocuments(
            __FUNCTION__,
            $documents,
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );
var_dump($results);
        $this->assertEquals(2, $count);
        $this->assertEquals('shmuel', 'shmuel2');

        $createdAt = [];
        foreach ($results as $index => $document) {
            $createdAt[$index] = $document->getCreatedAt();
            $this->assertNotEmpty(true, $document->getId());
            $this->assertArrayHasKey('$sequence', $document);
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
        $count = $database->createOrUpdateDocuments(__FUNCTION__, $documents, onNext: function ($doc) use (&$results) {
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

        $database->createOrUpdateDocumentsWithIncrease(
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

        $database->createOrUpdateDocumentsWithIncrease(
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

        $database->createOrUpdateDocuments(__FUNCTION__, [$document]);

        try {
            $database->createOrUpdateDocuments(__FUNCTION__, [$document->setAttribute('string', 'updated')]);
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

        $database->createOrUpdateDocuments(__FUNCTION__, [$document]);

        $results = [];
        $count = $database->createOrUpdateDocuments(
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

        $database->createOrUpdateDocuments(__FUNCTION__, [$document]);

        $newPermissions = [
            Permission::read(Role::any()),
            Permission::update(Role::user('user1')),
            Permission::delete(Role::user('user1')),
        ];

        $results = [];
        $count = $database->createOrUpdateDocuments(
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
        $docs = $database->createOrUpdateDocuments(__FUNCTION__, [
            $existingDocument->setAttribute('first', 'updated'),
            $newDocument,
        ]);

        $this->assertEquals(2, $docs);
        $this->assertEquals('updated', $existingDocument->getAttribute('first'));
        $this->assertEquals('last', $existingDocument->getAttribute('last'));
        $this->assertEquals('second', $newDocument->getAttribute('first'));
        $this->assertEquals('', $newDocument->getAttribute('last'));

        try {
            $database->createOrUpdateDocuments(__FUNCTION__, [
                $existingDocument->removeAttribute('first'),
                $newDocument
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof StructureException, $e->getMessage());
        }

        // Ensure missing optionals on existing document is allowed
        $docs = $database->createOrUpdateDocuments(__FUNCTION__, [
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
        $docs = $database->createOrUpdateDocuments(__FUNCTION__, [
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
        $docs = $database->createOrUpdateDocuments(__FUNCTION__, [
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

        $count = static::getDatabase()->createOrUpdateDocuments(__FUNCTION__, [$document]);
        $this->assertEquals(1, $count);

        // No changes, should return 0
        $count = static::getDatabase()->createOrUpdateDocuments(__FUNCTION__, [$document]);
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
            $db->createOrUpdateDocuments(__FUNCTION__, [$doc1, $doc2]);
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

        $db->createOrUpdateDocuments(__FUNCTION__, [$d1, $d2]);

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

        $this->expectException(Exception::class);
        $this->assertEquals(false, $database->decreaseDocumentAttribute('increase_decrease', $document->getId(), 'decrease', 10, 99));
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
}

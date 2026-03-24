<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use PDOException;
use Throwable;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\SetType;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

trait DocumentTests
{
    private static bool $documentsFixtureInit = false;

    private static ?Document $documentsFixtureDoc = null;

    /**
     * Create the 'documents' collection with standard attributes and a test document.
     * Cached for non-functional mode backward compatibility.
     */
    protected function initDocumentsFixture(): Document
    {
        if (self::$documentsFixtureInit && self::$documentsFixtureDoc !== null) {
            return self::$documentsFixtureDoc;
        }

        $database = $this->getDatabase();
        $database->createCollection('documents');

        $database->createAttribute('documents', new Attribute(key: 'string', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute('documents', new Attribute(key: 'integer_signed', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute('documents', new Attribute(key: 'integer_unsigned', type: ColumnType::Integer, size: 4, required: true, signed: false));
        $database->createAttribute('documents', new Attribute(key: 'bigint_signed', type: ColumnType::Integer, size: 8, required: true));
        $database->createAttribute('documents', new Attribute(key: 'bigint_unsigned', type: ColumnType::Integer, size: 9, required: true, signed: false));
        $database->createAttribute('documents', new Attribute(key: 'float_signed', type: ColumnType::Double, size: 0, required: true));
        $database->createAttribute('documents', new Attribute(key: 'float_unsigned', type: ColumnType::Double, size: 0, required: true, signed: false));
        $database->createAttribute('documents', new Attribute(key: 'boolean', type: ColumnType::Boolean, size: 0, required: true));
        $database->createAttribute('documents', new Attribute(key: 'colors', type: ColumnType::String, size: 32, required: true, default: null, signed: true, array: true));
        $database->createAttribute('documents', new Attribute(key: 'empty', type: ColumnType::String, size: 32, required: false, default: null, signed: true, array: true));
        $database->createAttribute('documents', new Attribute(key: 'with-dash', type: ColumnType::String, size: 128, required: false, default: null));
        $database->createAttribute('documents', new Attribute(key: 'id', type: ColumnType::Id, size: 0, required: false, default: null));

        $sequence = '1000000';
        if ($database->getAdapter()->getIdAttributeType() == ColumnType::Uuid7->value) {
            $sequence = '01890dd5-7331-7f3a-9c1b-123456789abc';
        }

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
            'integer_signed' => -Database::MAX_INT,
            'integer_unsigned' => Database::MAX_INT,
            'bigint_signed' => -Database::MAX_BIG_INT,
            'bigint_unsigned' => Database::MAX_BIG_INT,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
            'with-dash' => 'Works',
            'id' => $sequence,
        ]));

        self::$documentsFixtureInit = true;
        self::$documentsFixtureDoc = $document;

        return $document;
    }

    private static bool $moviesFixtureInit = false;

    private static ?array $moviesFixtureData = null;

    /**
     * Create the 'movies' collection with standard test data.
     * Returns ['$sequence' => ...].
     */
    protected function initMoviesFixture(): array
    {
        if (self::$moviesFixtureInit && self::$moviesFixtureData !== null) {
            return self::$moviesFixtureData;
        }

        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
        $this->getDatabase()->getAuthorization()->addRole('user:x');
        $database = $this->getDatabase();

        $database->createCollection('movies', permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users()),
        ]);

        $database->createAttribute('movies', new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute('movies', new Attribute(key: 'director', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute('movies', new Attribute(key: 'year', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute('movies', new Attribute(key: 'price', type: ColumnType::Double, size: 0, required: true));
        $database->createAttribute('movies', new Attribute(key: 'active', type: ColumnType::Boolean, size: 0, required: true));
        $database->createAttribute('movies', new Attribute(key: 'genres', type: ColumnType::String, size: 32, required: true, default: null, signed: true, array: true));
        $database->createAttribute('movies', new Attribute(key: 'with-dash', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute('movies', new Attribute(key: 'nullable', type: ColumnType::String, size: 128, required: false));

        $permissions = [
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
        ];

        $document = $database->createDocument('movies', new Document([
            '$id' => ID::custom('frozen'),
            '$permissions' => $permissions,
            'name' => 'Frozen',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2013,
            'price' => 39.50,
            'active' => true,
            'genres' => ['animation', 'kids'],
            'with-dash' => 'Works',
        ]));

        $database->createDocument('movies', new Document([
            '$permissions' => $permissions,
            'name' => 'Frozen II',
            'director' => 'Chris Buck & Jennifer Lee',
            'year' => 2019,
            'price' => 39.50,
            'active' => true,
            'genres' => ['animation', 'kids'],
            'with-dash' => 'Works',
        ]));

        $database->createDocument('movies', new Document([
            '$permissions' => $permissions,
            'name' => 'Captain America: The First Avenger',
            'director' => 'Joe Johnston',
            'year' => 2011,
            'price' => 25.94,
            'active' => true,
            'genres' => ['science fiction', 'action', 'comics'],
            'with-dash' => 'Works2',
        ]));

        $database->createDocument('movies', new Document([
            '$permissions' => $permissions,
            'name' => 'Captain Marvel',
            'director' => 'Anna Boden & Ryan Fleck',
            'year' => 2019,
            'price' => 25.99,
            'active' => true,
            'genres' => ['science fiction', 'action', 'comics'],
            'with-dash' => 'Works2',
        ]));

        $database->createDocument('movies', new Document([
            '$permissions' => $permissions,
            'name' => 'Work in Progress',
            'director' => 'TBD',
            'year' => 2025,
            'price' => 0.0,
            'active' => false,
            'genres' => [],
            'with-dash' => 'Works3',
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
            'nullable' => 'Not null',
        ]));

        self::$moviesFixtureInit = true;
        self::$moviesFixtureData = ['$sequence' => $document->getSequence()];

        return self::$moviesFixtureData;
    }

    private static bool $incDecFixtureInit = false;

    private static ?Document $incDecFixtureDoc = null;

    /**
     * Create the 'increase_decrease' collection and perform initial operations.
     */
    protected function initIncreaseDecreaseFixture(): Document
    {
        if (self::$incDecFixtureInit && self::$incDecFixtureDoc !== null) {
            return self::$incDecFixtureDoc;
        }

        $database = $this->getDatabase();
        $collection = 'increase_decrease';
        $database->createCollection($collection);

        $database->createAttribute($collection, new Attribute(key: 'increase', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute($collection, new Attribute(key: 'decrease', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute($collection, new Attribute(key: 'increase_text', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($collection, new Attribute(key: 'increase_float', type: ColumnType::Double, size: 0, required: true));
        $database->createAttribute($collection, new Attribute(key: 'sizes', type: ColumnType::Integer, size: 8, required: false, array: true));

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
            ],
        ]));

        $database->increaseDocumentAttribute($collection, $document->getId(), 'increase', 1, 101);
        $database->decreaseDocumentAttribute($collection, $document->getId(), 'decrease', 1, 98);
        $database->increaseDocumentAttribute($collection, $document->getId(), 'increase_float', 5.5, 110);
        $database->decreaseDocumentAttribute($collection, $document->getId(), 'increase_float', 1.1, 100);

        $document = $database->getDocument($collection, $document->getId());
        self::$incDecFixtureInit = true;
        self::$incDecFixtureDoc = $document;

        return $document;
    }

    public function testBigintSequence(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(__FUNCTION__);

        $sequence = 5_000_000_000_000_000;
        if ($database->getAdapter()->getIdAttributeType() == ColumnType::Uuid7->value) {
            $sequence = '01995753-881b-78cf-9506-2cffecf8f227';
        }

        $document = $database->createDocument(__FUNCTION__, new Document([
            '$sequence' => (string) $sequence,
            '$permissions' => [
                Permission::read(Role::any()),
            ],
        ]));

        $this->assertEquals((string) $sequence, $document->getSequence());

        $document = $database->getDocument(__FUNCTION__, $document->getId());
        $this->assertEquals((string) $sequence, $document->getSequence());

        $document = $database->findOne(__FUNCTION__, [Query::equal('$sequence', [(string) $sequence])]);
        $this->assertEquals((string) $sequence, $document->getSequence());
    }

    public function testCreateDocument(): void
    {
        $document = $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $sequence = '1000000';
        if ($database->getAdapter()->getIdAttributeType() == ColumnType::Uuid7->value) {
            $sequence = '01890dd5-7331-7f3a-9c1b-123456789abc';
        }

        $this->assertNotEmpty($document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
        $this->assertIsInt($document->getAttribute('integer_signed'));
        $this->assertEquals(-Database::MAX_INT, $document->getAttribute('integer_signed'));
        $this->assertIsInt($document->getAttribute('integer_unsigned'));
        $this->assertEquals(Database::MAX_INT, $document->getAttribute('integer_unsigned'));
        $this->assertIsInt($document->getAttribute('bigint_signed'));
        $this->assertEquals(-Database::MAX_BIG_INT, $document->getAttribute('bigint_signed'));
        $this->assertIsInt($document->getAttribute('bigint_signed'));
        $this->assertEquals(Database::MAX_BIG_INT, $document->getAttribute('bigint_unsigned'));
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
        $this->assertEquals($sequence, $document->getAttribute('id'));

        $sequence = '56000';
        if ($database->getAdapter()->getIdAttributeType() == ColumnType::Uuid7->value) {
            $sequence = '01890dd5-7331-7f3a-9c1b-123456789def';
        }

        // Test create document with manual internal id
        $manualIdDocument = $database->createDocument('documents', new Document([
            '$id' => '56000',
            '$sequence' => $sequence,
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
            'integer_signed' => -Database::MAX_INT,
            'integer_unsigned' => Database::MAX_INT,
            'bigint_signed' => -Database::MAX_BIG_INT,
            'bigint_unsigned' => Database::MAX_BIG_INT,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
            'with-dash' => 'Works',
        ]));

        $this->assertEquals($sequence, $manualIdDocument->getSequence());
        $this->assertNotEmpty($manualIdDocument->getId());
        $this->assertIsString($manualIdDocument->getAttribute('string'));
        $this->assertEquals('text📝', $manualIdDocument->getAttribute('string')); // Also makes sure an emoji is working
        $this->assertIsInt($manualIdDocument->getAttribute('integer_signed'));
        $this->assertEquals(-Database::MAX_INT, $manualIdDocument->getAttribute('integer_signed'));
        $this->assertIsInt($manualIdDocument->getAttribute('integer_unsigned'));
        $this->assertEquals(Database::MAX_INT, $manualIdDocument->getAttribute('integer_unsigned'));
        $this->assertIsInt($manualIdDocument->getAttribute('bigint_signed'));
        $this->assertEquals(-Database::MAX_BIG_INT, $manualIdDocument->getAttribute('bigint_signed'));
        $this->assertIsInt($manualIdDocument->getAttribute('bigint_unsigned'));
        $this->assertEquals(Database::MAX_BIG_INT, $manualIdDocument->getAttribute('bigint_unsigned'));
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

        $this->assertEquals($sequence, $manualIdDocument->getSequence());
        $this->assertNotEmpty($manualIdDocument->getId());
        $this->assertIsString($manualIdDocument->getAttribute('string'));
        $this->assertEquals('text📝', $manualIdDocument->getAttribute('string')); // Also makes sure an emoji is working
        $this->assertIsInt($manualIdDocument->getAttribute('integer_signed'));
        $this->assertEquals(-Database::MAX_INT, $manualIdDocument->getAttribute('integer_signed'));
        $this->assertIsInt($manualIdDocument->getAttribute('integer_unsigned'));
        $this->assertEquals(Database::MAX_INT, $manualIdDocument->getAttribute('integer_unsigned'));
        $this->assertIsInt($manualIdDocument->getAttribute('bigint_signed'));
        $this->assertEquals(-Database::MAX_BIG_INT, $manualIdDocument->getAttribute('bigint_signed'));
        $this->assertIsInt($manualIdDocument->getAttribute('bigint_unsigned'));
        $this->assertEquals(Database::MAX_BIG_INT, $manualIdDocument->getAttribute('bigint_unsigned'));
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
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertTrue($e instanceof StructureException);
                $this->assertStringContainsString('Invalid document structure: Attribute "float_unsigned" has invalid type. Value must be a valid range between 0 and', $e->getMessage());
            }
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
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertTrue($e instanceof StructureException);
                $this->assertEquals('Invalid document structure: Attribute "bigint_unsigned" has invalid type. Value must be a valid range between 0 and 9,223,372,036,854,775,807', $e->getMessage());
            }
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
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertTrue($e instanceof StructureException);
                $this->assertEquals('Invalid document structure: Attribute "$sequence" has invalid type. Invalid sequence value', $e->getMessage());
            }
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
        $this->assertNotEmpty($documentIdNull->getSequence());
        $this->assertNull($documentIdNull->getAttribute('id'));

        $documentIdNull = $database->getDocument('documents', $documentIdNull->getId());
        $this->assertNotEmpty($documentIdNull->getId());
        $this->assertNull($documentIdNull->getAttribute('id'));

        $documentIdNull = $database->findOne('documents', [
            query::isNull('id'),
        ]);
        $this->assertNotEmpty($documentIdNull->getId());
        $this->assertNull($documentIdNull->getAttribute('id'));

        $sequence = '0';
        if ($database->getAdapter()->getIdAttributeType() == ColumnType::Uuid7->value) {
            $sequence = '01890dd5-7331-7f3a-9c1b-123456789abc';
        }

        /**
         * Insert ID attribute with '0'
         */
        $documentId0 = $database->createDocument('documents', new Document([
            'id' => $sequence,
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
        $this->assertNotEmpty($documentId0->getSequence());

        $this->assertIsString($documentId0->getAttribute('id'));
        $this->assertEquals($sequence, $documentId0->getAttribute('id'));

        $documentId0 = $database->getDocument('documents', $documentId0->getId());
        $this->assertNotEmpty($documentId0->getSequence());
        $this->assertIsString($documentId0->getAttribute('id'));
        $this->assertEquals($sequence, $documentId0->getAttribute('id'));

        $documentId0 = $database->findOne('documents', [
            query::equal('id', [$sequence]),
        ]);
        $this->assertNotEmpty($documentId0->getSequence());
        $this->assertIsString($documentId0->getAttribute('id'));
        $this->assertEquals($sequence, $documentId0->getAttribute('id'));
    }

    public function testCreateDocuments(): void
    {
        $count = 3;
        $collection = 'testCreateDocuments';

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection($collection);

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'string', type: ColumnType::String, size: 128, required: true)));
        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'integer', type: ColumnType::Integer, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'bigint', type: ColumnType::Integer, size: 8, required: true)));

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
                'bigint' => Database::MAX_BIG_INT,
            ]);
        }

        $results = [];

        $count = $database->createDocuments($collection, $documents, 3, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals($count, \count($results));

        foreach ($results as $document) {
            $this->assertNotEmpty($document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(9223372036854775807, $document->getAttribute('bigint'));
        }

        $documents = $database->find($collection, [
            Query::orderAsc(),
        ]);

        $this->assertEquals($count, \count($documents));

        foreach ($documents as $document) {
            $this->assertNotEmpty($document->getId());
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
        $database = $this->getDatabase();

        $database->createCollection(__FUNCTION__);

        $this->assertEquals(true, $database->createAttribute(__FUNCTION__, new Attribute(key: 'string', type: ColumnType::String, size: 128, required: true)));

        /** @var array<Document> $documents */
        $documents = [];
        $offset = 1000000;
        for ($i = $offset; $i <= ($offset + 10); $i++) {
            $sequence = (string) $i;
            if ($database->getAdapter()->getIdAttributeType() == ColumnType::Uuid7->value) {
                // Replace last 6 digits with $i to make it unique
                $suffix = str_pad(substr((string) $i, -6), 6, '0', STR_PAD_LEFT);
                $sequence = '01890dd5-7331-7f3a-9c1b-123456'.$suffix;
            }

            $hash[$i] = $sequence;

            $documents[] = new Document([
                '$sequence' => $sequence,
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
            Query::orderAsc(),
        ]);

        foreach ($documents as $index => $document) {
            $this->assertEquals($hash[$index + $offset], $document->getSequence());
            $this->assertNotEmpty($document->getId());
            $this->assertEquals('text', $document->getAttribute('string'));
        }
    }

    public function testCreateDocumentsWithDifferentAttributes(): void
    {
        $collection = 'testDiffAttributes';

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection($collection);

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'string', type: ColumnType::String, size: 128, required: true)));
        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'integer', type: ColumnType::Integer, size: 0, required: false)));
        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'bigint', type: ColumnType::Integer, size: 8, required: false)));
        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'string_default', type: ColumnType::String, size: 128, required: false, default: 'default')));

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
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Upserts)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(__FUNCTION__);
        $database->createAttribute(__FUNCTION__, new Attribute(key: 'string', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute(__FUNCTION__, new Attribute(key: 'integer', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute(__FUNCTION__, new Attribute(key: 'bigint', type: ColumnType::Integer, size: 8, required: true));

        $documents = [
            new Document([
                '$id' => 'first',
                'string' => 'text📝',
                'integer' => 5,
                'bigint' => Database::MAX_BIG_INT,
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
                'bigint' => Database::MAX_BIG_INT,
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
            $this->assertNotEmpty($document->getId());
            $this->assertNotEmpty($document->getSequence());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::MAX_BIG_INT, $document->getAttribute('bigint'));
        }

        $documents = $database->find(__FUNCTION__);

        $this->assertEquals(2, count($documents));

        foreach ($documents as $document) {
            $this->assertNotEmpty($document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(5, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::MAX_BIG_INT, $document->getAttribute('bigint'));
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
            $this->assertNotEmpty($document->getId());
            $this->assertNotEmpty($document->getSequence());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('new text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(10, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::MAX_BIG_INT, $document->getAttribute('bigint'));
        }

        $documents = $database->find(__FUNCTION__);

        $this->assertEquals(2, count($documents));

        foreach ($documents as $index => $document) {
            $this->assertEquals($createdAt[$index], $document->getCreatedAt());
            $this->assertNotEmpty($document->getId());
            $this->assertIsString($document->getAttribute('string'));
            $this->assertEquals('new text📝', $document->getAttribute('string')); // Also makes sure an emoji is working
            $this->assertIsInt($document->getAttribute('integer'));
            $this->assertEquals(10, $document->getAttribute('integer'));
            $this->assertIsInt($document->getAttribute('bigint'));
            $this->assertEquals(Database::MAX_BIG_INT, $document->getAttribute('bigint'));
        }
    }

    public function testUpsertDocumentsInc(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Upserts)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(__FUNCTION__);
        $database->createAttribute(__FUNCTION__, new Attribute(key: 'string', type: ColumnType::String, size: 128, required: false));
        $database->createAttribute(__FUNCTION__, new Attribute(key: 'integer', type: ColumnType::Integer, size: 0, required: false));

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
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Upserts)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(__FUNCTION__);
        $database->createAttribute(__FUNCTION__, new Attribute(key: 'string', type: ColumnType::String, size: 128, required: true));

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

    public function testUpsertMixedPermissionDelta(): void
    {
        $db = $this->getDatabase();
        if (! $db->getAdapter()->supports(Capability::Upserts)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $db->createCollection(__FUNCTION__);
        $db->createAttribute(__FUNCTION__, new Attribute(key: 'v', type: ColumnType::Integer, size: 0, required: true));

        $d1 = $db->createDocument(__FUNCTION__, new Document([
            '$id' => 'a',
            'v' => 0,
            '$permissions' => [
                Permission::update(Role::any()),
            ],
        ]));
        $d2 = $db->createDocument(__FUNCTION__, new Document([
            '$id' => 'b',
            'v' => 0,
            '$permissions' => [
                Permission::update(Role::any()),
            ],
        ]));

        // d1 adds write, d2 removes update
        $d1->setAttribute('$permissions', [
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ]);
        $d2->setAttribute('$permissions', [
            Permission::read(Role::any()),
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

    public function testGetDocument(): void
    {
        $document = $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->getDocument('documents', $document->getId());

        $this->assertNotEmpty($document->getId());
        $this->assertIsString($document->getAttribute('string'));
        $this->assertEquals('text📝', $document->getAttribute('string'));
        $this->assertIsInt($document->getAttribute('integer_signed'));
        $this->assertEquals(-Database::MAX_INT, $document->getAttribute('integer_signed'));
        $this->assertIsFloat($document->getAttribute('float_signed'));
        $this->assertEquals(-5.55, $document->getAttribute('float_signed'));
        $this->assertIsFloat($document->getAttribute('float_unsigned'));
        $this->assertEquals(5.55, $document->getAttribute('float_unsigned'));
        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(true, $document->getAttribute('boolean'));
        $this->assertIsArray($document->getAttribute('colors'));
        $this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));
        $this->assertEquals('Works', $document->getAttribute('with-dash'));
    }

    public function testFind(): void
    {
        $this->initMoviesFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->createDocument('movies', new Document(['$id' => ['id_as_array']]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('$id must be of type string', $e->getMessage());
            $this->assertInstanceOf(StructureException::class, $e);
        }
    }

    public function testFindCheckInteger(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

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
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        /**
         * Boolean condition
         */
        $documents = $database->find('movies', [
            Query::equal('active', [true]),
        ]);

        $this->assertEquals(4, count($documents));
    }

    public function testFindFloat(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

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
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::QueryContains)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $documents = $database->find('movies', [
            Query::contains('genres', ['comics']),
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
            $this->assertEquals('Invalid query: Cannot query contains on attribute "price" because it is not an array, string, or object.', $e->getMessage());
            $this->assertTrue($e instanceof DatabaseException);
        }
    }

    public function testFindFulltext(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        /**
         * Fulltext search
         */
        if ($this->getDatabase()->getAdapter()->supports(Capability::Fulltext)) {
            $success = $database->createIndex('movies', new Index(key: 'name', type: IndexType::Fulltext, attributes: ['name']));
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

            if ($this->getDatabase()->getAdapter()->supports(Capability::FulltextWildcard)) {
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
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Fulltext)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'full_text';
        $database->createCollection($collection, permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users()),
        ]);

        $this->assertTrue($database->createAttribute($collection, new Attribute(key: 'ft', type: ColumnType::String, size: 128, required: true)));
        $this->assertTrue($database->createIndex($collection, new Index(key: 'ft-index', type: IndexType::Fulltext, attributes: ['ft'])));

        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'Alf: chapter_4@nasa.com',
        ]));

        $documents = $database->find($collection, [
            Query::search('ft', 'chapter_4'),
        ]);
        $this->assertEquals(1, count($documents));

        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'al@ba.io +-*)(<>~',
        ]));

        $documents = $database->find($collection, [
            Query::search('ft', 'al@ba.io'), // === al ba io*
        ]);

        if ($database->getAdapter()->supports(Capability::FulltextWildcard)) {
            $this->assertEquals(0, count($documents));
        } else {
            $this->assertEquals(1, count($documents));
        }

        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'donald duck',
        ]));

        $database->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'donald trump',
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

    public function testFindByID(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        /**
         * $id condition
         */
        $documents = $database->find('movies', [
            Query::equal('$id', ['frozen']),
        ]);

        $this->assertEquals(1, count($documents));
        $this->assertEquals('Frozen', $documents[0]['name']);
    }

    public function testFindByInternalID(): void
    {
        $data = $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        /**
         * Test that internal ID queries are handled correctly
         */
        $documents = $database->find('movies', [
            Query::equal('$sequence', [$data['$sequence']]),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testOrSingleQuery(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->find('movies', [
                Query::or([
                    Query::equal('active', [true]),
                ]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Invalid query: Or queries require at least two queries', $e->getMessage());
        }
    }

    public function testOrMultipleQueries(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $queries = [
            Query::or([
                Query::equal('active', [true]),
                Query::equal('name', ['Frozen II']),
            ]),
        ];
        $this->assertCount(4, $database->find('movies', $queries));
        $this->assertEquals(4, $database->count('movies', $queries));

        $queries = [
            Query::equal('active', [true]),
            Query::or([
                Query::equal('name', ['Frozen']),
                Query::equal('name', ['Frozen II']),
                Query::equal('director', ['Joe Johnston']),
            ]),
        ];

        $this->assertCount(3, $database->find('movies', $queries));
        $this->assertEquals(3, $database->count('movies', $queries));
    }

    public function testOrNested(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $queries = [
            Query::select(['director']),
            Query::equal('director', ['Joe Johnston']),
            Query::or([
                Query::equal('name', ['Frozen']),
                Query::or([
                    Query::equal('active', [true]),
                    Query::equal('active', [false]),
                ]),
            ]),
        ];

        $documents = $database->find('movies', $queries);
        $this->assertCount(1, $documents);
        $this->assertArrayNotHasKey('name', $documents[0]);

        $count = $database->count('movies', $queries);
        $this->assertEquals(1, $count);
    }

    public function testAndSingleQuery(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->find('movies', [
                Query::and([
                    Query::equal('active', [true]),
                ]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Invalid query: And queries require at least two queries', $e->getMessage());
        }
    }

    public function testAndMultipleQueries(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $queries = [
            Query::and([
                Query::equal('active', [true]),
                Query::equal('name', ['Frozen II']),
            ]),
        ];
        $this->assertCount(1, $database->find('movies', $queries));
        $this->assertEquals(1, $database->count('movies', $queries));
    }

    public function testAndNested(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $queries = [
            Query::or([
                Query::equal('active', [false]),
                Query::and([
                    Query::equal('active', [true]),
                    Query::equal('name', ['Frozen']),
                ]),
            ]),
        ];

        $documents = $database->find('movies', $queries);
        $this->assertCount(3, $documents);

        $count = $database->count('movies', $queries);
        $this->assertEquals(3, $count);
    }

    public function testFindNull(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $documents = $database->find('movies', [
            Query::isNull('nullable'),
        ]);

        $this->assertEquals(5, count($documents));
    }

    public function testFindNotNull(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $documents = $database->find('movies', [
            Query::isNotNull('nullable'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindStartsWith(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

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
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $documents = $database->find('movies', [
            Query::startsWith('name', 'Work in Progress'),
        ]);

        $this->assertEquals(2, count($documents));
    }

    public function testFindEndsWith(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $documents = $database->find('movies', [
            Query::endsWith('name', 'Marvel'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindNotContains(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::QueryContains)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Test notContains with array attributes - should return documents that don't contain specified genres
        $documents = $database->find('movies', [
            Query::notContains('genres', ['comics']),
        ]);

        $this->assertEquals(4, count($documents)); // 6 readable movies (user:x role added earlier) minus 2 with 'comics' genre

        // Test notContains with multiple values (AND logic - exclude documents containing ANY of these)
        $documents = $database->find('movies', [
            Query::notContains('genres', ['comics', 'kids']),
        ]);

        $this->assertEquals(2, count($documents)); // Only 'Work in Progress' and 'Work in Progress 2' have neither 'comics' nor 'kids'

        // Test notContains with non-existent genre - should return all readable documents
        $documents = $database->find('movies', [
            Query::notContains('genres', ['non-existent']),
        ]);

        $this->assertEquals(6, count($documents));

        // Test notContains with string attribute (substring search)
        $documents = $database->find('movies', [
            Query::notContains('name', ['Captain']),
        ]);
        $this->assertEquals(4, count($documents)); // 6 readable movies minus 2 containing 'Captain'

        // Test notContains combined with other queries (AND logic)
        $documents = $database->find('movies', [
            Query::notContains('genres', ['comics']),
            Query::greaterThan('year', 2000),
        ]);
        $this->assertLessThanOrEqual(4, count($documents)); // Subset of readable movies without 'comics' and after 2000

        // Test notContains with case sensitivity
        $documents = $database->find('movies', [
            Query::notContains('genres', ['COMICS']), // Different case
        ]);
        $this->assertEquals(6, count($documents)); // All readable movies since case doesn't match

        // Test error handling for invalid attribute type
        try {
            $database->find('movies', [
                Query::notContains('price', [10.5]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid query: Cannot query notContains on attribute "price" because it is not an array, string, or object.', $e->getMessage());
            $this->assertTrue($e instanceof DatabaseException);
        }
    }

    public function testFindNotSearch(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        // Only test if fulltext search is supported
        if ($this->getDatabase()->getAdapter()->supports(Capability::Fulltext)) {
            // Ensure fulltext index exists (may already exist from previous tests)
            try {
                $database->createIndex('movies', new Index(key: 'name', type: IndexType::Fulltext, attributes: ['name']));
            } catch (Throwable $e) {
                // Index may already exist, ignore duplicate error
                if (! str_contains($e->getMessage(), 'already exists')) {
                    throw $e;
                }
            }

            // Test notSearch - should return documents that don't match the search term
            $documents = $database->find('movies', [
                Query::notSearch('name', 'captain'),
            ]);

            $this->assertEquals(4, count($documents)); // 6 readable movies (user:x role added earlier) minus 2 with 'captain' in name

            // Test notSearch with term that doesn't exist - should return all readable documents
            $documents = $database->find('movies', [
                Query::notSearch('name', 'nonexistent'),
            ]);

            $this->assertEquals(6, count($documents));

            // Test notSearch with partial term
            if ($this->getDatabase()->getAdapter()->supports(Capability::FulltextWildcard)) {
                $documents = $database->find('movies', [
                    Query::notSearch('name', 'cap'),
                ]);

                $this->assertEquals(4, count($documents)); // 6 readable movies minus 2 matching 'cap*'
            }

            // Test notSearch with empty string - should return all readable documents
            $documents = $database->find('movies', [
                Query::notSearch('name', ''),
            ]);
            $this->assertEquals(6, count($documents)); // All readable movies since empty search matches nothing

            // Test notSearch combined with other filters
            $documents = $database->find('movies', [
                Query::notSearch('name', 'captain'),
                Query::lessThan('year', 2010),
            ]);
            $this->assertLessThanOrEqual(4, count($documents)); // Subset of non-captain movies before 2010

            // Test notSearch with special characters
            $documents = $database->find('movies', [
                Query::notSearch('name', '@#$%'),
            ]);
            $this->assertEquals(6, count($documents)); // All readable movies since special chars don't match
        }

        $this->assertEquals(true, true); // Test must do an assertion
    }

    public function testFindNotStartsWith(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

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
            Query::equal('year', [2006]),
        ]);
        $this->assertLessThanOrEqual(4, count($documents)); // Subset of non-Work movies from 2006
    }

    public function testFindNotEndsWith(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

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
            Query::limit(3),
        ]);
        $this->assertEquals(3, count($documents)); // Limited to 3 results
        $this->assertLessThanOrEqual(5, count($documents)); // But still excluding Marvel movies
    }

    public function testFindOrderRandom(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::OrderRandom)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Test orderRandom with default limit
        $documents = $database->find('movies', [
            Query::orderRandom(),
            Query::limit(1),
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertNotEmpty($documents[0]['name']); // Ensure we got a valid document

        // Test orderRandom with multiple documents
        $documents = $database->find('movies', [
            Query::orderRandom(),
            Query::limit(3),
        ]);
        $this->assertEquals(3, count($documents));

        // Test that orderRandom returns different results (not guaranteed but highly likely)
        $firstSet = $database->find('movies', [
            Query::orderRandom(),
            Query::limit(3),
        ]);
        $secondSet = $database->find('movies', [
            Query::orderRandom(),
            Query::limit(3),
        ]);

        // Extract IDs for comparison
        $firstIds = array_map(fn ($doc) => $doc['$id'], $firstSet);
        $secondIds = array_map(fn ($doc) => $doc['$id'], $secondSet);

        // While not guaranteed to be different, with 6 movies and selecting 3,
        // the probability of getting the same set in the same order is very low
        // We'll just check that we got valid results
        $this->assertEquals(3, count($firstIds));
        $this->assertEquals(3, count($secondIds));

        // Test orderRandom with more than available documents
        $documents = $database->find('movies', [
            Query::orderRandom(),
            Query::limit(10), // We only have 6 movies
        ]);
        $this->assertLessThanOrEqual(6, count($documents)); // Should return all available documents

        // Test orderRandom with filters
        $documents = $database->find('movies', [
            Query::greaterThan('price', 10),
            Query::orderRandom(),
            Query::limit(2),
        ]);
        $this->assertLessThanOrEqual(2, count($documents));
        foreach ($documents as $document) {
            $this->assertGreaterThan(10, $document['price']);
        }

        // Test orderRandom without explicit limit (should use default)
        $documents = $database->find('movies', [
            Query::orderRandom(),
        ]);
        $this->assertGreaterThan(0, count($documents));
        $this->assertLessThanOrEqual(25, count($documents)); // Default limit is 25
    }

    public function testSum(): void
    {
        $this->initMoviesFixture();
        /** @var Database $database */
        $database = $this->getDatabase();

        $this->getDatabase()->getAuthorization()->addRole('user:x');

        $sum = $database->sum('movies', 'year', [Query::equal('year', [2019])]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = $database->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025 + 2026, $sum);
        $sum = $database->sum('movies', 'price', [Query::equal('year', [2019])]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = $database->sum('movies', 'price', [Query::equal('year', [2019])]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));

        $sum = $database->sum('movies', 'year', [Query::equal('year', [2019])], 1);
        $this->assertEquals(2019, $sum);

        $this->getDatabase()->getAuthorization()->removeRole('user:x');

        $sum = $database->sum('movies', 'year', [Query::equal('year', [2019])]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = $database->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025, $sum);
        $sum = $database->sum('movies', 'price', [Query::equal('year', [2019])]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = $database->sum('movies', 'price', [Query::equal('year', [2019])]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
    }

    public function testUpdateDocument(): void
    {
        $document = $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();
        $document = $database->getDocument('documents', $document->getId());

        $document
            ->setAttribute('string', 'text📝 updated')
            ->setAttribute('integer_signed', -6)
            ->setAttribute('integer_unsigned', 6)
            ->setAttribute('float_signed', -5.56)
            ->setAttribute('float_unsigned', 5.56)
            ->setAttribute('boolean', false)
            ->setAttribute('colors', 'red', SetType::Append)
            ->setAttribute('with-dash', 'Works');

        $new = $this->getDatabase()->updateDocument($document->getCollection(), $document->getId(), $document);

        $this->assertNotEmpty($new->getId());
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
            ->setAttribute('$permissions', Permission::read(Role::guests()), SetType::Append)
            ->setAttribute('$permissions', Permission::create(Role::guests()), SetType::Append)
            ->setAttribute('$permissions', Permission::update(Role::guests()), SetType::Append)
            ->setAttribute('$permissions', Permission::delete(Role::guests()), SetType::Append);

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
    }

    public function testDeleteDocument(): void
    {
        $document = $this->initDocumentsFixture();
        $result = $this->getDatabase()->deleteDocument($document->getCollection(), $document->getId());
        $document = $this->getDatabase()->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $result);
        $this->assertEquals(true, $document->isEmpty());
    }

    public function testUpdateDocuments(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::BatchOperations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'testUpdateDocuments';
        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        $database->createCollection($collection, attributes: [
            new Attribute(key: 'string', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
            new Attribute(key: 'integer', type: ColumnType::Integer, size: 10000, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ], documentSecurity: false);

        for ($i = 0; $i < 10; $i++) {
            $database->createDocument($collection, new Document([
                '$id' => 'doc'.$i,
                'string' => 'text📝 '.$i,
                'integer' => $i,
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

        $this->getDatabase()->getAuthorization()->skip(function () use ($collection, $database) {
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

        $this->getDatabase()->getAuthorization()->addRole(Role::user('asd')->toString());

        $database->updateDocuments($collection, new Document([
            'string' => 'permission text',
        ]));

        $documents = $database->find($collection, [
            Query::equal('string', ['permission text']),
        ]);

        $this->assertCount(1, $documents);

        $this->getDatabase()->getAuthorization()->skip(function () use ($collection, $database) {
            $unmodifiedDocuments = $database->find($collection, [
                Query::equal('string', ['text📝 updated all']),
            ]);

            $this->assertCount(9, $unmodifiedDocuments);
        });

        $this->getDatabase()->getAuthorization()->skip(function () use ($collection, $database) {
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
            'string' => 'batchSize Test',
        ]), batchSize: 2));

        $documents = $database->find($collection);

        foreach ($documents as $document) {
            $this->assertEquals('batchSize Test', $document->getAttribute('string'));
        }

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
    }

    public function testUpdateDocumentsWithCallbackSupport(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::BatchOperations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'update_callback';
        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        $database->createCollection($collection, attributes: [
            new Attribute(key: 'string', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
            new Attribute(key: 'integer', type: ColumnType::Integer, size: 10000, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ], documentSecurity: false);

        for ($i = 0; $i < 10; $i++) {
            $database->createDocument($collection, new Document([
                '$id' => 'doc'.$i,
                'string' => 'text📝 '.$i,
                'integer' => $i,
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
        }, onError: function ($e) {
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

    public function testReadPermissionsSuccess(): void
    {
        $this->initDocumentsFixture();
        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::MAX_INT,
            'integer_unsigned' => Database::MAX_INT,
            'bigint_signed' => -Database::MAX_BIG_INT,
            'bigint_unsigned' => Database::MAX_BIG_INT,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $this->assertEquals(false, $document->isEmpty());

        $this->getDatabase()->getAuthorization()->cleanRoles();

        $document = $database->getDocument($document->getCollection(), $document->getId());
        $this->assertEquals(true, $document->isEmpty());

        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
    }

    public function testWritePermissionsSuccess(): void
    {
        $this->initDocumentsFixture();
        $this->getDatabase()->getAuthorization()->cleanRoles();

        /** @var Database $database */
        $database = $this->getDatabase();

        $this->expectException(AuthorizationException::class);
        $database->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::MAX_INT,
            'integer_unsigned' => Database::MAX_INT,
            'bigint_signed' => -Database::MAX_BIG_INT,
            'bigint_unsigned' => Database::MAX_BIG_INT,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));
    }

    public function testWritePermissionsUpdateFailure(): void
    {
        $this->initDocumentsFixture();
        $this->expectException(AuthorizationException::class);

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::MAX_INT,
            'integer_unsigned' => Database::MAX_INT,
            'bigint_signed' => -Database::MAX_BIG_INT,
            'bigint_unsigned' => Database::MAX_BIG_INT,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $this->getDatabase()->getAuthorization()->cleanRoles();

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
            'bigint_signed' => -Database::MAX_BIG_INT,
            'float_signed' => -Database::MAX_DOUBLE,
            'float_unsigned' => Database::MAX_DOUBLE,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

    }

    public function testUniqueIndexDuplicate(): void
    {
        $this->initMoviesFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $this->assertEquals(true, $database->createIndex('movies', new Index(key: 'uniqueIndex', type: IndexType::Unique, attributes: ['name'], lengths: [128], orders: [OrderDirection::Asc->value])));

        try {
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
                'with-dash' => 'Works4',
            ]));

            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }
    }

    public function testUniqueIndexDuplicateUpdate(): void
    {
        $this->initMoviesFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        // Ensure the unique index exists (created in testUniqueIndexDuplicate)
        try {
            $database->createIndex('movies', new Index(key: 'uniqueIndex', type: IndexType::Unique, attributes: ['name'], lengths: [128], orders: [OrderDirection::Asc->value]));
        } catch (\Throwable) {
            // Index may already exist
        }

        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());
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
            'with-dash' => 'Works4',
        ]));

        try {
            $database->updateDocument('movies', $document->getId(), $document->setAttribute('name', 'Frozen'));

            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }
    }

    public function propagateBulkDocuments(string $collection, int $amount = 10, bool $documentSecurity = false): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        for ($i = 0; $i < $amount; $i++) {
            $database->createDocument($collection, new Document(
                array_merge([
                    '$id' => 'doc'.$i,
                    'text' => 'value'.$i,
                    'integer' => $i,
                ], $documentSecurity ? [
                    '$permissions' => [
                        Permission::create(Role::any()),
                        Permission::read(Role::any()),
                    ],
                ] : [])
            ));
        }
    }

    public function testFulltextIndexWithInteger(): void
    {
        $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectException(Exception::class);
            if (! $this->getDatabase()->getAdapter()->supports(Capability::Fulltext)) {
                $this->expectExceptionMessage('Fulltext index is not supported');
            } else {
                $this->expectExceptionMessage('Attribute "integer_signed" cannot be part of a fulltext index, must be of type string');
            }

            $database->createIndex('documents', new Index(key: 'fulltext_integer', type: IndexType::Fulltext, attributes: ['string', 'integer_signed']));
        } else {
            $this->expectNotToPerformAssertions();

            return;
        }
    }

    public function testEnableDisableValidation(): void
    {
        $database = $this->getDatabase();

        $database->createCollection('validation', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createAttribute('validation', new Attribute(key: 'name', type: ColumnType::String, size: 10, required: false));

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

    public function testExceptionDuplicate(): void
    {
        $document = $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $document->setAttribute('$id', 'duplicated');
        $document->removeAttribute('$sequence');

        $database->createDocument($document->getCollection(), $document);
        $document->removeAttribute('$sequence');

        try {
            $database->createDocument($document->getCollection(), $document);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }
    }

    public function testExceptionCaseInsensitiveDuplicate(): void
    {
        $document = $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $document->setAttribute('$id', 'caseSensitive');
        $document->removeAttribute('$sequence');

        $database->createDocument($document->getCollection(), $document);

        $document->setAttribute('$id', 'CaseSensitive');
        $document->removeAttribute('$sequence');

        try {
            $database->createDocument($document->getCollection(), $document);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }
    }

    public function testEmptyTenant(): void
    {
        $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

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

        $doc = $database->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => 'tenant_test',
            'integer_signed' => 1,
            'integer_unsigned' => 1,
            'bigint_signed' => 1,
            'bigint_unsigned' => 1,
            'float_signed' => 1.0,
            'float_unsigned' => 1.0,
            'boolean' => true,
            'colors' => ['red'],
            'empty' => [],
            'with-dash' => 'test',
        ]));

        $this->assertArrayHasKey('$id', $doc);
        $this->assertArrayNotHasKey('$tenant', $doc);

        $document = $database->getDocument('documents', $doc->getId());
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);

        $document = $database->updateDocument('documents', $document->getId(), $document);
        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$tenant', $document);

        $database->deleteDocument('documents', $document->getId());
    }

    public function testDateTimeDocument(): void
    {
        /**
         * @var Database $database
         */
        $database = $this->getDatabase();
        $collection = 'create_modify_dates';
        $database->createCollection($collection);
        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'string', type: ColumnType::String, size: 128, required: false)));
        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'datetime', type: ColumnType::Datetime, size: 0, required: false, default: null, signed: true, array: false, format: null, formatOptions: [], filters: ['datetime'])));

        $date = '2000-01-01T10:00:00.000+00:00';
        // test - default behaviour of external datetime attribute not changed
        $doc = $database->createDocument($collection, new Document([
            '$id' => 'doc1',
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()), Permission::update(Role::any())],
            'datetime' => '',
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
            '$permissions' => [Permission::read(Role::any()), Permission::write(Role::any()), Permission::update(Role::any())],
            '$createdAt' => $date,
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

    public function testUpsertDateOperations(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Upserts)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'upsert_date_operations';
        $database->createCollection($collection);
        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'string', type: ColumnType::String, size: 128, required: false)));

        $database->setPreserveDates(true);

        $createDate = '2000-01-01T10:00:00.000+00:00';
        $updateDate = '2000-02-01T15:30:00.000+00:00';
        $date1 = '2000-01-01T10:00:00.000+00:00';
        $date2 = '2000-02-01T15:30:00.000+00:00';
        $date3 = '2000-03-01T20:45:00.000+00:00';
        $permissions = [Permission::read(Role::any()), Permission::write(Role::any()), Permission::update(Role::any())];

        // Test 1: Upsert new document with custom createdAt
        $upsertResults = [];
        $database->upsertDocuments($collection, [
            new Document([
                '$id' => 'upsert1',
                '$permissions' => $permissions,
                'string' => 'upsert1_initial',
                '$createdAt' => $createDate,
            ]),
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
                '$updatedAt' => $updateDate,
            ]),
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
                '$updatedAt' => $customDate,
            ]),
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
                '$createdAt' => $createDate,
            ]),
            new Document([
                '$id' => 'bulk_upsert2',
                '$permissions' => $permissions,
                'string' => 'bulk_upsert2_initial',
                '$updatedAt' => $updateDate,
            ]),
            new Document([
                '$id' => 'bulk_upsert3',
                '$permissions' => $permissions,
                'string' => 'bulk_upsert3_initial',
                '$createdAt' => $createDate,
                '$updatedAt' => $updateDate,
            ]),
            new Document([
                '$id' => 'bulk_upsert4',
                '$permissions' => $permissions,
                'string' => 'bulk_upsert4_initial',
            ]),
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
            '$updatedAt' => $newDate,
        ]);

        $upsertIds = [];
        foreach ($upsertDocuments as $doc) {
            $upsertIds[] = $doc->getId();
        }

        $database->updateDocuments($collection, $updateUpsertDoc, [
            Query::equal('$id', $upsertIds),
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
            '$updatedAt' => null,
        ]);

        $upsertIds = [];
        foreach ($upsertDocuments as $doc) {
            $upsertIds[] = $doc->getId();
        }

        $database->updateDocuments($collection, $updateUpsertDoc, [
            Query::equal('$id', $upsertIds),
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
            $this->assertEquals($newDate, $doc->getAttribute('$createdAt'), 'createdAt mismatch for upsert update');
            $this->assertEquals($newDate, $doc->getAttribute('$updatedAt'), 'updatedAt mismatch for upsert update');
            $this->assertEquals('bulk_upsert_updated_via_upsert', $doc->getAttribute('string'), 'string mismatch for upsert update');
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
            $this->assertNotEquals($customDate, $doc->getAttribute('$createdAt'), 'createdAt should not be custom date when disabled');
            $this->assertNotEquals($customDate, $doc->getAttribute('$updatedAt'), 'updatedAt should not be custom date when disabled');
            $this->assertEquals('bulk_upsert_disabled', $doc->getAttribute('string'), 'string mismatch for disabled upsert');
        }

        $database->setPreserveDates(false);
        $database->deleteCollection($collection);
    }

    public function testUpdateDocumentsCount(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Upserts)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collectionName = 'update_count';
        $database->createCollection($collectionName);

        $database->createAttribute($collectionName, new Attribute(key: 'key', type: ColumnType::String, size: 60, required: false));
        $database->createAttribute($collectionName, new Attribute(key: 'value', type: ColumnType::String, size: 60, required: false));

        $permissions = [Permission::read(Role::any()), Permission::write(Role::any()), Permission::update(Role::any())];

        $docs = [
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
                'key' => 'bulk_upsert4_initial',
            ]),
        ];
        $upsertUpdateResults = [];
        $count = $database->upsertDocuments($collectionName, $docs, onNext: function ($doc) use (&$upsertUpdateResults) {
            $upsertUpdateResults[] = $doc;
        });
        $this->assertCount(4, $upsertUpdateResults);
        $this->assertEquals(4, $count);

        $updates = new Document(['value' => 'test']);
        $newDocs = [];
        $count = $database->updateDocuments($collectionName, $updates, onNext: function ($doc) use (&$newDocs) {
            $newDocs[] = $doc;
        });

        $this->assertCount(4, $newDocs);
        $this->assertEquals(4, $count);

        $database->deleteCollection($collectionName);
    }

    public function testUpsertWithJSONFilters(): void
    {
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create collection with JSON filter attribute
        $collection = ID::unique();
        $database->createCollection($collection, permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createAttribute($collection, new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute($collection, new Attribute(key: 'metadata', type: ColumnType::String, size: 4000, required: true, filters: ['json']));

        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        // Test 1: Insertion (createDocument) with JSON filter
        $docId1 = 'json-doc-1';
        $initialMetadata = [
            'version' => '1.0.0',
            'tags' => ['php', 'database'],
            'config' => [
                'debug' => false,
                'timeout' => 30,
            ],
        ];

        $document1 = $database->createDocument($collection, new Document([
            '$id' => $docId1,
            'name' => 'Initial Document',
            'metadata' => $initialMetadata,
            '$permissions' => $permissions,
        ]));

        $this->assertEquals($docId1, $document1->getId());
        $this->assertEquals('Initial Document', $document1->getAttribute('name'));
        $this->assertIsArray($document1->getAttribute('metadata'));
        $this->assertEquals('1.0.0', $document1->getAttribute('metadata')['version']);
        $this->assertEquals(['php', 'database'], $document1->getAttribute('metadata')['tags']);

        // Test 2: Update (updateDocument) with modified JSON filter
        $updatedMetadata = [
            'version' => '2.0.0',
            'tags' => ['php', 'database', 'json'],
            'config' => [
                'debug' => true,
                'timeout' => 60,
                'cache' => true,
            ],
            'updated' => true,
        ];

        $document1->setAttribute('name', 'Updated Document');
        $document1->setAttribute('metadata', $updatedMetadata);

        $updatedDoc = $database->updateDocument($collection, $docId1, $document1);

        $this->assertEquals($docId1, $updatedDoc->getId());
        $this->assertEquals('Updated Document', $updatedDoc->getAttribute('name'));
        $this->assertIsArray($updatedDoc->getAttribute('metadata'));
        $this->assertEquals('2.0.0', $updatedDoc->getAttribute('metadata')['version']);
        $this->assertEquals(['php', 'database', 'json'], $updatedDoc->getAttribute('metadata')['tags']);
        $this->assertTrue($updatedDoc->getAttribute('metadata')['config']['debug']);
        $this->assertTrue($updatedDoc->getAttribute('metadata')['updated']);

        // Test 3: Upsert - Create new document (upsertDocument)
        $docId2 = 'json-doc-2';
        $newMetadata = [
            'version' => '1.5.0',
            'tags' => ['javascript', 'node'],
            'config' => [
                'debug' => false,
                'timeout' => 45,
            ],
        ];

        $document2 = new Document([
            '$id' => $docId2,
            'name' => 'New Upsert Document',
            'metadata' => $newMetadata,
            '$permissions' => $permissions,
        ]);

        $upsertedDoc = $database->upsertDocument($collection, $document2);

        $this->assertEquals($docId2, $upsertedDoc->getId());
        $this->assertEquals('New Upsert Document', $upsertedDoc->getAttribute('name'));
        $this->assertIsArray($upsertedDoc->getAttribute('metadata'));
        $this->assertEquals('1.5.0', $upsertedDoc->getAttribute('metadata')['version']);

        // Test 4: Upsert - Update existing document (upsertDocument)
        $document2->setAttribute('name', 'Updated Upsert Document');
        $document2->setAttribute('metadata', [
            'version' => '2.5.0',
            'tags' => ['javascript', 'node', 'typescript'],
            'config' => [
                'debug' => true,
                'timeout' => 90,
            ],
            'migrated' => true,
        ]);

        $upsertedDoc2 = $database->upsertDocument($collection, $document2);

        $this->assertEquals($docId2, $upsertedDoc2->getId());
        $this->assertEquals('Updated Upsert Document', $upsertedDoc2->getAttribute('name'));
        $this->assertIsArray($upsertedDoc2->getAttribute('metadata'));
        $this->assertEquals('2.5.0', $upsertedDoc2->getAttribute('metadata')['version']);
        $this->assertEquals(['javascript', 'node', 'typescript'], $upsertedDoc2->getAttribute('metadata')['tags']);
        $this->assertTrue($upsertedDoc2->getAttribute('metadata')['migrated']);

        // Test 5: Upsert - Bulk upsertDocuments (create and update)
        $docId3 = 'json-doc-3';
        $docId4 = 'json-doc-4';

        $bulkDocuments = [
            new Document([
                '$id' => $docId3,
                'name' => 'Bulk Upsert 1',
                'metadata' => [
                    'version' => '3.0.0',
                    'tags' => ['python', 'flask'],
                    'config' => ['debug' => false],
                ],
                '$permissions' => $permissions,
            ]),
            new Document([
                '$id' => $docId4,
                'name' => 'Bulk Upsert 2',
                'metadata' => [
                    'version' => '3.1.0',
                    'tags' => ['go', 'golang'],
                    'config' => ['debug' => true],
                ],
                '$permissions' => $permissions,
            ]),
            // Update existing document
            new Document([
                '$id' => $docId1,
                'name' => 'Bulk Updated Document',
                'metadata' => [
                    'version' => '3.0.0',
                    'tags' => ['php', 'database', 'bulk'],
                    'config' => [
                        'debug' => false,
                        'timeout' => 120,
                    ],
                    'bulkUpdated' => true,
                ],
                '$permissions' => $permissions,
            ]),
        ];

        $count = $database->upsertDocuments($collection, $bulkDocuments);
        $this->assertEquals(3, $count);

        // Verify bulk upsert results
        $bulkDoc1 = $database->getDocument($collection, $docId3);
        $this->assertEquals('Bulk Upsert 1', $bulkDoc1->getAttribute('name'));
        $this->assertEquals('3.0.0', $bulkDoc1->getAttribute('metadata')['version']);

        $bulkDoc2 = $database->getDocument($collection, $docId4);
        $this->assertEquals('Bulk Upsert 2', $bulkDoc2->getAttribute('name'));
        $this->assertEquals('3.1.0', $bulkDoc2->getAttribute('metadata')['version']);

        $bulkDoc3 = $database->getDocument($collection, $docId1);
        $this->assertEquals('Bulk Updated Document', $bulkDoc3->getAttribute('name'));
        $this->assertEquals('3.0.0', $bulkDoc3->getAttribute('metadata')['version']);
        $this->assertTrue($bulkDoc3->getAttribute('metadata')['bulkUpdated']);

        // Cleanup
        $database->deleteCollection($collection);
    }

    public function testFindRegex(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Skip test if regex is not supported
        if (! $database->getAdapter()->supports(Capability::Regex)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Determine regex support type
        $supportsPCRE = $database->getAdapter()->supports(Capability::PCRE);
        $supportsPOSIX = $database->getAdapter()->supports(Capability::POSIX);

        // Determine word boundary pattern based on support
        $wordBoundaryPattern = null;
        $wordBoundaryPatternPHP = null;
        if ($supportsPCRE) {
            $wordBoundaryPattern = '\\b'; // PCRE uses \b
            $wordBoundaryPatternPHP = '\\b'; // PHP preg_match uses \b
        } elseif ($supportsPOSIX) {
            $wordBoundaryPattern = '\\y'; // POSIX uses \y
            $wordBoundaryPatternPHP = '\\b'; // PHP preg_match still uses \b for verification
        }

        $database->createCollection('moviesRegex', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->assertEquals(true, $database->createAttribute('moviesRegex', new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true)));
            $this->assertEquals(true, $database->createAttribute('moviesRegex', new Attribute(key: 'director', type: ColumnType::String, size: 128, required: true)));
            $this->assertEquals(true, $database->createAttribute('moviesRegex', new Attribute(key: 'year', type: ColumnType::Integer, size: 0, required: true)));
        }

        if ($database->getAdapter()->supports(Capability::TrigramIndex)) {
            $database->createIndex('moviesRegex', new Index(key: 'trigram_name', type: IndexType::Trigram, attributes: ['name']));
            $database->createIndex('moviesRegex', new Index(key: 'trigram_director', type: IndexType::Trigram, attributes: ['director']));
        }

        // Create test documents
        $database->createDocuments('moviesRegex', [
            new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Frozen',
                'director' => 'Chris Buck & Jennifer Lee',
                'year' => 2013,
            ]),
            new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Frozen II',
                'director' => 'Chris Buck & Jennifer Lee',
                'year' => 2019,
            ]),
            new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Captain America: The First Avenger',
                'director' => 'Joe Johnston',
                'year' => 2011,
            ]),
            new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Captain Marvel',
                'director' => 'Anna Boden & Ryan Fleck',
                'year' => 2019,
            ]),
            new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Work in Progress',
                'director' => 'TBD',
                'year' => 2025,
            ]),
            new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Work in Progress 2',
                'director' => 'TBD',
                'year' => 2026,
            ]),
        ]);

        // Helper function to verify regex query completeness
        $verifyRegexQuery = function (string $attribute, string $regexPattern, array $queryResults) use ($database) {
            // Convert database regex pattern to PHP regex format.
            // POSIX-style word boundary (\y) is not supported by PHP PCRE, so map it to \b.
            $normalizedPattern = str_replace('\y', '\b', $regexPattern);
            $phpPattern = '/'.str_replace('/', '\/', $normalizedPattern).'/';

            // Get all documents to manually verify
            $allDocuments = $database->find('moviesRegex');

            // Manually filter documents that match the pattern
            $expectedMatches = [];
            foreach ($allDocuments as $doc) {
                $value = $doc->getAttribute($attribute);
                if (preg_match($phpPattern, $value)) {
                    $expectedMatches[] = $doc->getId();
                }
            }

            // Get IDs from query results
            $actualMatches = array_map(fn ($doc) => $doc->getId(), $queryResults);

            // Verify no extra documents are returned
            foreach ($queryResults as $doc) {
                $value = $doc->getAttribute($attribute);
                $this->assertTrue(
                    (bool) preg_match($phpPattern, $value),
                    "Document '{$doc->getId()}' with {$attribute}='{$value}' should match pattern '{$regexPattern}'"
                );
            }

            // Verify all expected documents are returned (no missing)
            sort($expectedMatches);
            sort($actualMatches);
            $this->assertEquals(
                $expectedMatches,
                $actualMatches,
                "Query should return exactly the documents matching pattern '{$regexPattern}' on attribute '{$attribute}'"
            );
        };

        // Test basic regex pattern - match movies starting with 'Captain'
        // Note: Pattern format may vary by adapter (MongoDB uses regex strings, SQL uses REGEXP)
        $pattern = '/^Captain/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', '^Captain'),
        ]);

        // Verify completeness: all matching documents returned, no extra documents
        $verifyRegexQuery('name', '^Captain', $documents);

        // Verify expected documents are included
        $names = array_map(fn ($doc) => $doc->getAttribute('name'), $documents);
        $this->assertTrue(in_array('Captain America: The First Avenger', $names));
        $this->assertTrue(in_array('Captain Marvel', $names));

        // Test regex pattern - match movies containing 'Frozen'
        $pattern = '/Frozen/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', 'Frozen'),
        ]);

        // Verify completeness: all matching documents returned, no extra documents
        $verifyRegexQuery('name', 'Frozen', $documents);

        // Test regex pattern - match exact title 'Frozen'
        $exactFrozenDocuments = $database->find('moviesRegex', [
            Query::regex('name', '^Frozen$'),
        ]);
        $verifyRegexQuery('name', '^Frozen$', $exactFrozenDocuments);
        $this->assertCount(1, $exactFrozenDocuments, 'Exact ^Frozen$ regex should return only one document');
        // Verify expected documents are included
        $names = array_map(fn ($doc) => $doc->getAttribute('name'), $documents);
        $this->assertTrue(in_array('Frozen', $names));
        $this->assertTrue(in_array('Frozen II', $names));

        // Test regex pattern - match movies ending with 'Marvel'
        $pattern = '/Marvel$/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', 'Marvel$'),
        ]);

        // Verify completeness: all matching documents returned, no extra documents
        $verifyRegexQuery('name', 'Marvel$', $documents);

        $this->assertEquals(1, count($documents)); // Only Captain Marvel
        $this->assertEquals('Captain Marvel', $documents[0]->getAttribute('name'));

        // Test regex pattern - match movies with 'Work' in the name
        $pattern = '/.*Work.*/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', '.*Work.*'),
        ]);

        // Verify completeness: all matching documents returned, no extra documents
        $verifyRegexQuery('name', '.*Work.*', $documents);

        // Verify expected documents are included
        $names = array_map(fn ($doc) => $doc->getAttribute('name'), $documents);
        $this->assertTrue(in_array('Work in Progress', $names));
        $this->assertTrue(in_array('Work in Progress 2', $names));

        // Test regex pattern - match movies with 'Buck' in director
        $pattern = '/.*Buck.*/';
        $documents = $database->find('moviesRegex', [
            Query::regex('director', '.*Buck.*'),
        ]);

        // Verify completeness: all matching documents returned, no extra documents
        $verifyRegexQuery('director', '.*Buck.*', $documents);

        // Verify expected documents are included
        $names = array_map(fn ($doc) => $doc->getAttribute('name'), $documents);
        $this->assertTrue(in_array('Frozen', $names));
        $this->assertTrue(in_array('Frozen II', $names));

        // Test regex with case pattern - adapters may be case-sensitive or case-insensitive
        // MySQL/MariaDB REGEXP is case-insensitive by default, MongoDB is case-sensitive
        $patternCaseSensitive = '/captain/';
        $patternCaseInsensitive = '/captain/i';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', 'captain'), // lowercase
        ]);

        // Verify all returned documents match the pattern (case-insensitive check for verification)
        foreach ($documents as $doc) {
            $name = $doc->getAttribute('name');
            // Verify that returned documents contain 'captain' (case-insensitive check)
            $this->assertTrue(
                (bool) preg_match($patternCaseInsensitive, $name),
                "Document '{$name}' should match pattern 'captain' (case-insensitive check)"
            );
        }

        // Verify completeness: Check what the database actually returns
        // Some adapters (MongoDB) are case-sensitive, others (MySQL/MariaDB) are case-insensitive
        // We'll determine expected matches based on case-sensitive matching (pure regex behavior)
        // If the adapter is case-insensitive, it will return more documents, which is fine
        $allDocuments = $database->find('moviesRegex');
        $expectedMatchesCaseSensitive = [];
        $expectedMatchesCaseInsensitive = [];
        foreach ($allDocuments as $doc) {
            $name = $doc->getAttribute('name');
            if (preg_match($patternCaseSensitive, $name)) {
                $expectedMatchesCaseSensitive[] = $doc->getId();
            }
            if (preg_match($patternCaseInsensitive, $name)) {
                $expectedMatchesCaseInsensitive[] = $doc->getId();
            }
        }

        $actualMatches = array_map(fn ($doc) => $doc->getId(), $documents);
        sort($actualMatches);

        // The database might be case-sensitive (MongoDB) or case-insensitive (MySQL/MariaDB)
        // Check which one matches the actual results
        sort($expectedMatchesCaseSensitive);
        sort($expectedMatchesCaseInsensitive);

        // Verify that actual results match either case-sensitive or case-insensitive expectations
        $matchesCaseSensitive = ($expectedMatchesCaseSensitive === $actualMatches);
        $matchesCaseInsensitive = ($expectedMatchesCaseInsensitive === $actualMatches);

        $this->assertTrue(
            $matchesCaseSensitive || $matchesCaseInsensitive,
            'Query results should match either case-sensitive ('.count($expectedMatchesCaseSensitive).' docs) or case-insensitive ('.count($expectedMatchesCaseInsensitive).' docs) expectations. Got '.count($actualMatches).' documents.'
        );

        // Test regex with case-insensitive pattern (if adapter supports it via flags)
        // Test with uppercase to verify case sensitivity
        $pattern = '/Captain/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', 'Captain'), // uppercase
        ]);

        // Verify all returned documents match the pattern
        foreach ($documents as $doc) {
            $name = $doc->getAttribute('name');
            $this->assertTrue(
                (bool) preg_match($pattern, $name),
                "Document '{$name}' should match pattern 'Captain'"
            );
        }

        // Verify completeness
        $allDocuments = $database->find('moviesRegex');
        $expectedMatches = [];
        foreach ($allDocuments as $doc) {
            $name = $doc->getAttribute('name');
            if (preg_match($pattern, $name)) {
                $expectedMatches[] = $doc->getId();
            }
        }
        $actualMatches = array_map(fn ($doc) => $doc->getId(), $documents);
        sort($expectedMatches);
        sort($actualMatches);
        $this->assertEquals(
            $expectedMatches,
            $actualMatches,
            "Query should return exactly the documents matching pattern 'Captain'"
        );

        // Test regex combined with other queries
        $pattern = '/^Captain/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', '^Captain'),
            Query::greaterThan('year', 2010),
        ]);

        // Verify all returned documents match both conditions
        foreach ($documents as $doc) {
            $name = $doc->getAttribute('name');
            $year = $doc->getAttribute('year');
            $this->assertTrue(
                (bool) preg_match($pattern, $name),
                "Document '{$name}' should match pattern '{$pattern}'"
            );
            $this->assertGreaterThan(2010, $year, "Document '{$name}' should have year > 2010");
        }

        // Verify completeness: manually check all documents that match both conditions
        $allDocuments = $database->find('moviesRegex');
        $expectedMatches = [];
        foreach ($allDocuments as $doc) {
            $name = $doc->getAttribute('name');
            $year = $doc->getAttribute('year');
            if (preg_match($pattern, $name) && $year > 2010) {
                $expectedMatches[] = $doc->getId();
            }
        }
        $actualMatches = array_map(fn ($doc) => $doc->getId(), $documents);
        sort($expectedMatches);
        sort($actualMatches);
        $this->assertEquals(
            $expectedMatches,
            $actualMatches,
            "Query should return exactly the documents matching both regex '^Captain' and year > 2010"
        );

        // Test regex with limit
        $pattern = '/.*/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', '.*'), // Match all
            Query::limit(3),
        ]);

        $this->assertEquals(3, count($documents));

        // Verify all returned documents match the pattern (should match all)
        foreach ($documents as $doc) {
            $name = $doc->getAttribute('name');
            $this->assertTrue(
                (bool) preg_match($pattern, $name),
                "Document '{$name}' should match pattern '{$pattern}'"
            );
        }

        // Note: With limit, we can't verify completeness, but we can verify all returned match

        // Test regex with non-matching pattern
        $pattern = '/^NonExistentPattern$/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', '^NonExistentPattern$'),
        ]);

        $this->assertEquals(0, count($documents));

        // Verify no documents match (double-check by getting all and filtering)
        $allDocuments = $database->find('moviesRegex');
        $matchingCount = 0;
        foreach ($allDocuments as $doc) {
            $name = $doc->getAttribute('name');
            if (preg_match($pattern, $name)) {
                $matchingCount++;
            }
        }
        $this->assertEquals(0, $matchingCount, "No documents should match pattern '{$pattern}'");

        // Verify completeness: no documents should be returned
        $this->assertEquals([], array_map(fn ($doc) => $doc->getId(), $documents));

        // Test regex with special characters (should be escaped or handled properly)
        $pattern = '/.*:.*/';
        $documents = $database->find('moviesRegex', [
            Query::regex('name', '.*:.*'), // Match movies with colon
        ]);

        // Verify completeness: all matching documents returned, no extra documents
        $verifyRegexQuery('name', '.*:.*', $documents);

        // Verify expected document is included
        $names = array_map(fn ($doc) => $doc->getAttribute('name'), $documents);
        $this->assertTrue(in_array('Captain America: The First Avenger', $names));

        // ReDOS safety: ensure pathological patterns respond quickly and do not hang
        $catastrophicPattern = '(a+)+$';
        $start = microtime(true);
        $redosDocs = $database->find('moviesRegex', [
            Query::regex('name', $catastrophicPattern),
        ]);
        $elapsed = microtime(true) - $start;
        $this->assertLessThan(1.0, $elapsed, 'Regex evaluation should not be slow or vulnerable to ReDOS');
        $verifyRegexQuery('name', $catastrophicPattern, $redosDocs);
        $this->assertCount(0, $redosDocs, 'Pathological regex should not match any movie titles');

        // Test regex search pattern - match movies with word boundaries
        // Only test if word boundaries are supported (PCRE or POSIX)
        if ($wordBoundaryPattern !== null) {
            $dbPattern = $wordBoundaryPattern.'Work'.$wordBoundaryPattern;
            $phpPattern = '/'.$wordBoundaryPatternPHP.'Work'.$wordBoundaryPatternPHP.'/';
            $documents = $database->find('moviesRegex', [
                Query::regex('name', $dbPattern),
            ]);

            // Verify all returned documents match the pattern
            foreach ($documents as $doc) {
                $name = $doc->getAttribute('name');
                $this->assertTrue(
                    (bool) preg_match($phpPattern, $name),
                    "Document '{$name}' should match pattern '{$dbPattern}'"
                );
            }

            // Verify completeness: manually check all documents
            $allDocuments = $database->find('moviesRegex');
            $expectedMatches = [];
            foreach ($allDocuments as $doc) {
                $name = $doc->getAttribute('name');
                if (preg_match($phpPattern, $name)) {
                    $expectedMatches[] = $doc->getId();
                }
            }
            $actualMatches = array_map(fn ($doc) => $doc->getId(), $documents);
            sort($expectedMatches);
            sort($actualMatches);
            $this->assertEquals(
                $expectedMatches,
                $actualMatches,
                "Query should return exactly the documents matching pattern '{$dbPattern}'"
            );
        }

        // Test regex search with multiple patterns - match movies containing 'Captain' or 'Frozen'
        $pattern1 = '/Captain/';
        $pattern2 = '/Frozen/';
        $documents = $database->find('moviesRegex', [
            Query::or([
                Query::regex('name', 'Captain'),
                Query::regex('name', 'Frozen'),
            ]),
        ]);

        // Verify all returned documents match at least one pattern
        foreach ($documents as $doc) {
            $name = $doc->getAttribute('name');
            $matchesPattern1 = (bool) preg_match($pattern1, $name);
            $matchesPattern2 = (bool) preg_match($pattern2, $name);
            $this->assertTrue(
                $matchesPattern1 || $matchesPattern2,
                "Document '{$name}' should match either pattern 'Captain' or 'Frozen'"
            );
        }

        // Verify completeness: manually check all documents
        $allDocuments = $database->find('moviesRegex');
        $expectedMatches = [];
        foreach ($allDocuments as $doc) {
            $name = $doc->getAttribute('name');
            if (preg_match($pattern1, $name) || preg_match($pattern2, $name)) {
                $expectedMatches[] = $doc->getId();
            }
        }
        $actualMatches = array_map(fn ($doc) => $doc->getId(), $documents);
        sort($expectedMatches);
        sort($actualMatches);
        $this->assertEquals(
            $expectedMatches,
            $actualMatches,
            "Query should return exactly the documents matching pattern 'Captain' OR 'Frozen'"
        );
        $database->deleteCollection('moviesRegex');
    }

    public function testRegexInjection(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        // Skip test if regex is not supported
        if (! $database->getAdapter()->supports(Capability::Regex)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collectionName = 'injectionTest';
        $database->createCollection($collectionName, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->assertEquals(true, $database->createAttribute($collectionName, new Attribute(key: 'text', type: ColumnType::String, size: 1000, required: true)));
        }

        // Create test documents - one that should match, one that shouldn't
        $database->createDocument($collectionName, new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'text' => 'target',
        ]));

        $database->createDocument($collectionName, new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'text' => 'other',
        ]));

        // SQL injection attempts - these should NOT return the "other" document
        $sqlInjectionPatterns = [
            "target') OR '1'='1",           // SQL injection attempt
            "target' OR 1=1--",            // SQL injection with comment
            "target' OR 'x'='x",           // SQL injection attempt
            "target' UNION SELECT *--",     // SQL UNION injection
        ];

        // MongoDB injection attempts - these should NOT return the "other" document
        $mongoInjectionPatterns = [
            'target" || "1"=="1',          // MongoDB injection attempt
            'target" || true',              // MongoDB boolean injection
            'target"} || {"text": "other"}', // MongoDB operator injection
        ];

        $allInjectionPatterns = array_merge($sqlInjectionPatterns, $mongoInjectionPatterns);

        foreach ($allInjectionPatterns as $pattern) {
            try {
                $results = $database->find($collectionName, [
                    Query::regex('text', $pattern),
                ]);

                // Critical check: if injection succeeded, we might get the "other" document
                // which should NOT match a pattern starting with "target"
                $foundOther = false;
                foreach ($results as $doc) {
                    $text = $doc->getAttribute('text');
                    if ($text === 'other') {
                        $foundOther = true;

                        // Verify that "other" doesn't actually match the pattern as a regex
                        $matches = @preg_match('/'.str_replace('/', '\/', $pattern).'/', $text);
                        if ($matches === 0 || $matches === false) {
                            // "other" doesn't match the pattern but was returned
                            // This indicates potential injection vulnerability
                            $this->fail(
                                "Potential injection detected: Pattern '{$pattern}' returned document 'other' ".
                                "which doesn't match the pattern. This suggests SQL/MongoDB injection may have succeeded."
                            );
                        }
                    }
                }

                // Additional verification: check that all returned documents actually match the pattern
                foreach ($results as $doc) {
                    $text = $doc->getAttribute('text');
                    $matches = @preg_match('/'.str_replace('/', '\/', $pattern).'/', $text);

                    // If pattern is invalid, skip validation
                    if ($matches === false) {
                        continue;
                    }

                    // If document doesn't match but was returned, it's suspicious
                    if ($matches === 0) {
                        $this->fail(
                            "Potential injection: Document '{$text}' was returned for pattern '{$pattern}' ".
                            "but doesn't match the regex pattern."
                        );
                    }
                }

            } catch (\Exception $e) {
                // Exceptions are acceptable - they indicate the injection was blocked or caused an error
                // This is actually good - it means the system rejected the malicious pattern
                $this->assertInstanceOf(\Exception::class, $e);
            }
        }

        // Test that legitimate regex patterns still work correctly
        $legitimatePatterns = [
            'target',      // Should match "target"
            '^target',     // Should match "target" (anchored)
            'other',       // Should match "other"
        ];

        foreach ($legitimatePatterns as $pattern) {
            try {
                $results = $database->find($collectionName, [
                    Query::regex('text', $pattern),
                ]);

                $this->assertIsArray($results);

                // Verify each result actually matches
                foreach ($results as $doc) {
                    $text = $doc->getAttribute('text');
                    $matches = @preg_match('/'.str_replace('/', '\/', $pattern).'/', $text);
                    if ($matches !== false) {
                        $this->assertEquals(
                            1,
                            $matches,
                            "Document '{$text}' should match pattern '{$pattern}'"
                        );
                    }
                }
            } catch (\Exception $e) {
                $this->fail("Legitimate pattern '{$pattern}' should not throw exception: ".$e->getMessage());
            }
        }

        // Cleanup
        $database->deleteCollection($collectionName);
    }

    /**
     * Test ReDoS (Regular Expression Denial of Service) with timeout protection
     * This test verifies that ReDoS patterns either timeout properly or complete quickly,
     * preventing denial of service attacks.
     */
    //    public function testRegexRedos(): void
    //    {
    //        /** @var Database $database */
    //        $database = static::getDatabase();
    //
    //        // Skip test if regex is not supported
    //        if (!$database->getAdapter()->supports(Capability::Regex)) {
    //            $this->expectNotToPerformAssertions();
    //            return;
    //        }
    //
    //        $collectionName = 'redosTimeoutTest';
    //        $database->createCollection($collectionName, permissions: [
    //            Permission::create(Role::any()),
    //            Permission::read(Role::any()),
    //            Permission::update(Role::any()),
    //            Permission::delete(Role::any()),
    //        ]);
    //
    //        if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
    //            $this->assertEquals(true, $database->createAttribute($collectionName, new Attribute(key: 'text', type: ColumnType::String, size: 1000, required: true)));
    //        }
    //
    //        // Create documents with strings designed to trigger ReDoS
    //        // These strings have many 'a's but end with 'c' instead of 'b'
    //        // This causes catastrophic backtracking with patterns like (a+)+b
    //        $redosStrings = [];
    //        for ($i = 15; $i <= 35; $i += 5) {
    //            $redosStrings[] = str_repeat('a', $i) . 'c';
    //        }
    //
    //        // Also add some normal strings
    //        $normalStrings = [
    //            'normal text',
    //            'another string',
    //            'test123',
    //            'valid data',
    //        ];
    //
    //        $documents = [];
    //        foreach ($redosStrings as $text) {
    //            $documents[] = new Document([
    //                '$permissions' => [
    //                    Permission::read(Role::any()),
    //                    Permission::create(Role::any()),
    //                    Permission::update(Role::any()),
    //                    Permission::delete(Role::any()),
    //                ],
    //                'text' => $text,
    //            ]);
    //        }
    //
    //        foreach ($normalStrings as $text) {
    //            $documents[] = new Document([
    //                '$permissions' => [
    //                    Permission::read(Role::any()),
    //                    Permission::create(Role::any()),
    //                    Permission::update(Role::any()),
    //                    Permission::delete(Role::any()),
    //                ],
    //                'text' => $text,
    //            ]);
    //        }
    //
    //        $database->createDocuments($collectionName, $documents);
    //
    //        // ReDoS patterns that cause exponential backtracking
    //        $redosPatterns = [
    //            '(a+)+b',      // Classic ReDoS: nested quantifiers
    //            '(a|a)*b',     // Alternation with quantifier
    //            '(a+)+$',      // Anchored pattern
    //            '(a*)*b',      // Nested star quantifiers
    //            '(a+)+b+',     // Multiple nested quantifiers
    //            '(.+)+b',      // Generic nested quantifiers
    //            '(.*)+b',      // Generic nested quantifiers
    //        ];
    //
    //        $supportsTimeout = $database->getAdapter()->supports(Capability::Timeouts);
    //
    //        if ($supportsTimeout) {
    //            $database->setTimeout(2000);
    //        }
    //
    //        foreach ($redosPatterns as $pattern) {
    //            $startTime = microtime(true);
    //
    //            try {
    //                $results = $database->find($collectionName, [
    //                    Query::regex('text', $pattern),
    //                ]);
    //                $elapsed = microtime(true) - $startTime;
    //                // If timeout is supported, the query should either:
    //                // 1. Complete quickly (< 3 seconds) if ReDoS is mitigated
    //                // 2. Throw TimeoutException if it takes too long
    //                if ($supportsTimeout) {
    //                    // If we got here without timeout, it should have completed quickly
    //                    $this->assertLessThan(
    //                        3.0,
    //                        $elapsed,
    //                        "Regex pattern '{$pattern}' should complete quickly or timeout. Took {$elapsed}s"
    //                    );
    //                } else {
    //                    // Without timeout support, we just check it doesn't hang forever
    //                    // Set a reasonable upper bound (15 seconds) for systems without timeout
    //                    $this->assertLessThan(
    //                        15.0,
    //                        $elapsed,
    //                        "Regex pattern '{$pattern}' should not cause excessive delay. Took {$elapsed}s"
    //                    );
    //                }
    //
    //                // Verify results: none of our ReDoS strings should match these patterns
    //                // (they all end with 'c', not 'b')
    //                foreach ($results as $doc) {
    //                    $text = $doc->getAttribute('text');
    //                    // If it matched, verify it's actually a valid match
    //                    $matches = @preg_match('/' . str_replace('/', '\/', $pattern) . '/', $text);
    //                    if ($matches !== false) {
    //                        $this->assertEquals(
    //                            1,
    //                            $matches,
    //                            "Document with text '{$text}' should actually match pattern '{$pattern}'"
    //                        );
    //                    }
    //                }
    //
    //            } catch (TimeoutException $e) {
    //                // Timeout is expected for ReDoS patterns if not properly mitigated
    //                $elapsed = microtime(true) - $startTime;
    //                $this->assertInstanceOf(
    //                    TimeoutException::class,
    //                    $e,
    //                    "Regex pattern '{$pattern}' should timeout if it causes ReDoS. Elapsed: {$elapsed}s"
    //                );
    //
    //                // Timeout should happen within reasonable time (not immediately, but not too late)
    //                // Fast timeouts are actually good - they mean the system is protecting itself quickly
    //                $this->assertGreaterThan(
    //                    0.05,
    //                    $elapsed,
    //                    "Timeout should occur after some minimal processing time"
    //                );
    //
    //                // Timeout should happen before the timeout limit (with some buffer)
    //                if ($supportsTimeout) {
    //                    $this->assertLessThan(
    //                        5.0,
    //                        $elapsed,
    //                        "Timeout should occur within reasonable time (before 5 seconds)"
    //                    );
    //                }
    //
    //            } catch (\Exception $e) {
    //                // Check if this is a query interruption/timeout from MySQL (error 1317)
    //                // MySQL sometimes throws "Query execution was interrupted" instead of TimeoutException
    //                $message = $e->getMessage();
    //                $isQueryInterrupted = false;
    //
    //                // Check message for interruption keywords
    //                if (strpos($message, 'Query execution was interrupted') !== false ||
    //                    strpos($message, 'interrupted') !== false) {
    //                    $isQueryInterrupted = true;
    //                }
    //
    //                // Check if it's a PDOException with error code 1317
    //                if ($e instanceof PDOException) {
    //                    $errorInfo = $e->errorInfo ?? [];
    //                    // Error 1317 is "Query execution was interrupted"
    //                    if (isset($errorInfo[1]) && $errorInfo[1] === 1317) {
    //                        $isQueryInterrupted = true;
    //                    }
    //                    // Also check SQLSTATE 70100
    //                    if ($e->getCode() === '70100') {
    //                        $isQueryInterrupted = true;
    //                    }
    //                }
    //
    //                if ($isQueryInterrupted) {
    //                    // This is effectively a timeout - MySQL interrupted the query
    //                    $elapsed = microtime(true) - $startTime;
    //                    $this->assertGreaterThan(
    //                        0.05,
    //                        $elapsed,
    //                        "Query interruption should occur after some minimal processing time"
    //                    );
    //                    // This is acceptable - the query was interrupted due to timeout
    //                    continue;
    //                }
    //
    //                // Other exceptions are unexpected
    //                $this->fail("Unexpected exception for pattern '{$pattern}': " . get_class($e) . " - " . $e->getMessage());
    //            }
    //        }
    //
    //        // Test with a pattern that should match quickly (not ReDoS)
    //        $safePattern = 'normal';
    //        $startTime = microtime(true);
    //        $results = $database->find($collectionName, [
    //            Query::regex('text', $safePattern),
    //        ]);
    //        $elapsed = microtime(true) - $startTime;
    //
    //        // Safe patterns should complete very quickly
    //        $this->assertLessThan(1.0, $elapsed, 'Safe regex pattern should complete quickly');
    //        $this->assertGreaterThan(0, count($results), 'Safe pattern should match some documents');
    //
    //        // Verify safe pattern results are correct
    //        foreach ($results as $doc) {
    //            $text = $doc->getAttribute('text');
    //            $this->assertStringContainsString('normal', $text, "Document '{$text}' should contain 'normal'");
    //        }
    //
    //        // Cleanup
    //        if ($supportsTimeout) {
    //            $database->clearTimeout();
    //        }
    //        $database->deleteCollection($collectionName);
    //    }
}

<?php

namespace Tests\E2E\Adapter;

use Exception;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Index;
use Utopia\Database\Validator\Structure;
use Utopia\Http\Validator\Range;

ini_set('memory_limit', '2048M');

abstract class Base extends TestCase
{
    protected static string $namespace;

    /**
     * @var Authorization
     */
    protected static ?Authorization $authorization = null;

    /**
     * @return Database
     */
    abstract protected function getDatabase(): Database;

    /**
     * @return string
     */
    abstract protected static function getAdapterName(): string;

    public function setUp(): void
    {
        if (is_null(self::$authorization)) {
            self::$authorization = new Authorization();
        }

        self::$authorization->addRole('any');
    }

    public function tearDown(): void
    {
        self::$authorization->setDefaultStatus(true);
    }

    protected string $testDatabase = 'utopiaTests';

    public function testPing(): void
    {
        $this->assertEquals(true, $this->getDatabase()->ping());
    }

    public function testCreateExistsDelete(): void
    {
        $schemaSupport = $this->getDatabase()->getAdapter()->getSupportForSchemas();
        if (!$schemaSupport) {
            $this->assertEquals($this->getDatabase(), $this->getDatabase()->setDatabase($this->testDatabase));
            $this->assertEquals(true, $this->getDatabase()->create());
            return;
        }

        if (!$this->getDatabase()->exists($this->testDatabase)) {
            $this->assertEquals(true, $this->getDatabase()->create());
        }
        $this->assertEquals(true, $this->getDatabase()->exists($this->testDatabase));
        $this->assertEquals(true, $this->getDatabase()->delete($this->testDatabase));
        $this->assertEquals(false, $this->getDatabase()->exists($this->testDatabase));
        $this->assertEquals($this->getDatabase(), $this->getDatabase()->setDatabase($this->testDatabase));
        $this->assertEquals(true, $this->getDatabase()->create());
    }

    public function testUpdateDeleteCollectionNotFound(): void
    {
        try {
            static::getDatabase()->deleteCollection('not_found');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        try {
            static::getDatabase()->updateCollection('not_found', [], true);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }
    }

    public function testDeleteRelatedCollection(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('c1');
        $this->getDatabase()->createCollection('c2');

        // ONE_TO_ONE
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c1'));
        $collection = $this->getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c1');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c2'));
        $collection = $this->getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c2');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c1'));
        $collection = $this->getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c1');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c2'));
        $collection = $this->getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // ONE_TO_MANY
        $this->getDatabase()->createCollection('c2');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c1'));
        $collection = $this->getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c1');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c2'));
        $collection = $this->getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c2');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c1'));
        $collection = $this->getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c1');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c2'));
        $collection = $this->getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // RELATION_MANY_TO_ONE
        $this->getDatabase()->createCollection('c2');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c1'));
        $collection = $this->getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c1');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c2'));
        $collection = $this->getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c2');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c1'));
        $collection = $this->getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $this->getDatabase()->createCollection('c1');
        $this->getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, $this->getDatabase()->deleteCollection('c2'));
        $collection = $this->getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));
    }

    public function testVirtualRelationsAttributes(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('v1');
        static::getDatabase()->createCollection('v2');

        /**
         * RELATION_ONE_TO_ONE
         * TwoWay is false no attribute is created on v2
         */
        static::getDatabase()->createRelationship(
            collection: 'v1',
            relatedCollection: 'v2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: false
        );

        try {
            static::getDatabase()->createDocument('v2', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
                'v1' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->createDocument('v2', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
                'v1' => [
                    '$id' => 'test',
                    '$permissions' => [],
                ]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->find('v2', [
                Query::equal('v1', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        /**
         * Success for later test update
         */

        $doc = static::getDatabase()->createDocument('v1', new Document([
            '$id' => 'man',
            '$permissions' => [
                Permission::update(Role::any()),
                Permission::read(Role::any()),
            ],
            'v2' => [
                '$id' => 'woman',
                '$permissions' => [
                    Permission::update(Role::any()),
                    Permission::read(Role::any())
                ]
            ]
        ]));

        $this->assertEquals('man', $doc->getId());

        try {
            static::getDatabase()->updateDocument('v1', 'man', new Document([
                '$permissions' => [],
                'v2' => [[
                    '$id' => 'woman',
                    '$permissions' => []
                ]]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        static::getDatabase()->deleteRelationship('v1', 'v2');

        /**
         * RELATION_ONE_TO_MANY
         * No attribute is created in V1 collection
         */
        static::getDatabase()->createRelationship(
            collection: 'v1',
            relatedCollection: 'v2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );

        try {
            static::getDatabase()->createDocument('v1', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
                'v2' => [ // Expecting Array of arrays or array of strings, object provided
                    '$id' => 'test',
                    '$permissions' => [],
                ]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->createDocument('v1', new Document([
                '$permissions' => [],
                'v2' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->createDocument('v2', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
                'v1' => [[  // Expecting a string or an object ,array provided
                    '$id' => 'test',
                    '$permissions' => [],
                ]]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        /**
         * Success for later test update
         */
        $doc = static::getDatabase()->createDocument('v2', new Document([
            '$id' => 'v2_uid',
            '$permissions' => [
                Permission::update(Role::any()),
            ],
            'v1' => [
                '$id' => 'v1_uid',
                '$permissions' => [
                    Permission::update(Role::any())
                ],
            ]
        ]));

        $this->assertEquals('v2_uid', $doc->getId());

        /**
         * Test update
         */

        try {
            static::getDatabase()->updateDocument('v1', 'v1_uid', new Document([
                '$permissions' => [],
                'v2' => [ // Expecting array of arrays or array of strings, object given
                    '$id' => 'v2_uid',
                    '$permissions' => [],
                ]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->updateDocument('v1', 'v1_uid', new Document([
                '$permissions' => [],
                'v2' => 'v2_uid'
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->updateDocument('v2', 'v2_uid', new Document([
                '$permissions' => [],
                'v1' => [
                    '$id' => null, // Invalid value
                    '$permissions' => [],
                ]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        /**
         * Here we get this error: Unknown PDO Type for array
         * Added in Filter.php Text validator for relationship
         */
        try {
            static::getDatabase()->find('v2', [
                //@phpstan-ignore-next-line
                Query::equal('v1', [['doc1']]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        try {
            static::getDatabase()->find('v1', [
                Query::equal('v2', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        static::getDatabase()->deleteRelationship('v1', 'v2');

        /**
         * RELATION_MANY_TO_ONE
         * No attribute is created in V2 collection
         */
        static::getDatabase()->createRelationship(
            collection: 'v1',
            relatedCollection: 'v2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true
        );

        try {
            static::getDatabase()->createDocument('v1', new Document([
                '$id' => 'doc',
                '$permissions' => [],
                'v2' => [[ // Expecting an object or a string array provided
                    '$id' => 'test',
                    '$permissions' => [],
                ]]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->createDocument('v2', new Document([
                '$permissions' => [],
                'v1' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->createDocument('v2', new Document([
                '$id' => 'doc',
                '$permissions' => [],
                'v1' => [ // Expecting an array, object provided
                    '$id' => 'test',
                    '$permissions' => [],
                ]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->find('v2', [
                Query::equal('v1', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        /**
         * Success for later test update
         */
        $doc = static::getDatabase()->createDocument('v1', new Document([
            '$id' => 'doc1',
            '$permissions' => [
                Permission::update(Role::any()),
                Permission::read(Role::any()),
            ],
            'v2' => [
                '$id' => 'doc2',
                '$permissions' => [
                    Permission::update(Role::any()),
                    Permission::read(Role::any()),
                ],
            ]
        ]));

        $this->assertEquals('doc1', $doc->getId());

        try {
            static::getDatabase()->updateDocument('v1', 'doc1', new Document([
                '$permissions' => [
                    Permission::update(Role::any()),
                    Permission::read(Role::any()),
                ],
                'v2' => [[]],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->updateDocument('v2', 'doc2', new Document([
                '$permissions' => [],
                'v1' => null
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        static::getDatabase()->deleteRelationship('v1', 'v2');

        /**
         * RELATION_MANY_TO_MANY
         * No attribute on V1/v2 collections only on junction table
         */
        static::getDatabase()->createRelationship(
            collection: 'v1',
            relatedCollection: 'v2',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'students',
            twoWayKey: 'classes'
        );

        try {
            static::getDatabase()->createDocument('v1', new Document([
                '$permissions' => [],
                'students' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->createDocument('v2', new Document([
                '$permissions' => [],
                'classes' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->createDocument('v2', new Document([
                '$id' => 'doc',
                '$permissions' => [],
                'classes' => [ // Expected array, object provided
                    '$id' => 'test',
                    '$permissions' => [],
                ]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->find('v1', [
                Query::equal('students', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        try {
            static::getDatabase()->find('v2', [
                Query::equal('classes', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        /**
         * Success for later test update
         */

        $doc = static::getDatabase()->createDocument('v1', new Document([
            '$id' => 'class1',
            '$permissions' => [
                Permission::update(Role::any()),
                Permission::read(Role::any()),
            ],
            'students' => [
                [
                    '$id' => 'Richard',
                    '$permissions' => [
                        Permission::update(Role::any()),
                        Permission::read(Role::any())
                    ]
                ],
                [
                    '$id' => 'Bill',
                    '$permissions' => [
                        Permission::update(Role::any()),
                        Permission::read(Role::any())
                    ]
                ]
            ]
        ]));

        $this->assertEquals('class1', $doc->getId());

        try {
            static::getDatabase()->updateDocument('v1', 'class1', new Document([
                '$permissions' => [
                    Permission::update(Role::any()),
                    Permission::read(Role::any()),
                ],
                'students' => [
                    '$id' => 'Richard',
                    '$permissions' => [
                        Permission::update(Role::any()),
                        Permission::read(Role::any())
                    ]
                ]
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            static::getDatabase()->updateDocument('v1', 'class1', new Document([
                '$permissions' => [
                    Permission::update(Role::any()),
                    Permission::read(Role::any()),
                ],
                'students' => 'Richard'
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }
    }

    public function testPreserveDatesUpdate(): void
    {
        self::$authorization->disable();

        $this->getDatabase()->setPreserveDates(true);

        $this->getDatabase()->createCollection('preserve_update_dates');

        $this->getDatabase()->createAttribute('preserve_update_dates', 'attr1', Database::VAR_STRING, 10, false);

        $doc1 = $this->getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'attr1' => 'value1',
        ]));

        $doc2 = $this->getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc2',
            '$permissions' => [],
            'attr1' => 'value2',
        ]));

        $doc3 = $this->getDatabase()->createDocument('preserve_update_dates', new Document([
            '$id' => 'doc3',
            '$permissions' => [],
            'attr1' => 'value3',
        ]));

        $newDate = '2000-01-01T10:00:00.000+00:00';

        $doc1->setAttribute('$updatedAt', $newDate);
        $this->getDatabase()->updateDocument('preserve_update_dates', 'doc1', $doc1);
        $doc1 = $this->getDatabase()->getDocument('preserve_update_dates', 'doc1');
        $this->assertEquals($newDate, $doc1->getAttribute('$updatedAt'));

        $doc2->setAttribute('$updatedAt', $newDate);
        $doc3->setAttribute('$updatedAt', $newDate);
        $this->getDatabase()->updateDocuments('preserve_update_dates', [$doc2, $doc3], 2);

        $doc2 = $this->getDatabase()->getDocument('preserve_update_dates', 'doc2');
        $doc3 = $this->getDatabase()->getDocument('preserve_update_dates', 'doc3');
        $this->assertEquals($newDate, $doc2->getAttribute('$updatedAt'));
        $this->assertEquals($newDate, $doc3->getAttribute('$updatedAt'));

        $this->getDatabase()->deleteCollection('preserve_update_dates');

        $this->getDatabase()->setPreserveDates(false);

        self::$authorization->reset();
    }

    public function testPreserveDatesCreate(): void
    {
        self::$authorization->disable();

        $this->getDatabase()->setPreserveDates(true);

        $this->getDatabase()->createCollection('preserve_create_dates');

        $this->getDatabase()->createAttribute('preserve_create_dates', 'attr1', Database::VAR_STRING, 10, false);

        $date = '2000-01-01T10:00:00.000+00:00';

        $this->getDatabase()->createDocument('preserve_create_dates', new Document([
            '$id' => 'doc1',
            '$permissions' => [],
            'attr1' => 'value1',
            '$createdAt' => $date
        ]));

        $this->getDatabase()->createDocuments('preserve_create_dates', [
            new Document([
                '$id' => 'doc2',
                '$permissions' => [],
                'attr1' => 'value2',
                '$createdAt' => $date
            ]),
            new Document([
                '$id' => 'doc3',
                '$permissions' => [],
                'attr1' => 'value3',
                '$createdAt' => $date
            ]),
        ], 2);

        $doc1 = $this->getDatabase()->getDocument('preserve_create_dates', 'doc1');
        $doc2 = $this->getDatabase()->getDocument('preserve_create_dates', 'doc2');
        $doc3 = $this->getDatabase()->getDocument('preserve_create_dates', 'doc3');
        $this->assertEquals($date, $doc1->getAttribute('$createdAt'));
        $this->assertEquals($date, $doc2->getAttribute('$createdAt'));
        $this->assertEquals($date, $doc3->getAttribute('$createdAt'));

        $this->getDatabase()->deleteCollection('preserve_create_dates');

        $this->getDatabase()->setPreserveDates(false);

        self::$authorization->reset();
    }

    /**
     * @throws Exception|Throwable
     */
    public function testIndexValidation(): void
    {
        $attributes = [
            new Document([
                '$id' => ID::custom('title1'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('title2'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 500,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['title1', 'title2'],
                'lengths' => [701,50],
                'orders' => [],
            ]),
        ];

        $collection = new Document([
            '$id' => ID::custom('index_length'),
            'name' => 'test',
            'attributes' => $attributes,
            'indexes' => $indexes
        ]);

        $validator = new Index($attributes, $this->getDatabase()->getAdapter()->getMaxIndexLength());

        $errorMessage = 'Index length 701 is larger than the size for title1: 700"';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            $this->getDatabase()->createCollection($collection->getId(), $attributes, $indexes, [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals($errorMessage, $e->getMessage());
        }

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['title1', 'title2'],
                'lengths' => [700], // 700, 500 (length(title2))
                'orders' => [],
            ]),
        ];

        $collection->setAttribute('indexes', $indexes);

        if ($this->getDatabase()->getAdapter()->getMaxIndexLength() > 0) {
            $errorMessage = 'Index length is longer than the maximum: ' . $this->getDatabase()->getAdapter()->getMaxIndexLength();
            $this->assertFalse($validator->isValid($indexes[0]));
            $this->assertEquals($errorMessage, $validator->getDescription());

            try {
                $this->getDatabase()->createCollection($collection->getId(), $attributes, $indexes);
                $this->fail('Failed to throw exception');
            } catch (Exception $e) {
                $this->assertEquals($errorMessage, $e->getMessage());
            }
        }

        $attributes[] = new Document([
            '$id' => ID::custom('integer'),
            'type' => Database::VAR_INTEGER,
            'format' => '',
            'size' => 10000,
            'signed' => true,
            'required' => false,
            'default' => null,
            'array' => false,
            'filters' => [],
        ]);

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_FULLTEXT,
                'attributes' => ['title1', 'integer'],
                'lengths' => [],
                'orders' => [],
            ]),
        ];

        $collection = new Document([
            '$id' => ID::custom('index_length'),
            'name' => 'test',
            'attributes' => $attributes,
            'indexes' => $indexes
        ]);

        $validator = new Index($attributes, $this->getDatabase()->getAdapter()->getMaxIndexLength());
        $errorMessage = 'Attribute "integer" cannot be part of a FULLTEXT index, must be of type string';
        $this->assertFalse($validator->isValid($indexes[0]));
        $this->assertEquals($errorMessage, $validator->getDescription());

        try {
            $this->getDatabase()->createCollection($collection->getId(), $attributes, $indexes);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals($errorMessage, $e->getMessage());
        }
    }

    public function testCreatedAtUpdatedAt(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', $this->getDatabase()->createCollection('created_at'));
        $this->getDatabase()->createAttribute('created_at', 'title', Database::VAR_STRING, 100, false);
        $document = $this->getDatabase()->createDocument('created_at', new Document([
            '$id' => ID::custom('uid123'),

            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $this->assertNotEmpty($document->getInternalId());
        $this->assertNotNull($document->getInternalId());
    }


    public function testQueryTimeout(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForTimeouts()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('global-timeouts');
        $this->assertEquals(true, static::getDatabase()->createAttribute('global-timeouts', 'longtext', Database::VAR_STRING, 100000000, true));

        for ($i = 0 ; $i <= 20 ; $i++) {
            $this->getDatabase()->createDocument('global-timeouts', new Document([
                'longtext' => file_get_contents(__DIR__ . '/../../resources/longtext.txt'),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any())
                ]
            ]));
        }

        static::getDatabase()->setTimeout(1);

        try {
            $this->getDatabase()->find('global-timeouts', [
                Query::notEqual('longtext', 'appwrite'),
            ]);
            $this->fail('Failed to throw exception');
        } catch (TimeoutException $ex) {
            static::getDatabase()->clearTimeout();
            static::getDatabase()->deleteCollection('global-timeouts');
        }
    }

    /**
     * @depends testCreateExistsDelete
     */
    public function testCreateListExistsDeleteCollection(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', $this->getDatabase()->createCollection('actors', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $this->assertCount(1, $this->getDatabase()->listCollections());
        $this->assertEquals(true, $this->getDatabase()->exists($this->testDatabase, 'actors'));

        // Collection names should not be unique
        $this->assertInstanceOf('Utopia\Database\Document', $this->getDatabase()->createCollection('actors2', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $this->assertCount(2, $this->getDatabase()->listCollections());
        $this->assertEquals(true, $this->getDatabase()->exists($this->testDatabase, 'actors2'));
        $collection = $this->getDatabase()->getCollection('actors2');
        $collection->setAttribute('name', 'actors'); // change name to one that exists
        $this->assertInstanceOf('Utopia\Database\Document', $this->getDatabase()->updateDocument(
            $collection->getCollection(),
            $collection->getId(),
            $collection
        ));
        $this->assertEquals(true, $this->getDatabase()->deleteCollection('actors2')); // Delete collection when finished
        $this->assertCount(1, $this->getDatabase()->listCollections());

        $this->assertEquals(false, $this->getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(true, $this->getDatabase()->deleteCollection('actors'));
        $this->assertEquals(true, $this->getDatabase()->getCollection('actors')->isEmpty());
        $this->assertEquals(false, $this->getDatabase()->exists($this->testDatabase, 'actors'));
    }

    public function testSizeCollection(): void
    {
        static::getDatabase()->createCollection('sizeTest1');
        static::getDatabase()->createCollection('sizeTest2');

        $size1 = static::getDatabase()->getSizeOfCollection('sizeTest1');
        $size2 = static::getDatabase()->getSizeOfCollection('sizeTest2');
        $sizeDifference = abs($size1 - $size2);
        // Size of an empty collection returns either 172032 or 167936 bytes randomly
        // Therefore asserting with a tolerance of 5000 bytes
        $byteDifference = 5000;

        if (!static::getDatabase()->analyzeCollection('sizeTest2')) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->assertLessThan($byteDifference, $sizeDifference);

        static::getDatabase()->createAttribute('sizeTest2', 'string1', Database::VAR_STRING, 20000, true);
        static::getDatabase()->createAttribute('sizeTest2', 'string2', Database::VAR_STRING, 254 + 1, true);
        static::getDatabase()->createAttribute('sizeTest2', 'string3', Database::VAR_STRING, 254 + 1, true);
        static::getDatabase()->createIndex('sizeTest2', 'index', Database::INDEX_KEY, ['string1', 'string2', 'string3'], [128, 128, 128]);


        $loopCount = 100;

        for ($i = 0; $i < $loopCount; $i++) {
            static::getDatabase()->createDocument('sizeTest2', new Document([
                '$id' => 'doc' . $i,
                'string1' => 'string1' . $i . str_repeat('A', 10000),
                'string2' => 'string2',
                'string3' => 'string3',
            ]));
        }

        static::getDatabase()->analyzeCollection('sizeTest2');

        $size2 = $this->getDatabase()->getSizeOfCollection('sizeTest2');

        $this->assertGreaterThan($size1, $size2);

        self::$authorization->skip(function () use ($loopCount) {
            for ($i = 0; $i < $loopCount; $i++) {
                $this->getDatabase()->deleteDocument('sizeTest2', 'doc' . $i);
            }
        });

        sleep(5);

        static::getDatabase()->analyzeCollection('sizeTest2');

        $size3 = $this->getDatabase()->getSizeOfCollection('sizeTest2');

        $this->assertLessThan($size2, $size3);
    }

    public function testSizeCollectionOnDisk(): void
    {
        $this->getDatabase()->createCollection('sizeTestDisk1');
        $this->getDatabase()->createCollection('sizeTestDisk2');

        $size1 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk1');
        $size2 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk2');
        $sizeDifference = abs($size1 - $size2);
        // Size of an empty collection returns either 172032 or 167936 bytes randomly
        // Therefore asserting with a tolerance of 5000 bytes
        $byteDifference = 5000;
        $this->assertLessThan($byteDifference, $sizeDifference);

        $this->getDatabase()->createAttribute('sizeTestDisk2', 'string1', Database::VAR_STRING, 20000, true);
        $this->getDatabase()->createAttribute('sizeTestDisk2', 'string2', Database::VAR_STRING, 254 + 1, true);
        $this->getDatabase()->createAttribute('sizeTestDisk2', 'string3', Database::VAR_STRING, 254 + 1, true);
        $this->getDatabase()->createIndex('sizeTestDisk2', 'index', Database::INDEX_KEY, ['string1', 'string2', 'string3'], [128, 128, 128]);

        $loopCount = 40;

        for ($i = 0; $i < $loopCount; $i++) {
            $this->getDatabase()->createDocument('sizeTestDisk2', new Document([
                'string1' => 'string1' . $i,
                'string2' => 'string2' . $i,
                'string3' => 'string3' . $i,
            ]));
        }

        $size2 = $this->getDatabase()->getSizeOfCollectionOnDisk('sizeTestDisk2');

        $this->assertGreaterThan($size1, $size2);
    }

    public function testSizeFullText(): void
    {
        // SQLite does not support fulltext indexes
        if (!$this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('fullTextSizeTest');

        $size1 = $this->getDatabase()->getSizeOfCollectionOnDisk('fullTextSizeTest');

        $this->getDatabase()->createAttribute('fullTextSizeTest', 'string1', Database::VAR_STRING, 128, true);
        $this->getDatabase()->createAttribute('fullTextSizeTest', 'string2', Database::VAR_STRING, 254, true);
        $this->getDatabase()->createAttribute('fullTextSizeTest', 'string3', Database::VAR_STRING, 254, true);
        $this->getDatabase()->createIndex('fullTextSizeTest', 'index', Database::INDEX_KEY, ['string1', 'string2', 'string3'], [128, 128, 128]);

        $loopCount = 10;

        for ($i = 0; $i < $loopCount; $i++) {
            $this->getDatabase()->createDocument('fullTextSizeTest', new Document([
                'string1' => 'string1' . $i,
                'string2' => 'string2' . $i,
                'string3' => 'string3' . $i,
            ]));
        }

        $size2 = $this->getDatabase()->getSizeOfCollectionOnDisk('fullTextSizeTest');

        $this->assertGreaterThan($size1, $size2);

        $this->getDatabase()->createIndex('fullTextSizeTest', 'fulltext_index', Database::INDEX_FULLTEXT, ['string1']);

        $size3 = $this->getDatabase()->getSizeOfCollectionOnDisk('fullTextSizeTest');

        $this->assertGreaterThan($size2, $size3);
    }

    public function testPurgeCollectionCache(): void
    {
        $this->getDatabase()->createCollection('redis');

        $this->assertEquals(true, $this->getDatabase()->createAttribute('redis', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('redis', 'age', Database::VAR_INTEGER, 0, true));

        $this->getDatabase()->createDocument('redis', new Document([
            '$id' => 'doc1',
            'name' => 'Richard',
            'age' => 15,
            '$permissions' => [
                Permission::read(Role::any()),
            ]
        ]));

        $document = $this->getDatabase()->getDocument('redis', 'doc1');

        $this->assertEquals('Richard', $document->getAttribute('name'));
        $this->assertEquals(15, $document->getAttribute('age'));

        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('redis', 'age'));

        $document = $this->getDatabase()->getDocument('redis', 'doc1');
        $this->assertEquals('Richard', $document->getAttribute('name'));
        $this->assertArrayNotHasKey('age', $document);

        $this->assertEquals(true, $this->getDatabase()->createAttribute('redis', 'age', Database::VAR_INTEGER, 0, true));

        $document = $this->getDatabase()->getDocument('redis', 'doc1');
        $this->assertEquals('Richard', $document->getAttribute('name'));
        $this->assertArrayHasKey('age', $document);
    }

    public function testCreateDeleteAttribute(): void
    {
        $this->getDatabase()->createCollection('attributes');

        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'string1', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'string2', Database::VAR_STRING, 16382 + 1, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'string3', Database::VAR_STRING, 65535 + 1, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'string4', Database::VAR_STRING, 16777215 + 1, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'bigint', Database::VAR_INTEGER, 8, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'boolean', Database::VAR_BOOLEAN, 0, true));

        $this->assertEquals(true, $this->getDatabase()->createIndex('attributes', 'string1_index', Database::INDEX_KEY, ['string1']));
        $this->assertEquals(true, $this->getDatabase()->createIndex('attributes', 'string2_index', Database::INDEX_KEY, ['string2'], [255]));
        $this->assertEquals(true, $this->getDatabase()->createIndex('attributes', 'multi_index', Database::INDEX_KEY, ['string1', 'string2', 'string3'], [128, 128, 128]));

        $collection = $this->getDatabase()->getCollection('attributes');
        $this->assertCount(8, $collection->getAttribute('attributes'));
        $this->assertCount(3, $collection->getAttribute('indexes'));

        // Array
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'string_list', Database::VAR_STRING, 128, true, null, true, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'integer_list', Database::VAR_INTEGER, 0, true, null, true, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'float_list', Database::VAR_FLOAT, 0, true, null, true, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'boolean_list', Database::VAR_BOOLEAN, 0, true, null, true, true));

        $collection = $this->getDatabase()->getCollection('attributes');
        $this->assertCount(12, $collection->getAttribute('attributes'));

        // Default values
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'string_default', Database::VAR_STRING, 256, false, 'test'));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'integer_default', Database::VAR_INTEGER, 0, false, 1));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'float_default', Database::VAR_FLOAT, 0, false, 1.5));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'boolean_default', Database::VAR_BOOLEAN, 0, false, false));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'datetime_default', Database::VAR_DATETIME, 0, false, '2000-06-12T14:12:55.000+00:00', true, false, null, [], ['datetime']));

        $collection = $this->getDatabase()->getCollection('attributes');
        $this->assertCount(17, $collection->getAttribute('attributes'));

        // Delete
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'string1'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'string2'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'string3'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'string4'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'integer'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'bigint'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'float'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'boolean'));

        $collection = $this->getDatabase()->getCollection('attributes');
        $this->assertCount(9, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // Delete Array
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'string_list'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'integer_list'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'float_list'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'boolean_list'));

        $collection = $this->getDatabase()->getCollection('attributes');
        $this->assertCount(5, $collection->getAttribute('attributes'));

        // Delete default
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'string_default'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'integer_default'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'float_default'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'boolean_default'));
        $this->assertEquals(true, $this->getDatabase()->deleteAttribute('attributes', 'datetime_default'));

        $collection = $this->getDatabase()->getCollection('attributes');
        $this->assertCount(0, $collection->getAttribute('attributes'));

        // Test for custom chars in ID
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'as_5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'as5dasdasdas_', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', '.as5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', '-as5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'as-5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'as5dasdasdas-', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'socialAccountForYoutubeSubscribersss', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', '5f058a89258075f058a89258075f058t9214', Database::VAR_BOOLEAN, 0, true));

        // Test non-shared tables duplicates throw duplicate
        static::getDatabase()->createAttribute('attributes', 'duplicate', Database::VAR_STRING, 128, true);
        try {
            static::getDatabase()->createAttribute('attributes', 'duplicate', Database::VAR_STRING, 128, true);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }
    }

    /**
     * Using phpunit dataProviders to check that all these combinations of types/defaults throw exceptions
     * https://phpunit.de/manual/3.7/en/writing-tests-for-phpunit.html#writing-tests-for-phpunit.data-providers
     *
     * @return array<array<bool|float|int|string>>
     */
    public function invalidDefaultValues(): array
    {
        return [
            [Database::VAR_STRING, 1],
            [Database::VAR_STRING, 1.5],
            [Database::VAR_STRING, false],
            [Database::VAR_INTEGER, "one"],
            [Database::VAR_INTEGER, 1.5],
            [Database::VAR_INTEGER, true],
            [Database::VAR_FLOAT, 1],
            [Database::VAR_FLOAT, "one"],
            [Database::VAR_FLOAT, false],
            [Database::VAR_BOOLEAN, 0],
            [Database::VAR_BOOLEAN, "false"],
            [Database::VAR_BOOLEAN, 0.5],
        ];
    }

    /**
     * @depends      testCreateDeleteAttribute
     * @dataProvider invalidDefaultValues
     */
    public function testInvalidDefaultValues(string $type, mixed $default): void
    {
        $this->expectException(\Exception::class);
        $this->assertEquals(false, $this->getDatabase()->createAttribute('attributes', 'bad_default', $type, 256, true, $default));
    }

    /**
     * @depends testInvalidDefaultValues
     */
    public function testAttributeCaseInsensitivity(): void
    {
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'caseSensitive', Database::VAR_STRING, 128, true));
        $this->expectException(DuplicateException::class);
        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributes', 'CaseSensitive', Database::VAR_STRING, 128, true));
    }

    public function testAttributeKeyWithSymbols(): void
    {
        $this->getDatabase()->createCollection('attributesWithKeys');

        $this->assertEquals(true, $this->getDatabase()->createAttribute('attributesWithKeys', 'key_with.sym$bols', Database::VAR_STRING, 128, true));

        $document = $this->getDatabase()->createDocument('attributesWithKeys', new Document([
            'key_with.sym$bols' => 'value',
            '$permissions' => [
                Permission::read(Role::any()),
            ]
        ]));

        $this->assertEquals('value', $document->getAttribute('key_with.sym$bols'));

        $document = $this->getDatabase()->getDocument('attributesWithKeys', $document->getId());

        $this->assertEquals('value', $document->getAttribute('key_with.sym$bols'));
    }

    public function testCollectionNotFound(): void
    {
        try {
            $this->getDatabase()->find('not_exist', []);
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }
    }

    public function testAttributeNamesWithDots(): void
    {
        $this->getDatabase()->createCollection('dots.parent');

        $this->assertTrue($this->getDatabase()->createAttribute(
            collection: 'dots.parent',
            id: 'dots.name',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $document = $this->getDatabase()->find('dots.parent', [
            Query::select(['dots.name']),
        ]);
        $this->assertEmpty($document);

        $this->getDatabase()->createCollection('dots');

        $this->assertTrue($this->getDatabase()->createAttribute(
            collection: 'dots',
            id: 'name',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $this->getDatabase()->createRelationship(
            collection: 'dots.parent',
            relatedCollection: 'dots',
            type: Database::RELATION_ONE_TO_ONE
        );

        $this->getDatabase()->createDocument('dots.parent', new Document([
            '$id' => ID::custom('father'),
            'dots.name' => 'Bill clinton',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'dots' => [
                '$id' => ID::custom('child'),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ]
        ]));

        $documents = $this->getDatabase()->find('dots.parent', [
            Query::select(['*']),
        ]);

        $this->assertEquals('Bill clinton', $documents[0]['dots.name']);
    }

    /**
     * @depends testAttributeCaseInsensitivity
     */
    public function testIndexCaseInsensitivity(): void
    {
        $this->assertEquals(true, $this->getDatabase()->createIndex('attributes', 'key_caseSensitive', Database::INDEX_KEY, ['caseSensitive'], [128]));
        $this->expectException(DuplicateException::class);
        $this->assertEquals(true, $this->getDatabase()->createIndex('attributes', 'key_CaseSensitive', Database::INDEX_KEY, ['caseSensitive'], [128]));
    }

    /**
     * Ensure the collection is removed after use
     *
     * @depends testIndexCaseInsensitivity
     */
    public function testCleanupAttributeTests(): void
    {
        $this->getDatabase()->deleteCollection('attributes');
        $this->assertEquals(1, 1);
    }

    /**
     * @depends testCreateDeleteAttribute
     * @expectedException Exception
     */
    public function testUnknownFormat(): void
    {
        $this->expectException(\Exception::class);
        $this->assertEquals(false, $this->getDatabase()->createAttribute('attributes', 'bad_format', Database::VAR_STRING, 256, true, null, true, false, 'url'));
    }

    public function testCreateDeleteIndex(): void
    {
        $this->getDatabase()->createCollection('indexes');

        $this->assertEquals(true, $this->getDatabase()->createAttribute('indexes', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('indexes', 'order', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('indexes', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('indexes', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('indexes', 'boolean', Database::VAR_BOOLEAN, 0, true));

        // Indexes
        $this->assertEquals(true, $this->getDatabase()->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, $this->getDatabase()->createIndex('indexes', 'index2', Database::INDEX_KEY, ['float', 'integer'], [], [Database::ORDER_ASC, Database::ORDER_DESC]));
        $this->assertEquals(true, $this->getDatabase()->createIndex('indexes', 'index3', Database::INDEX_KEY, ['integer', 'boolean'], [], [Database::ORDER_ASC, Database::ORDER_DESC, Database::ORDER_DESC]));
        $this->assertEquals(true, $this->getDatabase()->createIndex('indexes', 'index4', Database::INDEX_UNIQUE, ['string'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, $this->getDatabase()->createIndex('indexes', 'index5', Database::INDEX_UNIQUE, ['$id', 'string'], [128], [Database::ORDER_ASC]));
        $this->assertEquals(true, $this->getDatabase()->createIndex('indexes', 'order', Database::INDEX_UNIQUE, ['order'], [128], [Database::ORDER_ASC]));

        $collection = $this->getDatabase()->getCollection('indexes');
        $this->assertCount(6, $collection->getAttribute('indexes'));

        // Delete Indexes
        $this->assertEquals(true, $this->getDatabase()->deleteIndex('indexes', 'index1'));
        $this->assertEquals(true, $this->getDatabase()->deleteIndex('indexes', 'index2'));
        $this->assertEquals(true, $this->getDatabase()->deleteIndex('indexes', 'index3'));
        $this->assertEquals(true, $this->getDatabase()->deleteIndex('indexes', 'index4'));
        $this->assertEquals(true, $this->getDatabase()->deleteIndex('indexes', 'index5'));
        $this->assertEquals(true, $this->getDatabase()->deleteIndex('indexes', 'order'));

        $collection = $this->getDatabase()->getCollection('indexes');
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // Test non-shared tables duplicates throw duplicate
        static::getDatabase()->createIndex('indexes', 'duplicate', Database::INDEX_KEY, ['string', 'boolean'], [128], [Database::ORDER_ASC]);
        try {
            static::getDatabase()->createIndex('indexes', 'duplicate', Database::INDEX_KEY, ['string', 'boolean'], [128], [Database::ORDER_ASC]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        static::getDatabase()->deleteCollection('indexes');
    }

    public function testCreateCollectionWithSchema(): void
    {
        $attributes = [
            new Document([
                '$id' => ID::custom('attribute1'),
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute2'),
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute3'),
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index2'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute2'],
                'lengths' => [],
                'orders' => ['DESC'],
            ]),
            new Document([
                '$id' => ID::custom('index3'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute3', 'attribute2'],
                'lengths' => [],
                'orders' => ['DESC', 'ASC'],
            ]),
        ];

        $collection = $this->getDatabase()->createCollection('withSchema', $attributes, $indexes);

        $this->assertEquals(false, $collection->isEmpty());
        $this->assertEquals('withSchema', $collection->getId());

        $this->assertIsArray($collection->getAttribute('attributes'));
        $this->assertCount(3, $collection->getAttribute('attributes'));
        $this->assertEquals('attribute1', $collection->getAttribute('attributes')[0]['$id']);
        $this->assertEquals(Database::VAR_STRING, $collection->getAttribute('attributes')[0]['type']);
        $this->assertEquals('attribute2', $collection->getAttribute('attributes')[1]['$id']);
        $this->assertEquals(Database::VAR_INTEGER, $collection->getAttribute('attributes')[1]['type']);
        $this->assertEquals('attribute3', $collection->getAttribute('attributes')[2]['$id']);
        $this->assertEquals(Database::VAR_BOOLEAN, $collection->getAttribute('attributes')[2]['type']);

        $this->assertIsArray($collection->getAttribute('indexes'));
        $this->assertCount(3, $collection->getAttribute('indexes'));
        $this->assertEquals('index1', $collection->getAttribute('indexes')[0]['$id']);
        $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[0]['type']);
        $this->assertEquals('index2', $collection->getAttribute('indexes')[1]['$id']);
        $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[1]['type']);
        $this->assertEquals('index3', $collection->getAttribute('indexes')[2]['$id']);
        $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[2]['type']);

        $this->getDatabase()->deleteCollection('withSchema');

        // Test collection with dash (+attribute +index)
        $collection2 = $this->getDatabase()->createCollection('with-dash', [
            new Document([
                '$id' => ID::custom('attribute-one'),
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [
            new Document([
                '$id' => ID::custom('index-one'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute-one'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ])
        ]);

        $this->assertEquals(false, $collection2->isEmpty());
        $this->assertEquals('with-dash', $collection2->getId());
        $this->assertIsArray($collection2->getAttribute('attributes'));
        $this->assertCount(1, $collection2->getAttribute('attributes'));
        $this->assertEquals('attribute-one', $collection2->getAttribute('attributes')[0]['$id']);
        $this->assertEquals(Database::VAR_STRING, $collection2->getAttribute('attributes')[0]['type']);
        $this->assertIsArray($collection2->getAttribute('indexes'));
        $this->assertCount(1, $collection2->getAttribute('indexes'));
        $this->assertEquals('index-one', $collection2->getAttribute('indexes')[0]['$id']);
        $this->assertEquals(Database::INDEX_KEY, $collection2->getAttribute('indexes')[0]['type']);
        $this->getDatabase()->deleteCollection('with-dash');
    }

    public function testCreateCollectionValidator(): void
    {
        $collections = [
            "validatorTest",
            "validator-test",
            "validator_test",
            "validator.test",
        ];

        $attributes = [
            new Document([
                '$id' => ID::custom('attribute1'),
                'type' => Database::VAR_STRING,
                'size' => 2500, // longer than 768
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute-2'),
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute_3'),
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute.4'),
                'type' => Database::VAR_BOOLEAN,
                'size' => 0,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute5'),
                'type' => Database::VAR_STRING,
                'size' => 2500,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ])
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index-2'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute-2'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index_3'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute_3'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index.4'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute.4'],
                'lengths' => [],
                'orders' => ['ASC'],
            ]),
            new Document([
                '$id' => ID::custom('index_2_attributes'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1', 'attribute5'],
                'lengths' => [200, 300],
                'orders' => ['DESC'],
            ]),
        ];

        foreach ($collections as $id) {
            $collection = $this->getDatabase()->createCollection($id, $attributes, $indexes);

            $this->assertEquals(false, $collection->isEmpty());
            $this->assertEquals($id, $collection->getId());

            $this->assertIsArray($collection->getAttribute('attributes'));
            $this->assertCount(5, $collection->getAttribute('attributes'));
            $this->assertEquals('attribute1', $collection->getAttribute('attributes')[0]['$id']);
            $this->assertEquals(Database::VAR_STRING, $collection->getAttribute('attributes')[0]['type']);
            $this->assertEquals('attribute-2', $collection->getAttribute('attributes')[1]['$id']);
            $this->assertEquals(Database::VAR_INTEGER, $collection->getAttribute('attributes')[1]['type']);
            $this->assertEquals('attribute_3', $collection->getAttribute('attributes')[2]['$id']);
            $this->assertEquals(Database::VAR_BOOLEAN, $collection->getAttribute('attributes')[2]['type']);
            $this->assertEquals('attribute.4', $collection->getAttribute('attributes')[3]['$id']);
            $this->assertEquals(Database::VAR_BOOLEAN, $collection->getAttribute('attributes')[3]['type']);

            $this->assertIsArray($collection->getAttribute('indexes'));
            $this->assertCount(5, $collection->getAttribute('indexes'));
            $this->assertEquals('index1', $collection->getAttribute('indexes')[0]['$id']);
            $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[0]['type']);
            $this->assertEquals('index-2', $collection->getAttribute('indexes')[1]['$id']);
            $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[1]['type']);
            $this->assertEquals('index_3', $collection->getAttribute('indexes')[2]['$id']);
            $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[2]['type']);
            $this->assertEquals('index.4', $collection->getAttribute('indexes')[3]['$id']);
            $this->assertEquals(Database::INDEX_KEY, $collection->getAttribute('indexes')[3]['type']);

            $this->getDatabase()->deleteCollection($id);
        }
    }

    public function testCreateDocument(): Document
    {
        $this->getDatabase()->createCollection('documents');

        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'integer_signed', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'integer_unsigned', Database::VAR_INTEGER, 4, true, signed: false));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'bigint_signed', Database::VAR_INTEGER, 8, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'bigint_unsigned', Database::VAR_INTEGER, 9, true, signed: false));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'float_signed', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'float_unsigned', Database::VAR_FLOAT, 0, true, signed: false));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'boolean', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'colors', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'empty', Database::VAR_STRING, 32, false, null, true, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents', 'with-dash', Database::VAR_STRING, 128, false, null));

        $document = $this->getDatabase()->createDocument('documents', new Document([
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
        $manualIdDocument = $this->getDatabase()->createDocument('documents', new Document([
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

        $manualIdDocument = $this->getDatabase()->getDocument('documents', '56000');

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

        $this->getDatabase()->createCollection($collection);

        $this->assertEquals(true, $this->getDatabase()->createAttribute($collection, 'string', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute($collection, 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute($collection, 'bigint', Database::VAR_INTEGER, 8, true));

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

        $documents = $this->getDatabase()->createDocuments($collection, $documents, 3);

        $this->assertEquals($count, count($documents));

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

    public function testRespectNulls(): Document
    {
        $this->getDatabase()->createCollection('documents_nulls');

        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents_nulls', 'string', Database::VAR_STRING, 128, false));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents_nulls', 'integer', Database::VAR_INTEGER, 0, false));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents_nulls', 'bigint', Database::VAR_INTEGER, 8, false));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents_nulls', 'float', Database::VAR_FLOAT, 0, false));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('documents_nulls', 'boolean', Database::VAR_BOOLEAN, 0, false));

        $document = $this->getDatabase()->createDocument('documents_nulls', new Document([
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
        $this->getDatabase()->createCollection('defaults');

        $this->assertEquals(true, $this->getDatabase()->createAttribute('defaults', 'string', Database::VAR_STRING, 128, false, 'default'));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('defaults', 'integer', Database::VAR_INTEGER, 0, false, 1));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('defaults', 'float', Database::VAR_FLOAT, 0, false, 1.5));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('defaults', 'boolean', Database::VAR_BOOLEAN, 0, false, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('defaults', 'colors', Database::VAR_STRING, 32, false, ['red', 'green', 'blue'], true, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('defaults', 'datetime', Database::VAR_DATETIME, 0, false, '2000-06-12T14:12:55.000+00:00', true, false, null, [], ['datetime']));

        $document = $this->getDatabase()->createDocument('defaults', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $document2 = $this->getDatabase()->getDocument('defaults', $document->getId());
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
        $this->getDatabase()->deleteCollection('defaults');
    }

    /**
     * @throws AuthorizationException|LimitException|DuplicateException|StructureException|Exception|Throwable
     */
    public function testIncreaseDecrease(): Document
    {
        $collection = 'increase_decrease';
        $this->getDatabase()->createCollection($collection);

        $this->assertEquals(true, $this->getDatabase()->createAttribute($collection, 'increase', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute($collection, 'decrease', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute($collection, 'increase_text', Database::VAR_STRING, 255, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute($collection, 'increase_float', Database::VAR_FLOAT, 0, true));

        $document = $this->getDatabase()->createDocument($collection, new Document([
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

        $document = $this->getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(101, $document->getAttribute('increase'));
        $this->assertNotEquals($updatedAt, $document->getUpdatedAt());

        $this->assertEquals(true, $this->getDatabase()->decreaseDocumentAttribute($collection, $document->getId(), 'decrease', 1, 98));
        $document = $this->getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(99, $document->getAttribute('decrease'));

        $this->assertEquals(true, $this->getDatabase()->increaseDocumentAttribute($collection, $document->getId(), 'increase_float', 5.5, 110));
        $document = $this->getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(105.5, $document->getAttribute('increase_float'));

        $this->assertEquals(true, $this->getDatabase()->decreaseDocumentAttribute($collection, $document->getId(), 'increase_float', 1.1, 100));
        $document = $this->getDatabase()->getDocument($collection, $document->getId());
        $this->assertEquals(104.4, $document->getAttribute('increase_float'));

        return $document;
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseLimitMax(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(true, $this->getDatabase()->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase', 10.5, 102.4));
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testDecreaseLimitMin(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(false, $this->getDatabase()->decreaseDocumentAttribute('increase_decrease', $document->getId(), 'decrease', 10, 99));
    }

    /**
     * @depends testIncreaseDecrease
     */
    public function testIncreaseTextAttribute(Document $document): void
    {
        $this->expectException(Exception::class);
        $this->assertEquals(false, $this->getDatabase()->increaseDocumentAttribute('increase_decrease', $document->getId(), 'increase_text'));
    }

    /**
     * @depends testCreateDocument
     */
    public function testGetDocument(Document $document): Document
    {
        $document = $this->getDatabase()->getDocument('documents', $document->getId());

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

        $document = $this->getDatabase()->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed']),
        ]);

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
        $this->assertArrayNotHasKey('$permissions', $document);
        $this->assertArrayNotHasKey('$collection', $document);

        $document = $this->getDatabase()->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed', '$id']),
        ]);

        $this->assertArrayHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayNotHasKey('$permissions', $document);
        $this->assertArrayNotHasKey('$collection', $document);

        $document = $this->getDatabase()->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed', '$permissions']),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayHasKey('$permissions', $document);
        $this->assertArrayNotHasKey('$collection', $document);

        $document = $this->getDatabase()->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed', '$internalId']),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayNotHasKey('$permissions', $document);
        $this->assertArrayNotHasKey('$collection', $document);

        $document = $this->getDatabase()->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed', '$collection']),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayNotHasKey('$permissions', $document);
        $this->assertArrayHasKey('$collection', $document);

        $document = $this->getDatabase()->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed', '$createdAt']),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayHasKey('$createdAt', $document);
        $this->assertArrayNotHasKey('$updatedAt', $document);
        $this->assertArrayNotHasKey('$permissions', $document);
        $this->assertArrayNotHasKey('$collection', $document);

        $document = $this->getDatabase()->getDocument('documents', $documentId, [
            Query::select(['string', 'integer_signed', '$updatedAt']),
        ]);

        $this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayNotHasKey('$internalId', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayHasKey('$updatedAt', $document);
        $this->assertArrayNotHasKey('$permissions', $document);
        $this->assertArrayNotHasKey('$collection', $document);

        return $document;
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

        $this->getDatabase()->createIndex('documents', 'fulltext_integer', Database::INDEX_FULLTEXT, ['string','integer_signed']);
    }

    public function testListDocumentSearch(): void
    {
        $fulltextSupport = $this->getDatabase()->getAdapter()->getSupportForFulltextIndex();
        if (!$fulltextSupport) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createIndex('documents', 'string', Database::INDEX_FULLTEXT, ['string']);
        $this->getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'string' => '*test+alias@email-provider.com',
            'integer_signed' => 0,
            'integer_unsigned' => 0,
            'bigint_signed' => 0,
            'bigint_unsigned' => 0,
            'float_signed' => -5.55,
            'float_unsigned' => 5.55,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
            'empty' => [],
        ]));

        /**
         * Allow reserved keywords for search
         */
        $documents = $this->getDatabase()->find('documents', [
            Query::search('string', '*test+alias@email-provider.com'),
        ]);

        $this->assertEquals(1, count($documents));
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

    public function testEmptySearch(): void
    {
        $fulltextSupport = $this->getDatabase()->getAdapter()->getSupportForFulltextIndex();
        if (!$fulltextSupport) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $documents = $this->getDatabase()->find('documents', [
            Query::search('string', ''),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = $this->getDatabase()->find('documents', [
            Query::search('string', '*'),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = $this->getDatabase()->find('documents', [
            Query::search('string', '<>'),
        ]);
        $this->assertEquals(0, count($documents));
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
     * @depends testCreateDocuments
     * @param array<Document> $documents
     */
    public function testUpdateDocuments(array $documents): void
    {
        $collection  = 'testCreateDocuments';

        foreach ($documents as $document) {
            $document
                ->setAttribute('string', 'text📝 updated')
                ->setAttribute('integer', 6)
                ->setAttribute('$permissions', [
                    Permission::read(Role::users()),
                    Permission::create(Role::users()),
                    Permission::update(Role::users()),
                    Permission::delete(Role::users()),
                ]);
        }

        $documents = $this->getDatabase()->updateDocuments(
            $collection,
            $documents,
            \count($documents)
        );

        foreach ($documents as $document) {
            $this->assertEquals('text📝 updated', $document->getAttribute('string'));
            $this->assertEquals(6, $document->getAttribute('integer'));
        }

        $documents = $this->getDatabase()->find($collection, [
            Query::limit(\count($documents))
        ]);

        foreach ($documents as $document) {
            $this->assertEquals('text📝 updated', $document->getAttribute('string'));
            $this->assertEquals(6, $document->getAttribute('integer'));
            $this->assertEquals([
                Permission::read(Role::users()),
                Permission::create(Role::users()),
                Permission::update(Role::users()),
                Permission::delete(Role::users()),
            ], $document->getAttribute('$permissions'));
        }
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


    /**
     * @throws AuthorizationException
     * @throws DuplicateException
     * @throws ConflictException
     * @throws LimitException
     * @throws StructureException
     */
    public function testArrayAttribute(): void
    {
        self::$authorization->addRole(Role::any()->toString());

        $database = $this->getDatabase();
        $collection = 'json';
        $permissions = [Permission::read(Role::any())];

        $database->createCollection($collection, permissions: [
            Permission::create(Role::any()),
        ]);

        $this->assertEquals(true, $database->createAttribute(
            $collection,
            'booleans',
            Database::VAR_BOOLEAN,
            size: 0,
            required: true,
            array: true
        ));

        $this->assertEquals(true, $database->createAttribute(
            $collection,
            'names',
            Database::VAR_STRING,
            size: 255, // Does this mean each Element max is 255? We need to check this on Structure validation?
            required: false,
            array: true
        ));

        $this->assertEquals(true, $database->createAttribute(
            $collection,
            'numbers',
            Database::VAR_INTEGER,
            size: 0,
            required: false,
            array: true
        ));

        $this->assertEquals(true, $database->createAttribute(
            $collection,
            'age',
            Database::VAR_INTEGER,
            size: 0,
            required: false,
            signed: false
        ));

        $this->assertEquals(true, $database->createAttribute(
            $collection,
            'tv_show',
            Database::VAR_STRING,
            size: 700,
            required: false,
            signed: false,
        ));

        $this->assertEquals(true, $database->createAttribute(
            $collection,
            'short',
            Database::VAR_STRING,
            size: 5,
            required: false,
            signed: false,
            array: true
        ));

        $this->assertEquals(true, $database->createAttribute(
            $collection,
            'pref',
            Database::VAR_STRING,
            size: 16384,
            required: false,
            signed: false,
            filters: ['json'],
        ));

        try {
            $database->createDocument($collection, new Document([]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid document structure: Missing required attribute "booleans"', $e->getMessage());
        }

        $database->updateAttribute($collection, 'booleans', required: false);

        $doc = $database->getCollection($collection);
        $attribute = $doc->getAttribute('attributes')[0];
        $this->assertEquals('boolean', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(true, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);

        try {
            $database->createDocument($collection, new Document([
                'short' => ['More than 5 size'],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid document structure: Attribute "short[\'0\']" has invalid type. Value must be a valid string and no longer than 5 chars', $e->getMessage());
        }

        try {
            $database->createDocument($collection, new Document([
                'names' => ['Joe', 100],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid document structure: Attribute "names[\'1\']" has invalid type. Value must be a valid string and no longer than 255 chars', $e->getMessage());
        }

        try {
            $database->createDocument($collection, new Document([
                'age' => 1.5,
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid document structure: Attribute "age" has invalid type. Value must be a valid integer', $e->getMessage());
        }

        try {
            $database->createDocument($collection, new Document([
                'age' => -100,
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid document structure: Attribute "age" has invalid type. Value must be a valid range between 0 and 2,147,483,647', $e->getMessage());
        }

        $database->createDocument($collection, new Document([
            '$id' => 'id1',
            '$permissions' => $permissions,
            'booleans' => [false],
            'names' => ['Joe', 'Antony', '100'],
            'numbers' => [0, 100, 1000, -1],
            'age' => 41,
            'tv_show' => 'Everybody Loves Raymond',
            'pref' => [
                'fname' => 'Joe',
                'lname' => 'Baiden',
                'age' => 80,
                'male' => true,
            ],
        ]));

        $document = $database->getDocument($collection, 'id1');

        $this->assertEquals(false, $document->getAttribute('booleans')[0]);
        $this->assertEquals('Antony', $document->getAttribute('names')[1]);
        $this->assertEquals(100, $document->getAttribute('numbers')[1]);

        try {
            $database->createIndex($collection, 'indx', Database::INDEX_FULLTEXT, ['names']);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            if ($this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
                $this->assertEquals('"Fulltext" index is forbidden on array attributes', $e->getMessage());
            } else {
                $this->assertEquals('Fulltext index is not supported', $e->getMessage());
            }
        }

        try {
            $database->createIndex($collection, 'indx', Database::INDEX_KEY, ['numbers', 'names'], [100,100]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('An index may only contain one array attribute', $e->getMessage());
        }

        $this->assertEquals(true, $database->createAttribute(
            $collection,
            'long_size',
            Database::VAR_STRING,
            size: 2000,
            required: false,
            array: true
        ));

        if ($database->getAdapter()->getMaxIndexLength() > 0) {
            // If getMaxIndexLength() > 0 We clear length for array attributes
            $database->createIndex($collection, 'indx1', Database::INDEX_KEY, ['long_size'], [], []);
            $database->createIndex($collection, 'indx2', Database::INDEX_KEY, ['long_size'], [1000], []);

            try {
                $database->createIndex($collection, 'indx_numbers', Database::INDEX_KEY, ['tv_show', 'numbers'], [], []); // [700, 255]
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertEquals('Index length is longer than the maximum: 768', $e->getMessage());
            }
        }

        // We clear orders for array attributes
        $database->createIndex($collection, 'indx3', Database::INDEX_KEY, ['names'], [255], ['desc']);

        try {
            $database->createIndex($collection, 'indx4', Database::INDEX_KEY, ['age', 'names'], [10, 255], []);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Cannot set a length on "integer" attributes', $e->getMessage());
        }

        $this->assertTrue($database->createIndex($collection, 'indx6', Database::INDEX_KEY, ['age', 'names'], [null, 999], []));
        $this->assertTrue($database->createIndex($collection, 'indx7', Database::INDEX_KEY, ['age', 'booleans'], [0, 999], []));

        if ($this->getDatabase()->getAdapter()->getSupportForQueryContains()) {
            try {
                $database->find($collection, [
                    Query::equal('names', ['Joe']),
                ]);
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertEquals('Invalid query: Cannot query equal on attribute "names" because it is an array.', $e->getMessage());
            }

            try {
                $database->find($collection, [
                    Query::contains('age', [10])
                ]);
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertEquals('Invalid query: Cannot query contains on attribute "age" because it is not an array or string.', $e->getMessage());
            }

            $documents = $database->find($collection, [
                Query::isNull('long_size')
            ]);
            $this->assertCount(1, $documents);

            $documents = $database->find($collection, [
                Query::contains('tv_show', ['love'])
            ]);
            $this->assertCount(1, $documents);

            $documents = $database->find($collection, [
                Query::contains('names', ['Jake', 'Joe'])
            ]);
            $this->assertCount(1, $documents);

            $documents = $database->find($collection, [
                Query::contains('numbers', [-1, 0, 999])
            ]);
            $this->assertCount(1, $documents);

            $documents = $database->find($collection, [
                Query::contains('booleans', [false, true])
            ]);
            $this->assertCount(1, $documents);

            // Regular like query on primitive json string data
            $documents = $database->find($collection, [
                Query::contains('pref', ['Joe'])
            ]);
            $this->assertCount(1, $documents);
        }
    }

    /**
     * @return array<string, mixed>
     */
    public function testFind(): array
    {
        self::$authorization->addRole(Role::any()->toString());

        $this->getDatabase()->createCollection('movies', permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users())
        ]);

        $this->assertEquals(true, $this->getDatabase()->createAttribute('movies', 'name', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('movies', 'director', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('movies', 'year', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('movies', 'price', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('movies', 'active', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('movies', 'genres', Database::VAR_STRING, 32, true, null, true, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('movies', 'with-dash', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('movies', 'nullable', Database::VAR_STRING, 128, false));

        $document = $this->getDatabase()->createDocument('movies', new Document([
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

        $this->getDatabase()->createDocument('movies', new Document([
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

        $this->getDatabase()->createDocument('movies', new Document([
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

        $this->getDatabase()->createDocument('movies', new Document([
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

        $this->getDatabase()->createDocument('movies', new Document([
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

        $this->getDatabase()->createDocument('movies', new Document([
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

    public function testFindBasicChecks(): void
    {
        $documents = $this->getDatabase()->find('movies');
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
        $documents = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('$id'),
        ]);
        $this->assertEquals($lastDocumentId, $documents[0]->getId());
        $documents = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderAsc('$id'),
        ]);
        $this->assertEquals($firstDocumentId, $documents[0]->getId());

        /**
         * Check internal numeric ID sorting
         */
        $documents = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);
        $this->assertEquals($movieDocuments[\count($movieDocuments) - 1]->getId(), $documents[0]->getId());
        $documents = $this->getDatabase()->find('movies', [
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
        self::$authorization->addRole('user:x');
        $documents = $this->getDatabase()->find('movies');

        $this->assertEquals(6, count($documents));
    }

    public function testFindCheckInteger(): void
    {
        /**
         * Query with dash attribute
         */
        $documents = $this->getDatabase()->find('movies', [
            Query::equal('with-dash', ['Works']),
        ]);

        $this->assertEquals(2, count($documents));

        $documents = $this->getDatabase()->find('movies', [
            Query::equal('with-dash', ['Works2', 'Works3']),
        ]);

        $this->assertEquals(4, count($documents));

        /**
         * Check an Integer condition
         */
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
            Query::equal('active', [true]),
        ]);

        $this->assertEquals(4, count($documents));
    }

    public function testFindStringQueryEqual(): void
    {
        /**
         * String condition
         */
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
            Query::between('price', 25.94, 25.99),
        ]);
        $this->assertEquals(2, count($documents));

        $documents = $this->getDatabase()->find('movies', [
            Query::between('price', 30, 35),
        ]);
        $this->assertEquals(0, count($documents));

        $documents = $this->getDatabase()->find('movies', [
            Query::between('$createdAt', '1975-12-06', '2050-12-06'),
        ]);
        $this->assertEquals(6, count($documents));

        $documents = $this->getDatabase()->find('movies', [
            Query::between('$updatedAt', '1975-12-06T07:08:49.733+02:00', '2050-02-05T10:15:21.825+00:00'),
        ]);
        $this->assertEquals(6, count($documents));
    }

    public function testFindFloat(): void
    {
        /**
         * Float condition
         */
        $documents = $this->getDatabase()->find('movies', [
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

        $documents = $this->getDatabase()->find('movies', [
            Query::contains('genres', ['comics'])
        ]);

        $this->assertEquals(2, count($documents));

        /**
         * Array contains OR condition
         */
        $documents = $this->getDatabase()->find('movies', [
            Query::contains('genres', ['comics', 'kids']),
        ]);

        $this->assertEquals(4, count($documents));

        $documents = $this->getDatabase()->find('movies', [
            Query::contains('genres', ['non-existent']),
        ]);

        $this->assertEquals(0, count($documents));

        try {
            $this->getDatabase()->find('movies', [
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
            $success = $this->getDatabase()->createIndex('movies', 'name', Database::INDEX_FULLTEXT, ['name']);
            $this->assertEquals(true, $success);

            $documents = $this->getDatabase()->find('movies', [
                Query::search('name', 'captain'),
            ]);

            $this->assertEquals(2, count($documents));

            /**
             * Fulltext search (wildcard)
             */

            // TODO: Looks like the MongoDB implementation is a bit more complex, skipping that for now.
            // TODO: I think this needs a changes? how do we distinguish between regular full text and wildcard?

            if ($this->getDatabase()->getAdapter()->getSupportForFulltextWildCardIndex()) {
                $documents = $this->getDatabase()->find('movies', [
                    Query::search('name', 'cap'),
                ]);

                $this->assertEquals(2, count($documents));
            }
        }

        $this->assertEquals(true, true); // Test must do an assertion
    }

    public function testFindFulltextSpecialChars(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForFulltextIndex()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'full_text';
        $this->getDatabase()->createCollection($collection, permissions: [
            Permission::create(Role::any()),
            Permission::update(Role::users())
        ]);

        $this->assertTrue($this->getDatabase()->createAttribute($collection, 'ft', Database::VAR_STRING, 128, true));
        $this->assertTrue($this->getDatabase()->createIndex($collection, 'ft-index', Database::INDEX_FULLTEXT, ['ft']));

        $this->getDatabase()->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'Alf: chapter_4@nasa.com'
        ]));

        $documents = $this->getDatabase()->find($collection, [
            Query::search('ft', 'chapter_4'),
        ]);
        $this->assertEquals(1, count($documents));

        $this->getDatabase()->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'al@ba.io +-*)(<>~'
        ]));

        $documents = $this->getDatabase()->find($collection, [
            Query::search('ft', 'al@ba.io'), // === al ba io*
        ]);

        if ($this->getDatabase()->getAdapter()->getSupportForFulltextWildcardIndex()) {
            $this->assertEquals(0, count($documents));
        } else {
            $this->assertEquals(1, count($documents));
        }

        $this->getDatabase()->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'donald duck'
        ]));

        $this->getDatabase()->createDocument($collection, new Document([
            '$permissions' => [Permission::read(Role::any())],
            'ft' => 'donald trump'
        ]));

        $documents = $this->getDatabase()->find($collection, [
            Query::search('ft', 'donald trump'),
            Query::orderAsc('ft'),
        ]);
        $this->assertEquals(2, count($documents));

        $documents = $this->getDatabase()->find($collection, [
            Query::search('ft', '"donald trump"'), // Exact match
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindMultipleConditions(): void
    {
        /**
         * Multiple conditions
         */
        $documents = $this->getDatabase()->find('movies', [
            Query::equal('director', ['TBD']),
            Query::equal('year', [2026]),
        ]);

        $this->assertEquals(1, count($documents));

        /**
         * Multiple conditions and OR values
         */
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
            Query::equal('$internalId', [$data['$internalId']]),
        ]);

        $this->assertEquals(1, count($documents));
    }

    /**
     * @return void
     * @throws \Utopia\Database\Exception
     */
    public function testSelectInternalID(): void
    {
        $documents = $this->getDatabase()->find('movies', [
            Query::select(['$internalId', '$id']),
            Query::orderAsc(''),
            Query::limit(1),
        ]);

        $document = $documents[0];

        $this->assertArrayHasKey('$internalId', $document);
        $this->assertCount(2, $document);

        $document = $this->getDatabase()->getDocument('movies', $document->getId(), [
            Query::select(['$internalId']),
        ]);

        $this->assertArrayHasKey('$internalId', $document);
        $this->assertCount(1, $document);
    }

    public function testFindOrderBy(): void
    {
        /**
         * ORDER BY
         */
        $documents = $this->getDatabase()->find('movies', [
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
        $base = array_reverse($this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]));
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
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
        $movies = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = $this->getDatabase()->find('movies', [
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
        $movies = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = $this->getDatabase()->find('movies', [
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
        $movies = array_reverse($this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
        ]));

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = $this->getDatabase()->find('movies', [
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
        $movies = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc(''),
        ]);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc(''),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = $this->getDatabase()->find('movies', [
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
        $movies = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('year')
        ]);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[1])
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = $this->getDatabase()->find('movies', [
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
        $movies = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('year')
        ]);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[5])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[1]['name'], $documents[0]['name']);
        $this->assertEquals($movies[2]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('year'),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = $this->getDatabase()->find('movies', [
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
        $movies = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year')
        ]);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[1])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[3])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[4]['name'], $documents[0]['name']);
        $this->assertEquals($movies[5]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorAfter($movies[4])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[5]['name'], $documents[0]['name']);

        $documents = $this->getDatabase()->find('movies', [
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
        $movies = $this->getDatabase()->find('movies', [
            Query::limit(25),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year')
        ]);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[5])
        ]);

        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[3]['name'], $documents[0]['name']);
        $this->assertEquals($movies[4]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[4])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[2]['name'], $documents[0]['name']);
        $this->assertEquals($movies[3]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[2])
        ]);
        $this->assertEquals(2, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);
        $this->assertEquals($movies[1]['name'], $documents[1]['name']);

        $documents = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
            Query::orderAsc('year'),
            Query::cursorBefore($movies[1])
        ]);
        $this->assertEquals(1, count($documents));
        $this->assertEquals($movies[0]['name'], $documents[0]['name']);

        $documents = $this->getDatabase()->find('movies', [
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
        $documentsTest = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('price'),
        ]);
        $documents = $this->getDatabase()->find('movies', [
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
        $documentsTest = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$id'),
        ]);
        $documents = $this->getDatabase()->find('movies', [
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
        $documentsTest = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$createdAt'),
        ]);

        $documents = $this->getDatabase()->find('movies', [
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
        $documentsTest = $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::orderDesc('$updatedAt'),
        ]);
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
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
        $documents = $this->getDatabase()->find('movies', [
            Query::equal('director', ['TBD', 'Joe Johnston']),
            Query::equal('year', [2025]),
        ]);
        $this->assertEquals(1, count($documents));
    }

    public function testFindOrderByAfterException(): void
    {
        /**
         * ORDER BY - After Exception
         * Must be last assertion in test
         */
        $document = new Document([
            '$collection' => 'other collection'
        ]);

        $this->expectException(Exception::class);
        $this->getDatabase()->find('movies', [
            Query::limit(2),
            Query::offset(0),
            Query::cursorAfter($document)
        ]);
    }

    /**
     * @depends testUpdateDocument
     */
    public function testFindEdgeCases(Document $document): void
    {
        $collection = 'edgeCases';

        $this->getDatabase()->createCollection($collection);

        $this->assertEquals(true, $this->getDatabase()->createAttribute($collection, 'value', Database::VAR_STRING, 256, true));

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
            $this->getDatabase()->createDocument($collection, new Document([
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
        $documents = $this->getDatabase()->find($collection);

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
            $documents = $this->getDatabase()->find($collection, [
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
            $this->getDatabase()->find('movies', [
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
        $this->assertCount(4, $this->getDatabase()->find('movies', $queries));
        $this->assertEquals(4, $this->getDatabase()->count('movies', $queries));

        $queries = [
            Query::equal('active', [true]),
            Query::or([
                Query::equal('name', ['Frozen']),
                Query::equal('name', ['Frozen II']),
                Query::equal('director', ['Joe Johnston'])
            ])
        ];

        $this->assertCount(3, $this->getDatabase()->find('movies', $queries));
        $this->assertEquals(3, $this->getDatabase()->count('movies', $queries));
    }

    public function testOrNested(): void
    {
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

        $documents = $this->getDatabase()->find('movies', $queries);
        $this->assertCount(1, $documents);
        $this->assertArrayNotHasKey('name', $documents[0]);

        $count = $this->getDatabase()->count('movies', $queries);
        $this->assertEquals(1, $count);
    }

    public function testAndSingleQuery(): void
    {
        try {
            $this->getDatabase()->find('movies', [
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
        $this->assertCount(1, $this->getDatabase()->find('movies', $queries));
        $this->assertEquals(1, $this->getDatabase()->count('movies', $queries));
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

        $documents = $this->getDatabase()->find('movies', $queries);
        $this->assertCount(3, $documents);

        $count = $this->getDatabase()->count('movies', $queries);
        $this->assertEquals(3, $count);
    }

    public function testNestedIDQueries(): void
    {
        self::$authorization->addRole(Role::any()->toString());

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

    /**
     * @depends testFind
     */
    public function testFindOne(): void
    {
        $document = $this->getDatabase()->findOne('movies', [
            Query::offset(2),
            Query::orderAsc('name')
        ]);

        $this->assertFalse($document->isEmpty());
        $this->assertEquals('Frozen', $document->getAttribute('name'));

        $document = $this->getDatabase()->findOne('movies', [
            Query::offset(10)
        ]);
        $this->assertTrue($document->isEmpty());
    }

    public function testFindNull(): void
    {
        $documents = $this->getDatabase()->find('movies', [
            Query::isNull('nullable'),
        ]);

        $this->assertEquals(5, count($documents));
    }

    public function testFindNotNull(): void
    {
        $documents = $this->getDatabase()->find('movies', [
            Query::isNotNull('nullable'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindStartsWith(): void
    {
        $documents = $this->getDatabase()->find('movies', [
            Query::startsWith('name', 'Work'),
        ]);

        $this->assertEquals(2, count($documents));

        if ($this->getDatabase()->getAdapter() instanceof SQL) {
            $documents = $this->getDatabase()->find('movies', [
                Query::startsWith('name', '%ork'),
            ]);
        } else {
            $documents = $this->getDatabase()->find('movies', [
                Query::startsWith('name', '.*ork'),
            ]);
        }

        $this->assertEquals(0, count($documents));
    }

    public function testFindStartsWithWords(): void
    {
        $documents = $this->getDatabase()->find('movies', [
            Query::startsWith('name', 'Work in Progress'),
        ]);

        $this->assertEquals(2, count($documents));
    }

    public function testFindEndsWith(): void
    {
        $documents = $this->getDatabase()->find('movies', [
            Query::endsWith('name', 'Marvel'),
        ]);

        $this->assertEquals(1, count($documents));
    }

    public function testFindSelect(): void
    {
        $documents = $this->getDatabase()->find('movies', [
            Query::select(['name', 'year'])
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

        $documents = $this->getDatabase()->find('movies', [
            Query::select(['name', 'year', '$id'])
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

        $documents = $this->getDatabase()->find('movies', [
            Query::select(['name', 'year', '$internalId'])
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

        $documents = $this->getDatabase()->find('movies', [
            Query::select(['name', 'year', '$collection'])
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

        $documents = $this->getDatabase()->find('movies', [
            Query::select(['name', 'year', '$createdAt'])
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

        $documents = $this->getDatabase()->find('movies', [
            Query::select(['name', 'year', '$updatedAt'])
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

        $documents = $this->getDatabase()->find('movies', [
            Query::select(['name', 'year', '$permissions'])
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

    /**
     * @depends testFind
     */
    public function testCount(): void
    {
        $count = $this->getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        $count = $this->getDatabase()->count('movies', [Query::equal('year', [2019])]);
        $this->assertEquals(2, $count);
        $count = $this->getDatabase()->count('movies', [Query::equal('with-dash', ['Works'])]);
        $this->assertEquals(2, $count);
        $count = $this->getDatabase()->count('movies', [Query::equal('with-dash', ['Works2', 'Works3'])]);
        $this->assertEquals(4, $count);

        self::$authorization->removeRole('user:x');
        $count = $this->getDatabase()->count('movies');
        $this->assertEquals(5, $count);

        self::$authorization->disable();
        $count = $this->getDatabase()->count('movies');
        $this->assertEquals(6, $count);
        self::$authorization->reset();

        self::$authorization->disable();
        $count = $this->getDatabase()->count('movies', [], 3);
        $this->assertEquals(3, $count);
        self::$authorization->reset();

        /**
         * Test that OR queries are handled correctly
         */
        self::$authorization->disable();
        $count = $this->getDatabase()->count('movies', [
            Query::equal('director', ['TBD', 'Joe Johnston']),
            Query::equal('year', [2025]),
        ]);
        $this->assertEquals(1, $count);
        self::$authorization->reset();
    }

    /**
     * @depends testFind
     */
    public function testSum(): void
    {
        self::$authorization->addRole('user:x');
        $sum = $this->getDatabase()->sum('movies', 'year', [Query::equal('year', [2019]),]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = $this->getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025 + 2026, $sum);
        $sum = $this->getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = $this->getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));

        $sum = $this->getDatabase()->sum('movies', 'year', [Query::equal('year', [2019])], 1);
        $this->assertEquals(2019, $sum);

        self::$authorization->removeRole('user:x');
        self::$authorization->removeRole('userx');
        $sum = $this->getDatabase()->sum('movies', 'year', [Query::equal('year', [2019]),]);
        $this->assertEquals(2019 + 2019, $sum);
        $sum = $this->getDatabase()->sum('movies', 'year');
        $this->assertEquals(2013 + 2019 + 2011 + 2019 + 2025, $sum);
        $sum = $this->getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
        $this->assertEquals(round(39.50 + 25.99, 2), round($sum, 2));
        $sum = $this->getDatabase()->sum('movies', 'price', [Query::equal('year', [2019]),]);
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

        $result = $this->getDatabase()->encode($collection, $document);

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

        $result = $this->getDatabase()->decode($collection, $document);

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
     * @depends testCreateDocument
     */
    public function testReadPermissionsSuccess(Document $document): Document
    {
        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $document = $this->getDatabase()->createDocument('documents', new Document([
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

        self::$authorization->cleanRoles();

        $document = $this->getDatabase()->getDocument($document->getCollection(), $document->getId());
        $this->assertEquals(true, $document->isEmpty());

        self::$authorization->addRole(Role::any()->toString());

        return $document;
    }

    public function testReadPermissionsFailure(): Document
    {
        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $document = $this->getDatabase()->createDocument('documents', new Document([
            '$permissions' => [
                Permission::read(Role::user('1')),
                Permission::create(Role::user('1')),
                Permission::update(Role::user('1')),
                Permission::delete(Role::user('1')),
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

        self::$authorization->cleanRoles();

        $document = $this->getDatabase()->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $document->isEmpty());

        self::$authorization->addRole(Role::any()->toString());

        return $document;
    }

    /**
     * @depends testCreateDocument
     */
    public function testWritePermissionsSuccess(Document $document): void
    {
        self::$authorization->cleanRoles();

        $this->expectException(AuthorizationException::class);
        $this->getDatabase()->createDocument('documents', new Document([
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

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $document = $this->getDatabase()->createDocument('documents', new Document([
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

        self::$authorization->cleanRoles();

        $document = $this->getDatabase()->updateDocument('documents', $document->getId(), new Document([
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

    public function testNoChangeUpdateDocumentWithoutPermission(): Document
    {
        $document = $this->getDatabase()->createDocument('documents', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::INT_MAX,
            'integer_unsigned' => Database::INT_MAX,
            'bigint_signed' => -Database::BIG_INT_MAX,
            'bigint_unsigned' => Database::BIG_INT_MAX,
            'float_signed' => -123456789.12346,
            'float_unsigned' => 123456789.12346,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $updatedDocument = $this->getDatabase()->updateDocument(
            'documents',
            $document->getId(),
            $document
        );

        // Document should not be updated as there is no change.
        // It should also not throw any authorization exception without any permission because of no change.
        $this->assertEquals($updatedDocument->getUpdatedAt(), $document->getUpdatedAt());

        $document = $this->getDatabase()->createDocument('documents', new Document([
            '$id' => ID::unique(),
            '$permissions' => [],
            'string' => 'text📝',
            'integer_signed' => -Database::INT_MAX,
            'integer_unsigned' => Database::INT_MAX,
            'bigint_signed' => -Database::BIG_INT_MAX,
            'bigint_unsigned' => Database::BIG_INT_MAX,
            'float_signed' => -123456789.12346,
            'float_unsigned' => 123456789.12346,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        // Should throw exception, because nothing was updated, but there was no read permission
        try {
            $this->getDatabase()->updateDocument(
                'documents',
                $document->getId(),
                $document
            );
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }

        return $document;
    }

    public function testStructureValidationAfterRelationsAttribute(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection("structure_1", [], [], [Permission::create(Role::any())]);
        $this->getDatabase()->createCollection("structure_2", [], [], [Permission::create(Role::any())]);

        $this->getDatabase()->createRelationship(
            collection: "structure_1",
            relatedCollection: "structure_2",
            type: Database::RELATION_ONE_TO_ONE,
        );

        try {
            $this->getDatabase()->createDocument('structure_1', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'structure_2' => '100',
                'name' => 'Frozen', // Unknown attribute 'name' after relation attribute
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }
    }

    public function testNoChangeUpdateDocumentWithRelationWithoutPermission(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $attribute = new Document([
            '$id' => ID::custom("name"),
            'type' => Database::VAR_STRING,
            'size' => 100,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ]);

        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::delete(Role::any()),
        ];
        for ($i = 1; $i < 6; $i++) {
            $this->getDatabase()->createCollection("level{$i}", [$attribute], [], $permissions);
        }

        for ($i = 1; $i < 5; $i++) {
            $collectionId = $i;
            $relatedCollectionId = $i + 1;
            $this->getDatabase()->createRelationship(
                collection: "level{$collectionId}",
                relatedCollection: "level{$relatedCollectionId}",
                type: Database::RELATION_ONE_TO_ONE,
                id: "level{$relatedCollectionId}"
            );
        }

        // Create document with relationship with nested data
        $level1 = $this->getDatabase()->createDocument('level1', new Document([
            '$id' => 'level1',
            '$permissions' => [],
            'name' => 'Level 1',
            'level2' => [
                '$id' => 'level2',
                '$permissions' => [],
                'name' => 'Level 2',
                'level3' => [
                    '$id' => 'level3',
                    '$permissions' => [],
                    'name' => 'Level 3',
                    'level4' => [
                        '$id' => 'level4',
                        '$permissions' => [],
                        'name' => 'Level 4',
                        'level5' => [
                            '$id' => 'level5',
                            '$permissions' => [],
                            'name' => 'Level 5',
                        ]
                    ],
                ],
            ],
        ]));
        $this->getDatabase()->updateDocument('level1', $level1->getId(), new Document($level1->getArrayCopy()));
        $updatedLevel1 = $this->getDatabase()->getDocument('level1', $level1->getId());
        $this->assertEquals($level1, $updatedLevel1);

        try {
            $this->getDatabase()->updateDocument('level1', $level1->getId(), $level1->setAttribute('name', 'haha'));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }
        $level1->setAttribute('name', 'Level 1');
        $this->getDatabase()->updateCollection('level3', [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ], false);
        $level2 = $level1->getAttribute('level2');
        $level3 = $level2->getAttribute('level3');

        $level3->setAttribute('name', 'updated value');
        $level2->setAttribute('level3', $level3);
        $level1->setAttribute('level2', $level2);

        $level1 = $this->getDatabase()->updateDocument('level1', $level1->getId(), $level1);
        $this->assertEquals('updated value', $level1['level2']['level3']['name']);

        for ($i = 1; $i < 6; $i++) {
            $this->getDatabase()->deleteCollection("level{$i}");
        }
    }

    public function testExceptionAttributeLimit(): void
    {
        if ($this->getDatabase()->getLimitForAttributes() > 0) {
            // Load the collection up to the limit
            $attributes = [];
            for ($i = 0; $i < $this->getDatabase()->getLimitForAttributes(); $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test{$i}"),
                    'type' => Database::VAR_INTEGER,
                    'size' => 0,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            $this->getDatabase()->createCollection('attributeLimit', $attributes);

            $this->expectException(LimitException::class);
            $this->assertEquals(false, $this->getDatabase()->createAttribute('attributeLimit', "breaking", Database::VAR_INTEGER, 0, true));
        }

        // Default assertion for other adapters
        $this->assertEquals(1, 1);
    }

    /**
     * @depends testExceptionAttributeLimit
     */
    public function testCheckAttributeCountLimit(): void
    {
        if ($this->getDatabase()->getLimitForAttributes() > 0) {
            $collection = $this->getDatabase()->getCollection('attributeLimit');

            // create same attribute in testExceptionAttributeLimit
            $attribute = new Document([
                '$id' => ID::custom('breaking'),
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => true,
                'default' => null,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]);

            $this->expectException(LimitException::class);
            $this->assertEquals(false, $this->getDatabase()->checkAttribute($collection, $attribute));
        }

        // Default assertion for other adapters
        $this->assertEquals(1, 1);
    }

    /**
     * Using phpunit dataProviders to check that all these combinations of types/sizes throw exceptions
     * https://phpunit.de/manual/3.7/en/writing-tests-for-phpunit.html#writing-tests-for-phpunit.data-providers
     *
     * @return array<array<int>>
     */
    public function rowWidthExceedsMaximum(): array
    {
        return [
            // These combinations of attributes gets exactly to the 64k limit
            // [$key, $stringSize, $stringCount, $intCount, $floatCount, $boolCount]
            // [0, 1024, 15, 0, 731, 3],
            // [1, 512, 31, 0, 0, 833],
            // [2, 256, 62, 128, 0, 305],
            // [3, 128, 125, 30, 24, 2],
            //
            // Taken 500 bytes off for tests
            [0, 1024, 15, 0, 304, 3],
            [1, 512, 31, 0, 0, 333],
            [2, 256, 62, 103, 0, 5],
            [3, 128, 124, 30, 12, 14],
        ];
    }

    /**
     * @dataProvider rowWidthExceedsMaximum
     */
    public function testExceptionWidthLimit(int $key, int $stringSize, int $stringCount, int $intCount, int $floatCount, int $boolCount): void
    {
        if ($this->getDatabase()->getAdapter()::getDocumentSizeLimit() > 0) {
            $attributes = [];

            // Load the collection up to the limit
            // Strings
            for ($i = 0; $i < $stringCount; $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test_string{$i}"),
                    'type' => Database::VAR_STRING,
                    'size' => $stringSize,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            // Integers
            for ($i = 0; $i < $intCount; $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test_int{$i}"),
                    'type' => Database::VAR_INTEGER,
                    'size' => 0,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            // Floats
            for ($i = 0; $i < $floatCount; $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test_float{$i}"),
                    'type' => Database::VAR_FLOAT,
                    'size' => 0,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            // Booleans
            for ($i = 0; $i < $boolCount; $i++) {
                $attributes[] = new Document([
                    '$id' => ID::custom("test_bool{$i}"),
                    'type' => Database::VAR_BOOLEAN,
                    'size' => 0,
                    'required' => false,
                    'default' => null,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]);
            }

            $collection = $this->getDatabase()->createCollection("widthLimit{$key}", $attributes);

            $this->expectException(LimitException::class);
            $this->assertEquals(false, $this->getDatabase()->createAttribute("widthLimit{$key}", "breaking", Database::VAR_STRING, 100, true));
        }

        // Default assertion for other adapters
        $this->assertEquals(1, 1);
    }

    /**
     * @dataProvider rowWidthExceedsMaximum
     * @depends      testExceptionWidthLimit
     */
    public function testCheckAttributeWidthLimit(int $key, int $stringSize, int $stringCount, int $intCount, int $floatCount, int $boolCount): void
    {
        if ($this->getDatabase()->getAdapter()::getDocumentSizeLimit() > 0) {
            $collection = $this->getDatabase()->getCollection("widthLimit{$key}");

            // create same attribute in testExceptionWidthLimit
            $attribute = new Document([
                '$id' => ID::custom('breaking'),
                'type' => Database::VAR_STRING,
                'size' => 100,
                'required' => true,
                'default' => null,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]);

            $this->expectException(LimitException::class);
            $this->assertEquals(false, $this->getDatabase()->checkAttribute($collection, $attribute));
        }

        // Default assertion for other adapters
        $this->assertEquals(1, 1);
    }

    public function testExceptionIndexLimit(): void
    {
        $this->getDatabase()->createCollection('indexLimit');

        // add unique attributes for indexing
        for ($i = 0; $i < 64; $i++) {
            $this->assertEquals(true, $this->getDatabase()->createAttribute('indexLimit', "test{$i}", Database::VAR_STRING, 16, true));
        }

        // Testing for indexLimit
        // Add up to the limit, then check if the next index throws IndexLimitException
        for ($i = 0; $i < ($this->getDatabase()->getLimitForIndexes()); $i++) {
            $this->assertEquals(true, $this->getDatabase()->createIndex('indexLimit', "index{$i}", Database::INDEX_KEY, ["test{$i}"], [16]));
        }
        $this->expectException(LimitException::class);
        $this->assertEquals(false, $this->getDatabase()->createIndex('indexLimit', "index64", Database::INDEX_KEY, ["test64"], [16]));

        $this->getDatabase()->deleteCollection('indexLimit');
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionDuplicate(Document $document): void
    {
        $document->setAttribute('$id', 'duplicated');
        $this->getDatabase()->createDocument($document->getCollection(), $document);

        $this->expectException(DuplicateException::class);
        $this->getDatabase()->createDocument($document->getCollection(), $document);
    }

    /**
     * @depends testGetDocument
     */
    public function testExceptionCaseInsensitiveDuplicate(Document $document): Document
    {
        $document->setAttribute('$id', 'caseSensitive');
        $document->setAttribute('$internalId', '200');
        $this->getDatabase()->createDocument($document->getCollection(), $document);

        $document->setAttribute('$id', 'CaseSensitive');

        $this->expectException(DuplicateException::class);
        $this->getDatabase()->createDocument($document->getCollection(), $document);

        return $document;
    }

    /**
     * @depends testFind
     */
    public function testUniqueIndexDuplicate(): void
    {
        $this->expectException(DuplicateException::class);

        $this->assertEquals(true, $this->getDatabase()->createIndex('movies', 'uniqueIndex', Database::INDEX_UNIQUE, ['name'], [128], [Database::ORDER_ASC]));

        $this->getDatabase()->createDocument('movies', new Document([
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
        self::$authorization->addRole(Role::users()->toString());
        // create document then update to conflict with index
        $document = $this->getDatabase()->createDocument('movies', new Document([
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

        $this->getDatabase()->updateDocument('movies', $document->getId(), $document->setAttribute('name', 'Frozen'));
    }

    public function testGetAttributeLimit(): void
    {
        $this->assertIsInt($this->getDatabase()->getLimitForAttributes());
    }

    public function testGetIndexLimit(): void
    {
        $this->assertEquals(58, $this->getDatabase()->getLimitForIndexes());
    }

    public function testGetId(): void
    {
        $this->assertEquals(20, strlen(ID::unique()));
        $this->assertEquals(13, strlen(ID::unique(0)));
        $this->assertEquals(13, strlen(ID::unique(-1)));
        $this->assertEquals(23, strlen(ID::unique(10)));

        // ensure two sequential calls to getId do not give the same result
        $this->assertNotEquals(ID::unique(10), ID::unique(10));
    }

    public function testRenameIndex(): void
    {
        $database = $this->getDatabase();

        $numbers = $database->createCollection('numbers');
        $database->createAttribute('numbers', 'verbose', Database::VAR_STRING, 128, true);
        $database->createAttribute('numbers', 'symbol', Database::VAR_INTEGER, 0, true);

        $database->createIndex('numbers', 'index1', Database::INDEX_KEY, ['verbose'], [128], [Database::ORDER_ASC]);
        $database->createIndex('numbers', 'index2', Database::INDEX_KEY, ['symbol'], [0], [Database::ORDER_ASC]);

        $index = $database->renameIndex('numbers', 'index1', 'index3');

        $this->assertTrue($index);

        $numbers = $database->getCollection('numbers');

        $this->assertEquals('index2', $numbers->getAttribute('indexes')[1]['$id']);
        $this->assertEquals('index3', $numbers->getAttribute('indexes')[0]['$id']);
        $this->assertCount(2, $numbers->getAttribute('indexes'));
    }

    /**
     * @depends testRenameIndex
     * @expectedException Exception
     */
    public function testRenameIndexMissing(): void
    {
        $database = $this->getDatabase();
        $this->expectExceptionMessage('Index not found');
        $index = $database->renameIndex('numbers', 'index1', 'index4');
    }

    /**
     * @depends testRenameIndex
     * @expectedException Exception
     */
    public function testRenameIndexExisting(): void
    {
        $database = $this->getDatabase();
        $this->expectExceptionMessage('Index name already used');
        $index = $database->renameIndex('numbers', 'index3', 'index2');
    }

    public function testRenameAttribute(): void
    {
        $database = $this->getDatabase();

        $colors = $database->createCollection('colors');
        $database->createAttribute('colors', 'name', Database::VAR_STRING, 128, true);
        $database->createAttribute('colors', 'hex', Database::VAR_STRING, 128, true);

        $database->createIndex('colors', 'index1', Database::INDEX_KEY, ['name'], [128], [Database::ORDER_ASC]);

        $database->createDocument('colors', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'black',
            'hex' => '#000000'
        ]));

        $attribute = $database->renameAttribute('colors', 'name', 'verbose');

        $this->assertTrue($attribute);

        $colors = $database->getCollection('colors');
        $this->assertEquals('hex', $colors->getAttribute('attributes')[1]['$id']);
        $this->assertEquals('verbose', $colors->getAttribute('attributes')[0]['$id']);
        $this->assertCount(2, $colors->getAttribute('attributes'));

        // Attribute in index is renamed automatically on adapter-level. What we need to check is if metadata is properly updated
        $this->assertEquals('verbose', $colors->getAttribute('indexes')[0]->getAttribute("attributes")[0]);
        $this->assertCount(1, $colors->getAttribute('indexes'));

        // Document should be there if adapter migrated properly
        $document = $database->findOne('colors');
        $this->assertFalse($document->isEmpty());
        $this->assertEquals('black', $document->getAttribute('verbose'));
        $this->assertEquals('#000000', $document->getAttribute('hex'));
        $this->assertEquals(null, $document->getAttribute('name'));
    }

    /**
     * @depends testRenameAttribute
     * @expectedException Exception
     */
    public function textRenameAttributeMissing(): void
    {
        $database = $this->getDatabase();
        $this->expectExceptionMessage('Attribute not found');
        $database->renameAttribute('colors', 'name2', 'name3');
    }

    /**
     * @depends testRenameAttribute
     * @expectedException Exception
     */
    public function testRenameAttributeExisting(): void
    {
        $database = $this->getDatabase();
        $this->expectExceptionMessage('Attribute name already used');
        $database->renameAttribute('colors', 'verbose', 'hex');
    }

    public function testUpdateAttributeDefault(): void
    {
        $database = $this->getDatabase();

        $flowers = $database->createCollection('flowers');
        $database->createAttribute('flowers', 'name', Database::VAR_STRING, 128, true);
        $database->createAttribute('flowers', 'inStock', Database::VAR_INTEGER, 0, false);
        $database->createAttribute('flowers', 'date', Database::VAR_STRING, 128, false);

        $database->createDocument('flowers', new Document([
            '$id' => 'flowerWithDate',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Violet',
            'inStock' => 51,
            'date' => '2000-06-12 14:12:55.000'
        ]));

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily'
        ]));

        $this->assertNull($doc->getAttribute('inStock'));

        $database->updateAttributeDefault('flowers', 'inStock', 100);

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Iris'
        ]));

        $this->assertIsNumeric($doc->getAttribute('inStock'));
        $this->assertEquals(100, $doc->getAttribute('inStock'));

        $database->updateAttributeDefault('flowers', 'inStock', null);
    }

    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeRequired(): void
    {
        $database = $this->getDatabase();

        $database->updateAttributeRequired('flowers', 'inStock', true);

        $this->expectExceptionMessage('Invalid document structure: Missing required attribute "inStock"');

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily With Missing Stocks'
        ]));
    }

    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeFilter(): void
    {
        $database = $this->getDatabase();

        $database->createAttribute('flowers', 'cartModel', Database::VAR_STRING, 2000, false);

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily With CartData',
            'inStock' => 50,
            'cartModel' => '{"color":"string","size":"number"}'
        ]));

        $this->assertIsString($doc->getAttribute('cartModel'));
        $this->assertEquals('{"color":"string","size":"number"}', $doc->getAttribute('cartModel'));

        $database->updateAttributeFilters('flowers', 'cartModel', ['json']);

        $doc = $database->getDocument('flowers', $doc->getId());
        $this->assertIsArray($doc->getAttribute('cartModel'));
        $this->assertCount(2, $doc->getAttribute('cartModel'));
        $this->assertEquals('string', $doc->getAttribute('cartModel')['color']);
        $this->assertEquals('number', $doc->getAttribute('cartModel')['size']);
    }

    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeFormat(): void
    {
        $database = $this->getDatabase();

        $database->createAttribute('flowers', 'price', Database::VAR_INTEGER, 0, false);

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            '$id' => ID::custom('LiliPriced'),
            'name' => 'Lily Priced',
            'inStock' => 50,
            'cartModel' => '{}',
            'price' => 500
        ]));

        $this->assertIsNumeric($doc->getAttribute('price'));
        $this->assertEquals(500, $doc->getAttribute('price'));

        Structure::addFormat('priceRange', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];

            return new Range($min, $max);
        }, Database::VAR_INTEGER);

        $database->updateAttributeFormat('flowers', 'price', 'priceRange');
        $database->updateAttributeFormatOptions('flowers', 'price', ['min' => 1, 'max' => 10000]);

        $this->expectExceptionMessage('Invalid document structure: Attribute "price" has invalid format. Value must be a valid range between 1 and 10,000');

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily Overpriced',
            'inStock' => 50,
            'cartModel' => '{}',
            'price' => 15000
        ]));
    }

    /**
     * @depends testUpdateAttributeDefault
     * @depends testUpdateAttributeFormat
     */
    public function testUpdateAttributeStructure(): void
    {
        // TODO: When this becomes relevant, add many more tests (from all types to all types, chaging size up&down, switchign between array/non-array...

        Structure::addFormat('priceRangeNew', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];
            return new Range($min, $max);
        }, Database::VAR_INTEGER);

        $database = $this->getDatabase();

        // price attribute
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRange', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', default: 100);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRange', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', format: 'priceRangeNew');
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRangeNew', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', format: '');
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', formatOptions: ['min' => 1, 'max' => 999]);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 999], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', formatOptions: []);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', signed: false);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', required: true);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(true, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', type: Database::VAR_STRING, size: Database::LENGTH_KEY, format: '');
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('string', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(255, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(true, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $collection->getAttribute('attributes')[4]['formatOptions']);

        // Date attribute
        $attribute = $collection->getAttribute('attributes')[2];
        $this->assertEquals('date', $attribute['key']);
        $this->assertEquals('string', $attribute['type']);
        $this->assertEquals(null, $attribute['default']);

        $database->updateAttribute('flowers', 'date', type: Database::VAR_DATETIME, size: 0, filters: ['datetime']);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[2];
        $this->assertEquals('datetime', $attribute['type']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $doc = $database->getDocument('flowers', 'LiliPriced');
        $this->assertIsString($doc->getAttribute('price'));
        $this->assertEquals('500', $doc->getAttribute('price'));

        $doc = $database->getDocument('flowers', 'flowerWithDate');
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $doc->getAttribute('date'));
    }

    public function testUpdateAttributeRename(): void
    {
        static::getDatabase()->createCollection('rename_test');

        $this->assertEquals(true, static::getDatabase()->createAttribute('rename_test', 'rename_me', Database::VAR_STRING, 128, true));

        $doc = static::getDatabase()->createDocument('rename_test', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'rename_me' => 'string'
        ]));

        $this->assertEquals('string', $doc->getAttribute('rename_me'));

        // Create an index to check later
        static::getDatabase()->createIndex('rename_test', 'renameIndexes', Database::INDEX_KEY, ['rename_me'], [], [Database::ORDER_DESC, Database::ORDER_DESC]);

        static::getDatabase()->updateAttribute(
            collection: 'rename_test',
            id: 'rename_me',
            newKey: 'renamed',
        );

        $doc = static::getDatabase()->getDocument('rename_test', $doc->getId());

        // Check the attribute was correctly renamed
        $this->assertEquals('string', $doc->getAttribute('renamed'));
        $this->assertArrayNotHasKey('rename_me', $doc);

        // Check we can update the document with the new key
        $doc->setAttribute('renamed', 'string2');
        static::getDatabase()->updateDocument('rename_test', $doc->getId(), $doc);

        $doc = static::getDatabase()->getDocument('rename_test', $doc->getId());
        $this->assertEquals('string2', $doc->getAttribute('renamed'));

        // Check collection
        $collection = static::getDatabase()->getCollection('rename_test');
        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['key']);
        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['$id']);

        // Check empty key doesn't cause issues
        static::getDatabase()->updateAttribute(
            collection: 'rename_test',
            id: 'renamed',
            type: Database::VAR_STRING,
        );

        $doc = static::getDatabase()->getDocument('rename_test', $doc->getId());

        $this->assertEquals('string2', $doc->getAttribute('renamed'));
        $this->assertArrayNotHasKey('rename_me', $doc->getAttributes());

        // Check the metadata was correctly updated
        $attribute = $collection->getAttribute('attributes')[0];
        $this->assertEquals('renamed', $attribute['key']);
        $this->assertEquals('renamed', $attribute['$id']);

        // Check the indexes were updated
        $index = $collection->getAttribute('indexes')[0];
        $this->assertEquals('renamed', $index->getAttribute('attributes')[0]);
        $this->assertEquals(1, count($collection->getAttribute('indexes')));

        // Try and create new document with new key
        $doc = static::getDatabase()->createDocument('rename_test', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'renamed' => 'string'
        ]));

        $this->assertEquals('string', $doc->getAttribute('renamed'));

        // Make sure we can't create a new attribute with the old key
        try {
            $doc = static::getDatabase()->createDocument('rename_test', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'rename_me' => 'string'
            ]));
            $this->fail('Succeeded creating a document with old key after renaming the attribute');
        } catch (\Exception $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        // Check new key filtering
        static::getDatabase()->updateAttribute(
            collection: 'rename_test',
            id: 'renamed',
            newKey: 'renamed-test',
        );

        $doc = static::getDatabase()->getDocument('rename_test', $doc->getId());

        $this->assertEquals('string', $doc->getAttribute('renamed-test'));
        $this->assertArrayNotHasKey('renamed', $doc->getAttributes());
    }

    public function testUpdateAttributeRenameRelationshipTwoWay(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('rn_rs_test_a');
        static::getDatabase()->createCollection('rn_rs_test_b');

        static::getDatabase()->createAttribute('rn_rs_test_b', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            'rn_rs_test_a',
            'rn_rs_test_b',
            Database::RELATION_ONE_TO_ONE,
            true
        );

        $docA = static::getDatabase()->createDocument('rn_rs_test_a', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'rn_rs_test_b' => [
                '$id' => 'b1',
                'name' => 'B1'
            ]
        ]));

        $docB = static::getDatabase()->getDocument('rn_rs_test_b', 'b1');
        $this->assertArrayHasKey('rn_rs_test_a', $docB->getAttributes());
        $this->assertEquals('B1', $docB->getAttribute('name'));

        // Rename attribute
        static::getDatabase()->updateRelationship(
            collection: 'rn_rs_test_a',
            id: 'rn_rs_test_b',
            newKey: 'rn_rs_test_b_renamed'
        );

        // Rename again
        static::getDatabase()->updateRelationship(
            collection: 'rn_rs_test_a',
            id: 'rn_rs_test_b_renamed',
            newKey: 'rn_rs_test_b_renamed_2'
        );

        // Check our data is OK
        $docA = static::getDatabase()->getDocument('rn_rs_test_a', $docA->getId());
        $this->assertArrayHasKey('rn_rs_test_b_renamed_2', $docA->getAttributes());
        $this->assertEquals($docB->getId(), $docA->getAttribute('rn_rs_test_b_renamed_2')['$id']);
    }

    public function createRandomString(int $length = 10): string
    {
        return \substr(\bin2hex(\random_bytes(\max(1, \intval(($length + 1) / 2)))), 0, $length);
    }

    public function updateStringAttributeSize(int $size, Document $document): Document
    {
        static::getDatabase()->updateAttribute('resize_test', 'resize_me', Database::VAR_STRING, $size, true);

        $document = $document->setAttribute('resize_me', $this->createRandomString($size));

        static::getDatabase()->updateDocument('resize_test', $document->getId(), $document);
        $checkDoc = static::getDatabase()->getDocument('resize_test', $document->getId());

        $this->assertEquals($document->getAttribute('resize_me'), $checkDoc->getAttribute('resize_me'));
        $this->assertEquals($size, strlen($checkDoc->getAttribute('resize_me')));

        return $checkDoc;
    }

    public function testUpdateAttributeSize(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForAttributeResizing()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('resize_test');

        $this->assertEquals(true, static::getDatabase()->createAttribute('resize_test', 'resize_me', Database::VAR_STRING, 128, true));
        $document = static::getDatabase()->createDocument('resize_test', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'resize_me' => $this->createRandomString(128)
        ]));

        // Go up in size

        // 0-16381 to 16382-65535
        $document = $this->updateStringAttributeSize(16382, $document);

        // 16382-65535 to 65536-16777215
        $document = $this->updateStringAttributeSize(65536, $document);

        // 65536-16777216 to PHP_INT_MAX or adapter limit
        $maxStringSize = 16777217;
        $document = $this->updateStringAttributeSize($maxStringSize, $document);

        // Test going down in size with data that is too big (Expect Failure)
        try {
            static::getDatabase()->updateAttribute('resize_test', 'resize_me', Database::VAR_STRING, 128, true);
            $this->fail('Succeeded updating attribute size to smaller size with data that is too big');
        } catch (TruncateException $e) {
        }

        // Test going down in size when data isn't too big.
        static::getDatabase()->updateDocument('resize_test', $document->getId(), $document->setAttribute('resize_me', $this->createRandomString(128)));
        static::getDatabase()->updateAttribute('resize_test', 'resize_me', Database::VAR_STRING, 128, true);

        // VARCHAR -> VARCHAR Truncation Test
        static::getDatabase()->updateAttribute('resize_test', 'resize_me', Database::VAR_STRING, 1000, true);
        static::getDatabase()->updateDocument('resize_test', $document->getId(), $document->setAttribute('resize_me', $this->createRandomString(1000)));

        try {
            static::getDatabase()->updateAttribute('resize_test', 'resize_me', Database::VAR_STRING, 128, true);
            $this->fail('Succeeded updating attribute size to smaller size with data that is too big');
        } catch (TruncateException $e) {
        }
    }

    /**
     * @depends testCreatedAtUpdatedAt
     */
    public function testCreatedAtUpdatedAtAssert(): void
    {
        $document = $this->getDatabase()->getDocument('created_at', 'uid123');
        $this->assertEquals(true, !$document->isEmpty());
        sleep(1);
        $document->setAttribute('title', 'new title');
        $this->getDatabase()->updateDocument('created_at', 'uid123', $document);
        $document = $this->getDatabase()->getDocument('created_at', 'uid123');

        $this->assertGreaterThan($document->getCreatedAt(), $document->getUpdatedAt());
        $this->expectException(DuplicateException::class);

        $this->getDatabase()->createCollection('created_at');
    }

    public function testCreateDatetime(): void
    {
        $this->getDatabase()->createCollection('datetime');

        $this->assertEquals(true, $this->getDatabase()->createAttribute('datetime', 'date', Database::VAR_DATETIME, 0, true, null, true, false, null, [], ['datetime']));
        $this->assertEquals(true, $this->getDatabase()->createAttribute('datetime', 'date2', Database::VAR_DATETIME, 0, false, null, true, false, null, [], ['datetime']));

        $doc = $this->getDatabase()->createDocument('datetime', new Document([
            '$id' => ID::custom('id1234'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'date' => DateTime::now(),
        ]));

        $this->assertEquals(29, strlen($doc->getCreatedAt()));
        $this->assertEquals(29, strlen($doc->getUpdatedAt()));
        $this->assertEquals('+00:00', substr($doc->getCreatedAt(), -6));
        $this->assertEquals('+00:00', substr($doc->getUpdatedAt(), -6));
        $this->assertGreaterThan('2020-08-16T19:30:08.363+00:00', $doc->getCreatedAt());
        $this->assertGreaterThan('2020-08-16T19:30:08.363+00:00', $doc->getUpdatedAt());

        $document = $this->getDatabase()->getDocument('datetime', 'id1234');
        $dateValidator = new DatetimeValidator();
        $this->assertEquals(null, $document->getAttribute('date2'));
        $this->assertEquals(true, $dateValidator->isValid($document->getAttribute('date')));
        $this->assertEquals(false, $dateValidator->isValid($document->getAttribute('date2')));

        $documents = $this->getDatabase()->find('datetime', [
            Query::greaterThan('date', '1975-12-06 10:00:00+01:00'),
            Query::lessThan('date', '2030-12-06 10:00:00-01:00'),
        ]);
        $this->assertEquals(1, count($documents));

        $documents = $this->getDatabase()->find('datetime', [
            Query::greaterThan('$createdAt', '1975-12-06 11:00:00.000'),
        ]);
        $this->assertCount(1, $documents);

        try {
            $this->getDatabase()->createDocument('datetime', new Document([
                'date' => "1975-12-06 00:00:61" // 61 seconds is invalid
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        try {
            static::getDatabase()->createDocument('datetime', new Document([
                'date' => '+055769-02-14T17:56:18.000Z'
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        $invalidDates = [
            '+055769-02-14T17:56:18.000Z1',
            '1975-12-06 00:00:61',
            '16/01/2024 12:00:00AM'
        ];

        foreach ($invalidDates as $date) {
            try {
                $this->getDatabase()->find('datetime', [
                    Query::equal('date', [$date])
                ]);
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertTrue($e instanceof QueryException);
                $this->assertEquals('Invalid query: Query value is invalid for attribute "date"', $e->getMessage());
            }
        }
    }

    public function testCreateDateTimeAttributeFailure(): void
    {
        $this->getDatabase()->createCollection('datetime_fail');

        /** Test for FAILURE */
        $this->expectException(Exception::class);
        $this->getDatabase()->createAttribute('datetime_fail', 'date_fail', Database::VAR_DATETIME, 0, false);
    }

    public function testKeywords(): void
    {
        $database = $this->getDatabase();
        $keywords = $database->getKeywords();

        // Collection name tests
        $attributes = [
            new Document([
                '$id' => ID::custom('attribute1'),
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => ['ASC'],
            ]),
        ];

        foreach ($keywords as $keyword) {
            $collection = $database->createCollection($keyword, $attributes, $indexes);
            $this->assertEquals($keyword, $collection->getId());

            $document = $database->createDocument($keyword, new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                '$id' => ID::custom('helloWorld'),
                'attribute1' => 'Hello World',
            ]));
            $this->assertEquals('helloWorld', $document->getId());

            $document = $database->getDocument($keyword, 'helloWorld');
            $this->assertEquals('helloWorld', $document->getId());

            $documents = $database->find($keyword);
            $this->assertCount(1, $documents);
            $this->assertEquals('helloWorld', $documents[0]->getId());

            $collection = $database->deleteCollection($keyword);
            $this->assertTrue($collection);
        }

        // TODO: updateCollection name tests

        // Attribute name tests
        foreach ($keywords as $keyword) {
            $collectionName = 'rk' . $keyword; // rk is short-hand for reserved-keyword. We do this sicne there are some limits (64 chars max)

            $collection = $database->createCollection($collectionName);
            $this->assertEquals($collectionName, $collection->getId());

            $attribute = $this->getDatabase()->createAttribute($collectionName, $keyword, Database::VAR_STRING, 128, true);
            $this->assertEquals(true, $attribute);

            $document = new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                '$id' => 'reservedKeyDocument'
            ]);
            $document->setAttribute($keyword, 'Reserved:' . $keyword);

            $document = $database->createDocument($collectionName, $document);
            $this->assertEquals('reservedKeyDocument', $document->getId());
            $this->assertEquals('Reserved:' . $keyword, $document->getAttribute($keyword));

            $document = $database->getDocument($collectionName, 'reservedKeyDocument');
            $this->assertEquals('reservedKeyDocument', $document->getId());
            $this->assertEquals('Reserved:' . $keyword, $document->getAttribute($keyword));

            $documents = $database->find($collectionName);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());
            $this->assertEquals('Reserved:' . $keyword, $documents[0]->getAttribute($keyword));

            $documents = $database->find($collectionName, [Query::equal($keyword, ["Reserved:{$keyword}"])]);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());

            $documents = $database->find($collectionName, [
                Query::orderDesc($keyword)
            ]);
            $this->assertCount(1, $documents);
            $this->assertEquals('reservedKeyDocument', $documents[0]->getId());


            $collection = $database->deleteCollection($collectionName);
            $this->assertTrue($collection);

            // TODO: updateAttribute name tests
        }

        // TODO: Index name tests
    }

    public function testWritePermissions(): void
    {
        self::$authorization->addRole(Role::any()->toString());
        $database = $this->getDatabase();

        $database->createCollection('animals', permissions: [
            Permission::create(Role::any()),
        ], documentSecurity: true);

        $database->createAttribute('animals', 'type', Database::VAR_STRING, 128, true);

        $dog = $database->createDocument('animals', new Document([
            '$id' => 'dog',
            '$permissions' => [
                Permission::delete(Role::any()),
            ],
            'type' => 'Dog'
        ]));

        $cat = $database->createDocument('animals', new Document([
            '$id' => 'cat',
            '$permissions' => [
                Permission::update(Role::any()),
            ],
            'type' => 'Cat'
        ]));

        // No read permissions:

        $docs = $database->find('animals');
        $this->assertCount(0, $docs);

        $doc = $database->getDocument('animals', 'dog');
        $this->assertTrue($doc->isEmpty());

        $doc = $database->getDocument('animals', 'cat');
        $this->assertTrue($doc->isEmpty());

        // Cannot delete with update permission:
        $didFail = false;

        try {
            $database->deleteDocument('animals', 'cat');
        } catch (AuthorizationException) {
            $didFail = true;
        }

        $this->assertTrue($didFail);

        // Cannot update with delete permission:
        $didFail = false;

        try {
            $newDog = $dog->setAttribute('type', 'newDog');
            $database->updateDocument('animals', 'dog', $newDog);
        } catch (AuthorizationException) {
            $didFail = true;
        }

        $this->assertTrue($didFail);

        // Can delete:
        $database->deleteDocument('animals', 'dog');

        // Can update:
        $newCat = $cat->setAttribute('type', 'newCat');
        $database->updateDocument('animals', 'cat', $newCat);

        $docs = self::$authorization->skip(fn () => $database->find('animals'));
        $this->assertCount(1, $docs);
        $this->assertEquals('cat', $docs[0]['$id']);
        $this->assertEquals('newCat', $docs[0]['type']);
    }

    public function testNoInvalidKeysWithRelationships(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $this->getDatabase()->createCollection('species');
        $this->getDatabase()->createCollection('creatures');
        $this->getDatabase()->createCollection('characterstics');

        $this->getDatabase()->createAttribute('species', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('creatures', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('characterstics', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'species',
            relatedCollection: 'creatures',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'creature',
            twoWayKey:'species'
        );
        $this->getDatabase()->createRelationship(
            collection: 'creatures',
            relatedCollection: 'characterstics',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'characterstic',
            twoWayKey:'creature'
        );

        $species = $this->getDatabase()->createDocument('species', new Document([
            '$id' => ID::custom('1'),
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Canine',
            'creature' => [
                '$id' => ID::custom('1'),
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Dog',
                'characterstic' => [
                    '$id' => ID::custom('1'),
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'active',
                ]
            ]
        ]));
        $this->getDatabase()->updateDocument('species', $species->getId(), new Document([
            '$id' => ID::custom('1'),
            '$collection' => 'species',
            'creature' => [
                '$id' => ID::custom('1'),
                '$collection' => 'creatures',
                'characterstic' => [
                    '$id' => ID::custom('1'),
                    'name' => 'active',
                    '$collection' => 'characterstics',
                ]
            ]
        ]));
        $updatedSpecies = $this->getDatabase()->getDocument('species', $species->getId());
        $this->assertEquals($species, $updatedSpecies);
    }

    // Relationships
    public function testOneToOneOneWayRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('person');
        $this->getDatabase()->createCollection('library');

        $this->getDatabase()->createAttribute('person', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('library', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('library', 'area', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'person',
            relatedCollection: 'library',
            type: Database::RELATION_ONE_TO_ONE
        );

        // Check metadata for collection
        $collection = $this->getDatabase()->getCollection('person');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'library') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('library', $attribute['$id']);
                $this->assertEquals('library', $attribute['key']);
                $this->assertEquals('library', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('person', $attribute['options']['twoWayKey']);
            }
        }

        try {
            $this->getDatabase()->deleteAttribute('person', 'library');
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete relationship as an attribute', $e->getMessage());
        }

        // Create document with relationship with nested data
        $person1 = $this->getDatabase()->createDocument('person', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'library' => [
                '$id' => 'library1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 1',
                'area' => 'Area 1',
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $this->getDatabase()->updateDocument(
            'person',
            'person1',
            $person1->setAttribute('library', 'no-library')
        );

        $person1Document = $this->getDatabase()->getDocument('person', 'person1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $person1Document->getAttribute('library'));

        $this->getDatabase()->updateDocument(
            'person',
            'person1',
            $person1->setAttribute('library', 'library1')
        );

        // Update through create
        $library10 = $this->getDatabase()->createDocument('library', new Document([
            '$id' => 'library10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Library 10',
            'area' => 'Area 10',
        ]));
        $person10 = $this->getDatabase()->createDocument('person', new Document([
            '$id' => 'person10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 10',
            'library' => [
                '$id' => $library10->getId(),
                'name' => 'Library 10 Updated',
                'area' => 'Area 10 Updated',
            ],
        ]));
        $this->assertEquals('Library 10 Updated', $person10->getAttribute('library')->getAttribute('name'));
        $library10 = $this->getDatabase()->getDocument('library', $library10->getId());
        $this->assertEquals('Library 10 Updated', $library10->getAttribute('name'));

        // Create document with relationship with related ID
        $this->getDatabase()->createDocument('library', new Document([
            '$id' => 'library2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Library 2',
            'area' => 'Area 2',
        ]));
        $this->getDatabase()->createDocument('person', new Document([
            '$id' => 'person2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 2',
            'library' => 'library2',
        ]));

        // Get documents with relationship
        $person1 = $this->getDatabase()->getDocument('person', 'person1');
        $library = $person1->getAttribute('library');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('person', $library);

        $person = $this->getDatabase()->getDocument('person', 'person2');
        $library = $person->getAttribute('library');
        $this->assertEquals('library2', $library['$id']);
        $this->assertArrayNotHasKey('person', $library);

        // Get related documents
        $library = $this->getDatabase()->getDocument('library', 'library1');
        $this->assertArrayNotHasKey('person', $library);

        $library = $this->getDatabase()->getDocument('library', 'library2');
        $this->assertArrayNotHasKey('person', $library);

        $people = $this->getDatabase()->find('person', [
            Query::select(['name'])
        ]);

        $this->assertArrayNotHasKey('library', $people[0]);

        $people = $this->getDatabase()->find('person');
        $this->assertEquals(3, \count($people));

        // Select related document attributes
        $person = $this->getDatabase()->findOne('person', [
            Query::select(['*', 'library.name'])
        ]);

        if ($person->isEmpty()) {
            throw new Exception('Person not found');
        }

        $this->assertEquals('Library 1', $person->getAttribute('library')->getAttribute('name'));
        $this->assertArrayNotHasKey('area', $person->getAttribute('library'));

        $person = $this->getDatabase()->getDocument('person', 'person1', [
            Query::select(['*', 'library.name', '$id'])
        ]);

        $this->assertEquals('Library 1', $person->getAttribute('library')->getAttribute('name'));
        $this->assertArrayNotHasKey('area', $person->getAttribute('library'));



        $document = $this->getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['name']),
        ]);
        $this->assertArrayNotHasKey('library', $document);
        $this->assertEquals('Person 1', $document['name']);

        $document = $this->getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['*']),
        ]);
        $this->assertEquals('library1', $document['library']);

        $document = $this->getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['library.*']),
        ]);
        $this->assertEquals('Library 1', $document['library']['name']);
        $this->assertArrayNotHasKey('name', $document);

        // Update root document attribute without altering relationship
        $person1 = $this->getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('name', 'Person 1 Updated')
        );

        $this->assertEquals('Person 1 Updated', $person1->getAttribute('name'));
        $person1 = $this->getDatabase()->getDocument('person', 'person1');
        $this->assertEquals('Person 1 Updated', $person1->getAttribute('name'));

        // Update nested document attribute
        $person1 = $this->getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute(
                'library',
                $person1
                ->getAttribute('library')
                ->setAttribute('name', 'Library 1 Updated')
            )
        );

        $this->assertEquals('Library 1 Updated', $person1->getAttribute('library')->getAttribute('name'));
        $person1 = $this->getDatabase()->getDocument('person', 'person1');
        $this->assertEquals('Library 1 Updated', $person1->getAttribute('library')->getAttribute('name'));

        // Create new document with no relationship
        $person3 = $this->getDatabase()->createDocument('person', new Document([
            '$id' => 'person3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 3',
        ]));

        // Update to relate to created document
        $person3 = $this->getDatabase()->updateDocument(
            'person',
            $person3->getId(),
            $person3->setAttribute('library', new Document([
                '$id' => 'library3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => 'Library 3',
                'area' => 'Area 3',
            ]))
        );

        $this->assertEquals('library3', $person3->getAttribute('library')['$id']);
        $person3 = $this->getDatabase()->getDocument('person', 'person3');
        $this->assertEquals('Library 3', $person3['library']['name']);

        $libraryDocument = $this->getDatabase()->getDocument('library', 'library3');
        $libraryDocument->setAttribute('name', 'Library 3 updated');
        $this->getDatabase()->updateDocument('library', 'library3', $libraryDocument);
        $libraryDocument = $this->getDatabase()->getDocument('library', 'library3');
        $this->assertEquals('Library 3 updated', $libraryDocument['name']);

        $person3 = $this->getDatabase()->getDocument('person', 'person3');
        // Todo: This is failing
        $this->assertEquals($libraryDocument['name'], $person3['library']['name']);
        $this->assertEquals('library3', $person3->getAttribute('library')['$id']);

        // One to one can't relate to multiple documents, unique index throws duplicate
        try {
            $this->getDatabase()->updateDocument(
                'person',
                $person1->getId(),
                $person1->setAttribute('library', 'library2')
            );
            $this->fail('Failed to throw duplicate exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Create new document
        $library4 = $this->getDatabase()->createDocument('library', new Document([
            '$id' => 'library4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Library 4',
            'area' => 'Area 4',
        ]));

        // Relate existing document to new document
        $this->getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('library', 'library4')
        );

        // Relate existing document to new document as nested data
        $this->getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('library', $library4)
        );

        // Rename relationship key
        $this->getDatabase()->updateRelationship(
            collection: 'person',
            id: 'library',
            newKey: 'newLibrary'
        );

        // Get document with again
        $person = $this->getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary');
        $this->assertEquals('library4', $library['$id']);

        // Create person with no relationship
        $this->getDatabase()->createDocument('person', new Document([
            '$id' => 'person4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 4',
        ]));

        // Can delete parent document with no relation with on delete set to restrict
        $deleted = $this->getDatabase()->deleteDocument('person', 'person4');
        $this->assertEquals(true, $deleted);

        $person4 = $this->getDatabase()->getDocument('person', 'person4');
        $this->assertEquals(true, $person4->isEmpty());

        // Cannot delete document while still related to another with on delete set to restrict
        try {
            $this->getDatabase()->deleteDocument('person', 'person1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Can delete child document while still related to another with on delete set to restrict
        $person5 = $this->getDatabase()->createDocument('person', new Document([
            '$id' => 'person5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 5',
            'newLibrary' => [
                '$id' => 'library5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 5',
                'area' => 'Area 5',
            ],
        ]));
        $deleted = $this->getDatabase()->deleteDocument('library', 'library5');
        $this->assertEquals(true, $deleted);
        $person5 = $this->getDatabase()->getDocument('person', 'person5');
        $this->assertEquals(null, $person5->getAttribute('newLibrary'));

        // Change on delete to set null
        $this->getDatabase()->updateRelationship(
            collection: 'person',
            id: 'newLibrary',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, no effect on children for one-way
        $this->getDatabase()->deleteDocument('person', 'person1');

        // Delete child, set parent relating attribute to null for one-way
        $this->getDatabase()->deleteDocument('library', 'library2');

        // Check relation was set to null
        $person2 = $this->getDatabase()->getDocument('person', 'person2');
        $this->assertEquals(null, $person2->getAttribute('newLibrary', ''));

        // Relate to another document
        $this->getDatabase()->updateDocument(
            'person',
            $person2->getId(),
            $person2->setAttribute('newLibrary', 'library4')
        );

        // Change on delete to cascade
        $this->getDatabase()->updateRelationship(
            collection: 'person',
            id: 'newLibrary',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $this->getDatabase()->deleteDocument('person', 'person2');

        // Check parent and child were deleted
        $person = $this->getDatabase()->getDocument('person', 'person2');
        $this->assertEquals(true, $person->isEmpty());

        $library = $this->getDatabase()->getDocument('library', 'library4');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        $this->getDatabase()->deleteRelationship(
            'person',
            'newLibrary'
        );

        // Check parent doesn't have relationship anymore
        $person = $this->getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary', '');
        $this->assertEquals(null, $library);
    }

    /**
     * @throws AuthorizationException
     * @throws LimitException
     * @throws DuplicateException
     * @throws StructureException
     * @throws Throwable
     */
    public function testOneToOneTwoWayRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('country');
        $this->getDatabase()->createCollection('city');

        $this->getDatabase()->createAttribute('country', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('city', 'code', Database::VAR_STRING, 3, true);
        $this->getDatabase()->createAttribute('city', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'country',
            relatedCollection: 'city',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $collection = $this->getDatabase()->getCollection('country');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'city') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('city', $attribute['$id']);
                $this->assertEquals('city', $attribute['key']);
                $this->assertEquals('city', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('country', $attribute['options']['twoWayKey']);
            }
        }

        $collection = $this->getDatabase()->getCollection('city');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'country') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('country', $attribute['$id']);
                $this->assertEquals('country', $attribute['key']);
                $this->assertEquals('country', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('city', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $doc = new Document([
            '$id' => 'country1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'England',
            'city' => [
                '$id' => 'city1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'London',
                'code' => 'LON',
            ],
        ]);

        $this->getDatabase()->createDocument('country', new Document($doc->getArrayCopy()));
        $country1 = $this->getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('London', $country1->getAttribute('city')->getAttribute('name'));

        // Update a document with non existing related document. It should not get added to the list.
        $this->getDatabase()->updateDocument('country', 'country1', (new Document($doc->getArrayCopy()))->setAttribute('city', 'no-city'));

        $country1Document = $this->getDatabase()->getDocument('country', 'country1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $country1Document->getAttribute('city'));
        $this->getDatabase()->updateDocument('country', 'country1', (new Document($doc->getArrayCopy()))->setAttribute('city', 'city1'));
        try {
            $this->getDatabase()->deleteDocument('country', 'country1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(RestrictedException::class, $e);
        }

        $this->assertTrue($this->getDatabase()->deleteDocument('city', 'city1'));

        $city1 = $this->getDatabase()->getDocument('city', 'city1');
        $this->assertTrue($city1->isEmpty());

        $country1 = $this->getDatabase()->getDocument('country', 'country1');
        $this->assertTrue($country1->getAttribute('city')->isEmpty());

        $this->assertTrue($this->getDatabase()->deleteDocument('country', 'country1'));

        $this->getDatabase()->createDocument('country', new Document($doc->getArrayCopy()));
        $country1 = $this->getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('London', $country1->getAttribute('city')->getAttribute('name'));

        // Create document with relationship with related ID
        $this->getDatabase()->createDocument('city', new Document([
            '$id' => 'city2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Paris',
            'code' => 'PAR',
        ]));
        $this->getDatabase()->createDocument('country', new Document([
            '$id' => 'country2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'France',
            'city' => 'city2',
        ]));

        // Create from child side
        $this->getDatabase()->createDocument('city', new Document([
            '$id' => 'city3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Christchurch',
            'code' => 'CHC',
            'country' => [
                '$id' => 'country3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'New Zealand',
            ],
        ]));
        $this->getDatabase()->createDocument('country', new Document([
            '$id' => 'country4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Australia',
        ]));
        $this->getDatabase()->createDocument('city', new Document([
            '$id' => 'city4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Sydney',
            'code' => 'SYD',
            'country' => 'country4',
        ]));

        // Get document with relationship
        $city = $this->getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('country');
        $this->assertEquals('country1', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = $this->getDatabase()->getDocument('city', 'city2');
        $country = $city->getAttribute('country');
        $this->assertEquals('country2', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = $this->getDatabase()->getDocument('city', 'city3');
        $country = $city->getAttribute('country');
        $this->assertEquals('country3', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = $this->getDatabase()->getDocument('city', 'city4');
        $country = $city->getAttribute('country');
        $this->assertEquals('country4', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        // Get inverse document with relationship
        $country = $this->getDatabase()->getDocument('country', 'country1');
        $city = $country->getAttribute('city');
        $this->assertEquals('city1', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = $this->getDatabase()->getDocument('country', 'country2');
        $city = $country->getAttribute('city');
        $this->assertEquals('city2', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = $this->getDatabase()->getDocument('country', 'country3');
        $city = $country->getAttribute('city');
        $this->assertEquals('city3', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = $this->getDatabase()->getDocument('country', 'country4');
        $city = $country->getAttribute('city');
        $this->assertEquals('city4', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $countries = $this->getDatabase()->find('country');

        $this->assertEquals(4, \count($countries));

        // Select related document attributes
        $country = $this->getDatabase()->findOne('country', [
            Query::select(['*', 'city.name'])
        ]);

        if ($country->isEmpty()) {
            throw new Exception('Country not found');
        }

        $this->assertEquals('London', $country->getAttribute('city')->getAttribute('name'));
        $this->assertArrayNotHasKey('code', $country->getAttribute('city'));

        $country = $this->getDatabase()->getDocument('country', 'country1', [
            Query::select(['*', 'city.name'])
        ]);

        $this->assertEquals('London', $country->getAttribute('city')->getAttribute('name'));
        $this->assertArrayNotHasKey('code', $country->getAttribute('city'));

        $country1 = $this->getDatabase()->getDocument('country', 'country1');

        // Update root document attribute without altering relationship
        $country1 = $this->getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('name', 'Country 1 Updated')
        );

        $this->assertEquals('Country 1 Updated', $country1->getAttribute('name'));
        $country1 = $this->getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('Country 1 Updated', $country1->getAttribute('name'));

        $city2 = $this->getDatabase()->getDocument('city', 'city2');

        // Update inverse root document attribute without altering relationship
        $city2 = $this->getDatabase()->updateDocument(
            'city',
            $city2->getId(),
            $city2->setAttribute('name', 'City 2 Updated')
        );

        $this->assertEquals('City 2 Updated', $city2->getAttribute('name'));
        $city2 = $this->getDatabase()->getDocument('city', 'city2');
        $this->assertEquals('City 2 Updated', $city2->getAttribute('name'));

        // Update nested document attribute
        $country1 = $this->getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute(
                'city',
                $country1
                ->getAttribute('city')
                ->setAttribute('name', 'City 1 Updated')
            )
        );

        $this->assertEquals('City 1 Updated', $country1->getAttribute('city')->getAttribute('name'));
        $country1 = $this->getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('City 1 Updated', $country1->getAttribute('city')->getAttribute('name'));

        // Update inverse nested document attribute
        $city2 = $this->getDatabase()->updateDocument(
            'city',
            $city2->getId(),
            $city2->setAttribute(
                'country',
                $city2
                ->getAttribute('country')
                ->setAttribute('name', 'Country 2 Updated')
            )
        );

        $this->assertEquals('Country 2 Updated', $city2->getAttribute('country')->getAttribute('name'));
        $city2 = $this->getDatabase()->getDocument('city', 'city2');
        $this->assertEquals('Country 2 Updated', $city2->getAttribute('country')->getAttribute('name'));

        // Create new document with no relationship
        $country5 = $this->getDatabase()->createDocument('country', new Document([
            '$id' => 'country5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Country 5',
        ]));

        // Update to relate to created document
        $country5 = $this->getDatabase()->updateDocument(
            'country',
            $country5->getId(),
            $country5->setAttribute('city', new Document([
                '$id' => 'city5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => 'City 5',
                'code' => 'C5',
            ]))
        );

        $this->assertEquals('city5', $country5->getAttribute('city')['$id']);
        $country5 = $this->getDatabase()->getDocument('country', 'country5');
        $this->assertEquals('city5', $country5->getAttribute('city')['$id']);

        // Create new document with no relationship
        $city6 = $this->getDatabase()->createDocument('city', new Document([
            '$id' => 'city6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'City6',
            'code' => 'C6',
        ]));

        // Update to relate to created document
        $city6 = $this->getDatabase()->updateDocument(
            'city',
            $city6->getId(),
            $city6->setAttribute('country', new Document([
                '$id' => 'country6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => 'Country 6',
            ]))
        );

        $this->assertEquals('country6', $city6->getAttribute('country')['$id']);
        $city6 = $this->getDatabase()->getDocument('city', 'city6');
        $this->assertEquals('country6', $city6->getAttribute('country')['$id']);

        // One to one can't relate to multiple documents, unique index throws duplicate
        try {
            $this->getDatabase()->updateDocument(
                'country',
                $country1->getId(),
                $country1->setAttribute('city', 'city2')
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $city1 = $this->getDatabase()->getDocument('city', 'city1');

        // Set relationship to null
        $city1 = $this->getDatabase()->updateDocument(
            'city',
            $city1->getId(),
            $city1->setAttribute('country', null)
        );

        $this->assertEquals(null, $city1->getAttribute('country'));
        $city1 = $this->getDatabase()->getDocument('city', 'city1');
        $this->assertEquals(null, $city1->getAttribute('country'));

        // Create a new city with no relation
        $city7 = $this->getDatabase()->createDocument('city', new Document([
            '$id' => 'city7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Copenhagen',
            'code' => 'CPH',
        ]));

        // Update document with relation to new document
        $this->getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('city', 'city7')
        );

        // Relate existing document to new document as nested data
        $this->getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('city', $city7)
        );

        // Create a new country with no relation
        $this->getDatabase()->createDocument('country', new Document([
            '$id' => 'country7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Denmark'
        ]));

        // Update inverse document with new related document
        $this->getDatabase()->updateDocument(
            'city',
            $city1->getId(),
            $city1->setAttribute('country', 'country7')
        );

        // Rename relationship keys on both sides
        $this->getDatabase()->updateRelationship(
            'country',
            'city',
            'newCity',
            'newCountry'
        );

        // Get document with new relationship key
        $city = $this->getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals('country7', $country['$id']);

        // Get inverse document with new relationship key
        $country = $this->getDatabase()->getDocument('country', 'country7');
        $city = $country->getAttribute('newCity');
        $this->assertEquals('city1', $city['$id']);

        // Create a new country with no relation
        $this->getDatabase()->createDocument('country', new Document([
            '$id' => 'country8',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Denmark'
        ]));

        // Can delete parent document with no relation with on delete set to restrict
        $deleted = $this->getDatabase()->deleteDocument('country', 'country8');
        $this->assertEquals(1, $deleted);

        $country8 = $this->getDatabase()->getDocument('country', 'country8');
        $this->assertEquals(true, $country8->isEmpty());


        // Cannot delete document while still related to another with on delete set to restrict
        try {
            $this->getDatabase()->deleteDocument('country', 'country1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $this->getDatabase()->updateRelationship(
            collection: 'country',
            id: 'newCity',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $this->getDatabase()->updateDocument('city', 'city1', new Document(['newCountry' => null, '$id' => 'city1']));
        $city1 = $this->getDatabase()->getDocument('city', 'city1');
        $this->assertNull($city1->getAttribute('newCountry'));

        // Check Delete TwoWay TRUE && RELATION_MUTATE_SET_NULL && related value NULL
        $this->assertTrue($this->getDatabase()->deleteDocument('city', 'city1'));
        $city1 = $this->getDatabase()->getDocument('city', 'city1');
        $this->assertTrue($city1->isEmpty());

        // Delete parent, will set child relationship to null for two-way
        $this->getDatabase()->deleteDocument('country', 'country1');

        // Check relation was set to null
        $city7 = $this->getDatabase()->getDocument('city', 'city7');
        $this->assertEquals(null, $city7->getAttribute('country', ''));

        // Delete child, set parent relationship to null for two-way
        $this->getDatabase()->deleteDocument('city', 'city2');

        // Check relation was set to null
        $country2 = $this->getDatabase()->getDocument('country', 'country2');
        $this->assertEquals(null, $country2->getAttribute('city', ''));

        // Relate again
        $this->getDatabase()->updateDocument(
            'city',
            $city7->getId(),
            $city7->setAttribute('newCountry', 'country2')
        );

        // Change on delete to cascade
        $this->getDatabase()->updateRelationship(
            collection: 'country',
            id: 'newCity',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $this->getDatabase()->deleteDocument('country', 'country7');

        // Check parent and child were deleted
        $library = $this->getDatabase()->getDocument('country', 'country7');
        $this->assertEquals(true, $library->isEmpty());

        $library = $this->getDatabase()->getDocument('city', 'city1');
        $this->assertEquals(true, $library->isEmpty());

        // Delete child, will delete parent for two-way
        $this->getDatabase()->deleteDocument('city', 'city7');

        // Check parent and child were deleted
        $library = $this->getDatabase()->getDocument('city', 'city7');
        $this->assertEquals(true, $library->isEmpty());

        $library = $this->getDatabase()->getDocument('country', 'country2');
        $this->assertEquals(true, $library->isEmpty());

        // Create new document to check after deleting relationship
        $this->getDatabase()->createDocument('city', new Document([
            '$id' => 'city7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Munich',
            'code' => 'MUC',
            'newCountry' => [
                '$id' => 'country7',
                'name' => 'Germany'
            ]
        ]));

        // Delete relationship
        $this->getDatabase()->deleteRelationship(
            'country',
            'newCity'
        );

        // Try to get document again
        $country = $this->getDatabase()->getDocument('country', 'country4');
        $city = $country->getAttribute('newCity');
        $this->assertEquals(null, $city);

        // Try to get inverse document again
        $city = $this->getDatabase()->getDocument('city', 'city7');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals(null, $country);
    }

    public function testIdenticalTwoWayKeyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('parent');
        $this->getDatabase()->createCollection('child');

        $this->getDatabase()->createRelationship(
            collection: 'parent',
            relatedCollection: 'child',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'child1'
        );

        try {
            $this->getDatabase()->createRelationship(
                collection: 'parent',
                relatedCollection: 'child',
                type: Database::RELATION_ONE_TO_MANY,
                id: 'children',
            );
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Related attribute already exists', $e->getMessage());
        }

        $this->getDatabase()->createRelationship(
            collection: 'parent',
            relatedCollection: 'child',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'children',
            twoWayKey: 'parent_id'
        );

        $collection = $this->getDatabase()->getCollection('parent');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'child1') {
                $this->assertEquals('parent', $attribute['options']['twoWayKey']);
            }

            if ($attribute['key'] === 'children') {
                $this->assertEquals('parent_id', $attribute['options']['twoWayKey']);
            }
        }

        $this->getDatabase()->createDocument('parent', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'child1' => [
                '$id' => 'foo',
                '$permissions' => [Permission::read(Role::any())],
            ],
            'children' => [
                [
                    '$id' => 'bar',
                    '$permissions' => [Permission::read(Role::any())],
                ],
            ],
        ]));

        $documents = $this->getDatabase()->find('parent', []);
        $document  = array_pop($documents);
        $this->assertArrayHasKey('child1', $document);
        $this->assertEquals('foo', $document->getAttribute('child1')->getId());
        $this->assertArrayHasKey('children', $document);
        $this->assertEquals('bar', $document->getAttribute('children')[0]->getId());

        try {
            $this->getDatabase()->updateRelationship(
                collection: 'parent',
                id: 'children',
                newKey: 'child1'
            );
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Attribute already exists', $e->getMessage());
        }

        try {
            $this->getDatabase()->updateRelationship(
                collection: 'parent',
                id: 'children',
                newTwoWayKey: 'parent'
            );
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Related attribute already exists', $e->getMessage());
        }
    }

    public function testOneToManyOneWayRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('artist');
        $this->getDatabase()->createCollection('album');

        $this->getDatabase()->createAttribute('artist', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('album', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('album', 'price', Database::VAR_FLOAT, 0, true);

        $this->getDatabase()->createRelationship(
            collection: 'artist',
            relatedCollection: 'album',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'albums'
        );

        // Check metadata for collection
        $collection = $this->getDatabase()->getCollection('artist');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'albums') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('albums', $attribute['$id']);
                $this->assertEquals('albums', $attribute['key']);
                $this->assertEquals('album', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('artist', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $artist1 = $this->getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 1',
            'albums' => [
                [
                    '$id' => 'album1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any())
                    ],
                    'name' => 'Album 1',
                    'price' => 9.99,
                ],
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $this->getDatabase()->updateDocument('artist', 'artist1', $artist1->setAttribute('albums', ['album1', 'no-album']));

        $artist1Document = $this->getDatabase()->getDocument('artist', 'artist1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($artist1Document->getAttribute('albums')));

        // Create document with relationship with related ID
        $this->getDatabase()->createDocument('album', new Document([
            '$id' => 'album2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Album 2',
            'price' => 19.99,
        ]));
        $this->getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 2',
            'albums' => [
                'album2',
                [
                    '$id' => 'album33',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Album 3',
                    'price' => 33.33,
                ]
            ]
        ]));

        $documents = $this->getDatabase()->find('artist', [
            Query::select(['name']),
            Query::limit(1)
        ]);
        $this->assertArrayNotHasKey('albums', $documents[0]);

        // Get document with relationship
        $artist = $this->getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album1', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);

        $artist = $this->getDatabase()->getDocument('artist', 'artist2');
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album2', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);
        $this->assertEquals('album33', $albums[1]['$id']);
        $this->assertCount(2, $albums);

        // Get related document
        $album = $this->getDatabase()->getDocument('album', 'album1');
        $this->assertArrayNotHasKey('artist', $album);

        $album = $this->getDatabase()->getDocument('album', 'album2');
        $this->assertArrayNotHasKey('artist', $album);

        $artists = $this->getDatabase()->find('artist');

        $this->assertEquals(2, \count($artists));

        // Select related document attributes
        $artist = $this->getDatabase()->findOne('artist', [
            Query::select(['*', 'albums.name'])
        ]);

        if ($artist->isEmpty()) {
            $this->fail('Artist not found');
        }

        $this->assertEquals('Album 1', $artist->getAttribute('albums')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('price', $artist->getAttribute('albums')[0]);

        $artist = $this->getDatabase()->getDocument('artist', 'artist1', [
            Query::select(['*', 'albums.name'])
        ]);

        $this->assertEquals('Album 1', $artist->getAttribute('albums')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('price', $artist->getAttribute('albums')[0]);

        // Update root document attribute without altering relationship
        $artist1 = $this->getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('name', 'Artist 1 Updated')
        );

        $this->assertEquals('Artist 1 Updated', $artist1->getAttribute('name'));
        $artist1 = $this->getDatabase()->getDocument('artist', 'artist1');
        $this->assertEquals('Artist 1 Updated', $artist1->getAttribute('name'));

        // Update nested document attribute
        $albums = $artist1->getAttribute('albums', []);
        $albums[0]->setAttribute('name', 'Album 1 Updated');

        $artist1 = $this->getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', $albums)
        );

        $this->assertEquals('Album 1 Updated', $artist1->getAttribute('albums')[0]->getAttribute('name'));
        $artist1 = $this->getDatabase()->getDocument('artist', 'artist1');
        $this->assertEquals('Album 1 Updated', $artist1->getAttribute('albums')[0]->getAttribute('name'));

        $albumId = $artist1->getAttribute('albums')[0]->getAttribute('$id');
        $albumDocument = $this->getDatabase()->getDocument('album', $albumId);
        $albumDocument->setAttribute('name', 'Album 1 Updated!!!');
        $this->getDatabase()->updateDocument('album', $albumDocument->getId(), $albumDocument);
        $albumDocument = $this->getDatabase()->getDocument('album', $albumDocument->getId());
        $artist1 = $this->getDatabase()->getDocument('artist', $artist1->getId());

        $this->assertEquals('Album 1 Updated!!!', $albumDocument['name']);
        $this->assertEquals($albumDocument->getId(), $artist1->getAttribute('albums')[0]->getId());
        $this->assertEquals($albumDocument->getAttribute('name'), $artist1->getAttribute('albums')[0]->getAttribute('name'));

        // Create new document with no relationship
        $artist3 = $this->getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 3',
        ]));

        // Update to relate to created document
        $artist3 = $this->getDatabase()->updateDocument(
            'artist',
            $artist3->getId(),
            $artist3->setAttribute('albums', [new Document([
                '$id' => 'album3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Album 3',
                'price' => 29.99,
            ])])
        );

        $this->assertEquals('Album 3', $artist3->getAttribute('albums')[0]->getAttribute('name'));
        $artist3 = $this->getDatabase()->getDocument('artist', 'artist3');
        $this->assertEquals('Album 3', $artist3->getAttribute('albums')[0]->getAttribute('name'));

        // Update document with new related documents, will remove existing relations
        $this->getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', ['album2'])
        );

        // Update document with new related documents, will remove existing relations
        $this->getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', ['album1', 'album2'])
        );

        // Rename relationship key
        $this->getDatabase()->updateRelationship(
            'artist',
            'albums',
            'newAlbums'
        );

        // Get document with new relationship key
        $artist = $this->getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('newAlbums');
        $this->assertEquals('album1', $albums[0]['$id']);

        // Create new document with no relationship
        $this->getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 4',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $this->getDatabase()->deleteDocument('artist', 'artist4');
        $this->assertEquals(true, $deleted);

        $artist4 = $this->getDatabase()->getDocument('artist', 'artist4');
        $this->assertEquals(true, $artist4->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $this->getDatabase()->deleteDocument('artist', 'artist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $this->getDatabase()->updateRelationship(
            collection: 'artist',
            id: 'newAlbums',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, set child relationship to null
        $this->getDatabase()->deleteDocument('artist', 'artist1');

        // Check relation was set to null
        $album2 = $this->getDatabase()->getDocument('album', 'album2');
        $this->assertEquals(null, $album2->getAttribute('artist', ''));

        // Relate again
        $this->getDatabase()->updateDocument(
            'album',
            $album2->getId(),
            $album2->setAttribute('artist', 'artist2')
        );

        // Change on delete to cascade
        $this->getDatabase()->updateRelationship(
            collection: 'artist',
            id: 'newAlbums',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $this->getDatabase()->deleteDocument('artist', 'artist2');

        // Check parent and child were deleted
        $library = $this->getDatabase()->getDocument('artist', 'artist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $this->getDatabase()->getDocument('album', 'album2');
        $this->assertEquals(true, $library->isEmpty());

        $albums = [];
        for ($i = 1 ; $i <= 50 ; $i++) {
            $albums[] = [
                '$id' => 'album_' . $i,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'album ' . $i . ' ' . 'Artist 100',
                'price' => 100,
            ];
        }

        $artist = $this->getDatabase()->createDocument('artist', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 100',
            'newAlbums' => $albums
        ]));

        $artist = $this->getDatabase()->getDocument('artist', $artist->getId());
        $this->assertCount(50, $artist->getAttribute('newAlbums'));

        $albums = $this->getDatabase()->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999)
        ]);

        $this->assertCount(50, $albums);

        $count = $this->getDatabase()->count('album', [
            Query::equal('artist', [$artist->getId()]),
        ]);

        $this->assertEquals(50, $count);

        $this->getDatabase()->deleteDocument('album', 'album_1');
        $artist = $this->getDatabase()->getDocument('artist', $artist->getId());
        $this->assertCount(49, $artist->getAttribute('newAlbums'));

        $this->getDatabase()->deleteDocument('artist', $artist->getId());

        $albums = $this->getDatabase()->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999)
        ]);

        $this->assertCount(0, $albums);

        // Delete relationship
        $this->getDatabase()->deleteRelationship(
            'artist',
            'newAlbums'
        );

        // Try to get document again
        $artist = $this->getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('newAlbums', '');
        $this->assertEquals(null, $albums);
    }

    public function testOneToManyTwoWayRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('customer');
        $this->getDatabase()->createCollection('account');

        $this->getDatabase()->createAttribute('customer', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('account', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('account', 'number', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'customer',
            relatedCollection: 'account',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'accounts'
        );

        // Check metadata for collection
        $collection = $this->getDatabase()->getCollection('customer');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'accounts') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('accounts', $attribute['$id']);
                $this->assertEquals('accounts', $attribute['key']);
                $this->assertEquals('account', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('customer', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = $this->getDatabase()->getCollection('account');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'customer') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('customer', $attribute['$id']);
                $this->assertEquals('customer', $attribute['key']);
                $this->assertEquals('customer', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('accounts', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $customer1 = $this->getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 1',
            'accounts' => [
                [
                    '$id' => 'account1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Account 1',
                    'number' => '123456789',
                ],
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $this->getDatabase()->updateDocument('customer', 'customer1', $customer1->setAttribute('accounts', ['account1','no-account']));

        $customer1Document = $this->getDatabase()->getDocument('customer', 'customer1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($customer1Document->getAttribute('accounts')));

        // Create document with relationship with related ID
        $account2 = $this->getDatabase()->createDocument('account', new Document([
            '$id' => 'account2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 2',
            'number' => '987654321',
        ]));
        $this->getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 2',
            'accounts' => [
                'account2'
            ]
        ]));

        // Create from child side
        $this->getDatabase()->createDocument('account', new Document([
            '$id' => 'account3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 3',
            'number' => '123456789',
            'customer' => [
                '$id' => 'customer3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Customer 3'
            ]
        ]));
        $this->getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 4',
        ]));
        $this->getDatabase()->createDocument('account', new Document([
            '$id' => 'account4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 4',
            'number' => '123456789',
            'customer' => 'customer4'
        ]));

        // Get documents with relationship
        $customer = $this->getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account1', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $this->getDatabase()->getDocument('customer', 'customer2');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account2', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $this->getDatabase()->getDocument('customer', 'customer3');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account3', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $this->getDatabase()->getDocument('customer', 'customer4');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account4', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        // Get related documents
        $account = $this->getDatabase()->getDocument('account', 'account1');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer1', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = $this->getDatabase()->getDocument('account', 'account2');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer2', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = $this->getDatabase()->getDocument('account', 'account3');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer3', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = $this->getDatabase()->getDocument('account', 'account4');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer4', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $customers = $this->getDatabase()->find('customer');

        $this->assertEquals(4, \count($customers));

        // Select related document attributes
        $customer = $this->getDatabase()->findOne('customer', [
            Query::select(['*', 'accounts.name'])
        ]);

        if ($customer->isEmpty()) {
            throw new Exception('Customer not found');
        }

        $this->assertEquals('Account 1', $customer->getAttribute('accounts')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $customer->getAttribute('accounts')[0]);

        $customer = $this->getDatabase()->getDocument('customer', 'customer1', [
            Query::select(['*', 'accounts.name'])
        ]);

        $this->assertEquals('Account 1', $customer->getAttribute('accounts')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $customer->getAttribute('accounts')[0]);

        // Update root document attribute without altering relationship
        $customer1 = $this->getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('name', 'Customer 1 Updated')
        );

        $this->assertEquals('Customer 1 Updated', $customer1->getAttribute('name'));
        $customer1 = $this->getDatabase()->getDocument('customer', 'customer1');
        $this->assertEquals('Customer 1 Updated', $customer1->getAttribute('name'));

        $account2 = $this->getDatabase()->getDocument('account', 'account2');

        // Update inverse root document attribute without altering relationship
        $account2 = $this->getDatabase()->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute('name', 'Account 2 Updated')
        );

        $this->assertEquals('Account 2 Updated', $account2->getAttribute('name'));
        $account2 = $this->getDatabase()->getDocument('account', 'account2');
        $this->assertEquals('Account 2 Updated', $account2->getAttribute('name'));

        // Update nested document attribute
        $accounts = $customer1->getAttribute('accounts', []);
        $accounts[0]->setAttribute('name', 'Account 1 Updated');

        $customer1 = $this->getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', $accounts)
        );

        $this->assertEquals('Account 1 Updated', $customer1->getAttribute('accounts')[0]->getAttribute('name'));
        $customer1 = $this->getDatabase()->getDocument('customer', 'customer1');
        $this->assertEquals('Account 1 Updated', $customer1->getAttribute('accounts')[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $account2 = $this->getDatabase()->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute(
                'customer',
                $account2
                ->getAttribute('customer')
                ->setAttribute('name', 'Customer 2 Updated')
            )
        );

        $this->assertEquals('Customer 2 Updated', $account2->getAttribute('customer')->getAttribute('name'));
        $account2 = $this->getDatabase()->getDocument('account', 'account2');
        $this->assertEquals('Customer 2 Updated', $account2->getAttribute('customer')->getAttribute('name'));

        // Create new document with no relationship
        $customer5 = $this->getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 5',
        ]));

        // Update to relate to created document
        $customer5 = $this->getDatabase()->updateDocument(
            'customer',
            $customer5->getId(),
            $customer5->setAttribute('accounts', [new Document([
                '$id' => 'account5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Account 5',
                'number' => '123456789',
            ])])
        );

        $this->assertEquals('Account 5', $customer5->getAttribute('accounts')[0]->getAttribute('name'));
        $customer5 = $this->getDatabase()->getDocument('customer', 'customer5');
        $this->assertEquals('Account 5', $customer5->getAttribute('accounts')[0]->getAttribute('name'));

        // Create new child document with no relationship
        $account6 = $this->getDatabase()->createDocument('account', new Document([
            '$id' => 'account6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 6',
            'number' => '123456789',
        ]));

        // Update inverse to relate to created document
        $account6 = $this->getDatabase()->updateDocument(
            'account',
            $account6->getId(),
            $account6->setAttribute('customer', new Document([
                '$id' => 'customer6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Customer 6',
            ]))
        );

        $this->assertEquals('Customer 6', $account6->getAttribute('customer')->getAttribute('name'));
        $account6 = $this->getDatabase()->getDocument('account', 'account6');
        $this->assertEquals('Customer 6', $account6->getAttribute('customer')->getAttribute('name'));

        // Update document with new related document, will remove existing relations
        $this->getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', ['account2'])
        );

        // Update document with new related document
        $this->getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', ['account1', 'account2'])
        );

        // Update inverse document
        $this->getDatabase()->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute('customer', 'customer2')
        );

        // Rename relationship keys on both sides
        $this->getDatabase()->updateRelationship(
            'customer',
            'accounts',
            'newAccounts',
            'newCustomer'
        );

        // Get document with new relationship key
        $customer = $this->getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('newAccounts');
        $this->assertEquals('account1', $accounts[0]['$id']);

        // Get inverse document with new relationship key
        $account = $this->getDatabase()->getDocument('account', 'account1');
        $customer = $account->getAttribute('newCustomer');
        $this->assertEquals('customer1', $customer['$id']);

        // Create new document with no relationship
        $this->getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $this->getDatabase()->deleteDocument('customer', 'customer7');
        $this->assertEquals(true, $deleted);

        $customer7 = $this->getDatabase()->getDocument('customer', 'customer7');
        $this->assertEquals(true, $customer7->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $this->getDatabase()->deleteDocument('customer', 'customer1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $this->getDatabase()->updateRelationship(
            collection: 'customer',
            id: 'newAccounts',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, set child relationship to null
        $this->getDatabase()->deleteDocument('customer', 'customer1');

        // Check relation was set to null
        $account1 = $this->getDatabase()->getDocument('account', 'account1');
        $this->assertEquals(null, $account2->getAttribute('newCustomer', ''));

        // Relate again
        $this->getDatabase()->updateDocument(
            'account',
            $account1->getId(),
            $account1->setAttribute('newCustomer', 'customer2')
        );

        // Change on delete to cascade
        $this->getDatabase()->updateRelationship(
            collection: 'customer',
            id: 'newAccounts',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $this->getDatabase()->deleteDocument('customer', 'customer2');

        // Check parent and child were deleted
        $library = $this->getDatabase()->getDocument('customer', 'customer2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $this->getDatabase()->getDocument('account', 'account2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        $this->getDatabase()->deleteRelationship(
            'customer',
            'newAccounts'
        );

        // Try to get document again
        $customer = $this->getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('newAccounts');
        $this->assertEquals(null, $accounts);

        // Try to get inverse document again
        $accounts = $this->getDatabase()->getDocument('account', 'account1');
        $customer = $accounts->getAttribute('newCustomer');
        $this->assertEquals(null, $customer);
    }

    public function testManyToOneOneWayRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('review');
        $this->getDatabase()->createCollection('movie');

        $this->getDatabase()->createAttribute('review', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('movie', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('movie', 'length', Database::VAR_INTEGER, 0, true, formatOptions: ['min' => 0, 'max' => 999]);
        $this->getDatabase()->createAttribute('movie', 'date', Database::VAR_DATETIME, 0, false, filters: ['datetime']);
        $this->getDatabase()->createAttribute('review', 'date', Database::VAR_DATETIME, 0, false, filters: ['datetime']);
        $this->getDatabase()->createRelationship(
            collection: 'review',
            relatedCollection: 'movie',
            type: Database::RELATION_MANY_TO_ONE,
            twoWayKey: 'reviews'
        );

        // Check metadata for collection
        $collection = $this->getDatabase()->getCollection('review');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'movie') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('movie', $attribute['$id']);
                $this->assertEquals('movie', $attribute['key']);
                $this->assertEquals('movie', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('reviews', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = $this->getDatabase()->getCollection('movie');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'reviews') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('reviews', $attribute['$id']);
                $this->assertEquals('reviews', $attribute['key']);
                $this->assertEquals('review', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('movie', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $review1 = $this->getDatabase()->createDocument('review', new Document([
            '$id' => 'review1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 1',
            'date' => '2023-04-03 10:35:27.390',
            'movie' => [
                '$id' => 'movie1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Movie 1',
                'date' => '2023-04-03 10:35:27.390',
                'length' => 120,
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $this->getDatabase()->updateDocument('review', 'review1', $review1->setAttribute('movie', 'no-movie'));

        $review1Document = $this->getDatabase()->getDocument('review', 'review1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $review1Document->getAttribute('movie'));

        $this->getDatabase()->updateDocument('review', 'review1', $review1->setAttribute('movie', 'movie1'));

        // Create document with relationship to existing document by ID
        $review10 = $this->getDatabase()->createDocument('review', new Document([
            '$id' => 'review10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 10',
            'movie' => 'movie1',
            'date' => '2023-04-03 10:35:27.390',
        ]));

        // Create document with relationship with related ID
        $this->getDatabase()->createDocument('movie', new Document([
            '$id' => 'movie2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Movie 2',
            'length' => 90,
            'date' => '2023-04-03 10:35:27.390',
        ]));
        $this->getDatabase()->createDocument('review', new Document([
            '$id' => 'review2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 2',
            'movie' => 'movie2',
            'date' => '2023-04-03 10:35:27.390',
        ]));

        // Get document with relationship
        $review = $this->getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('movie', []);
        $this->assertEquals('movie1', $movie['$id']);
        $this->assertArrayNotHasKey('reviews', $movie);

        $documents = $this->getDatabase()->find('review', [
            Query::select(['date', 'movie.date'])
        ]);

        $this->assertCount(3, $documents);

        $document = $documents[0];
        $this->assertArrayHasKey('date', $document);
        $this->assertArrayHasKey('movie', $document);
        $this->assertArrayHasKey('date', $document->getAttribute('movie'));
        $this->assertArrayNotHasKey('name', $document);
        $this->assertEquals(29, strlen($document['date'])); // checks filter
        $this->assertEquals(29, strlen($document['movie']['date']));

        $review = $this->getDatabase()->getDocument('review', 'review2');
        $movie = $review->getAttribute('movie', []);
        $this->assertEquals('movie2', $movie['$id']);
        $this->assertArrayNotHasKey('reviews', $movie);

        // Get related document
        $movie = $this->getDatabase()->getDocument('movie', 'movie1');
        $this->assertArrayNotHasKey('reviews', $movie);

        $movie = $this->getDatabase()->getDocument('movie', 'movie2');
        $this->assertArrayNotHasKey('reviews', $movie);

        $reviews = $this->getDatabase()->find('review');

        $this->assertEquals(3, \count($reviews));

        // Select related document attributes
        $review = $this->getDatabase()->findOne('review', [
            Query::select(['*', 'movie.name'])
        ]);

        if ($review->isEmpty()) {
            throw new Exception('Review not found');
        }

        $this->assertEquals('Movie 1', $review->getAttribute('movie')->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $review->getAttribute('movie'));

        $review = $this->getDatabase()->getDocument('review', 'review1', [
            Query::select(['*', 'movie.name'])
        ]);

        $this->assertEquals('Movie 1', $review->getAttribute('movie')->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $review->getAttribute('movie'));

        // Update root document attribute without altering relationship
        $review1 = $this->getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('name', 'Review 1 Updated')
        );

        $this->assertEquals('Review 1 Updated', $review1->getAttribute('name'));
        $review1 = $this->getDatabase()->getDocument('review', 'review1');
        $this->assertEquals('Review 1 Updated', $review1->getAttribute('name'));

        // Update nested document attribute
        $movie = $review1->getAttribute('movie');
        $movie->setAttribute('name', 'Movie 1 Updated');

        $review1 = $this->getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('movie', $movie)
        );

        $this->assertEquals('Movie 1 Updated', $review1->getAttribute('movie')->getAttribute('name'));
        $review1 = $this->getDatabase()->getDocument('review', 'review1');
        $this->assertEquals('Movie 1 Updated', $review1->getAttribute('movie')->getAttribute('name'));

        // Create new document with no relationship
        $review5 = $this->getDatabase()->createDocument('review', new Document([
            '$id' => 'review5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 5',
        ]));

        // Update to relate to created document
        $review5 = $this->getDatabase()->updateDocument(
            'review',
            $review5->getId(),
            $review5->setAttribute('movie', new Document([
                '$id' => 'movie5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Movie 5',
                'length' => 90,
            ]))
        );

        $this->assertEquals('Movie 5', $review5->getAttribute('movie')->getAttribute('name'));
        $review5 = $this->getDatabase()->getDocument('review', 'review5');
        $this->assertEquals('Movie 5', $review5->getAttribute('movie')->getAttribute('name'));

        // Update document with new related document
        $this->getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('movie', 'movie2')
        );

        // Rename relationship keys on both sides
        $this->getDatabase()->updateRelationship(
            'review',
            'movie',
            'newMovie',
        );

        // Get document with new relationship key
        $review = $this->getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('newMovie');
        $this->assertEquals('movie2', $movie['$id']);

        // Reset values
        $review1 = $this->getDatabase()->getDocument('review', 'review1');

        $this->getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('newMovie', 'movie1')
        );

        // Create new document with no relationship
        $this->getDatabase()->createDocument('movie', new Document([
            '$id' => 'movie3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Movie 3',
            'length' => 90,
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $this->getDatabase()->deleteDocument('movie', 'movie3');
        $this->assertEquals(true, $deleted);

        $movie3 = $this->getDatabase()->getDocument('movie', 'movie3');
        $this->assertEquals(true, $movie3->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $this->getDatabase()->deleteDocument('movie', 'movie1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $this->getDatabase()->updateRelationship(
            collection: 'review',
            id: 'newMovie',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete child, set parent relationship to null
        $this->getDatabase()->deleteDocument('movie', 'movie1');

        // Check relation was set to null
        $review1 = $this->getDatabase()->getDocument('review', 'review1');
        $this->assertEquals(null, $review1->getAttribute('newMovie'));

        // Change on delete to cascade
        $this->getDatabase()->updateRelationship(
            collection: 'review',
            id: 'newMovie',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete child, will delete parent
        $this->getDatabase()->deleteDocument('movie', 'movie2');

        // Check parent and child were deleted
        $library = $this->getDatabase()->getDocument('movie', 'movie2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $this->getDatabase()->getDocument('review', 'review2');
        $this->assertEquals(true, $library->isEmpty());


        // Delete relationship
        $this->getDatabase()->deleteRelationship(
            'review',
            'newMovie'
        );

        // Try to get document again
        $review = $this->getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('newMovie');
        $this->assertEquals(null, $movie);
    }

    public function testManyToOneTwoWayRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('product');
        $this->getDatabase()->createCollection('store');

        $this->getDatabase()->createAttribute('store', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('store', 'opensAt', Database::VAR_STRING, 5, true);

        $this->getDatabase()->createAttribute(
            collection: 'product',
            id: 'name',
            type: Database::VAR_STRING,
            size: 255,
            required: true
        );

        $this->getDatabase()->createRelationship(
            collection: 'product',
            relatedCollection: 'store',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            twoWayKey: 'products'
        );

        // Check metadata for collection
        $collection = $this->getDatabase()->getCollection('product');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'store') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('store', $attribute['$id']);
                $this->assertEquals('store', $attribute['key']);
                $this->assertEquals('store', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('products', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = $this->getDatabase()->getCollection('store');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'products') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('products', $attribute['$id']);
                $this->assertEquals('products', $attribute['key']);
                $this->assertEquals('product', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('store', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $product1 = $this->getDatabase()->createDocument('product', new Document([
            '$id' => 'product1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 1',
            'store' => [
                '$id' => 'store1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Store 1',
                'opensAt' => '09:00',
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $this->getDatabase()->updateDocument('product', 'product1', $product1->setAttribute('store', 'no-store'));

        $product1Document = $this->getDatabase()->getDocument('product', 'product1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $product1Document->getAttribute('store'));

        $this->getDatabase()->updateDocument('product', 'product1', $product1->setAttribute('store', 'store1'));

        // Create document with relationship with related ID
        $this->getDatabase()->createDocument('store', new Document([
            '$id' => 'store2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 2',
            'opensAt' => '09:30',
        ]));
        $this->getDatabase()->createDocument('product', new Document([
            '$id' => 'product2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 2',
            'store' => 'store2',
        ]));

        // Create from child side
        $this->getDatabase()->createDocument('store', new Document([
            '$id' => 'store3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 3',
            'opensAt' => '11:30',
            'products' => [
                [
                    '$id' => 'product3',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Product 3',
                ],
            ],
        ]));

        $this->getDatabase()->createDocument('product', new Document([
            '$id' => 'product4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 4',
        ]));
        $this->getDatabase()->createDocument('store', new Document([
            '$id' => 'store4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 4',
            'opensAt' => '11:30',
            'products' => [
                'product4',
            ],
        ]));

        // Get document with relationship
        $product = $this->getDatabase()->getDocument('product', 'product1');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store1', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = $this->getDatabase()->getDocument('product', 'product2');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store2', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = $this->getDatabase()->getDocument('product', 'product3');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store3', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = $this->getDatabase()->getDocument('product', 'product4');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store4', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        // Get related document
        $store = $this->getDatabase()->getDocument('store', 'store1');
        $products = $store->getAttribute('products');
        $this->assertEquals('product1', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = $this->getDatabase()->getDocument('store', 'store2');
        $products = $store->getAttribute('products');
        $this->assertEquals('product2', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = $this->getDatabase()->getDocument('store', 'store3');
        $products = $store->getAttribute('products');
        $this->assertEquals('product3', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = $this->getDatabase()->getDocument('store', 'store4');
        $products = $store->getAttribute('products');
        $this->assertEquals('product4', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $products = $this->getDatabase()->find('product');

        $this->assertEquals(4, \count($products));

        // Select related document attributes
        $product = $this->getDatabase()->findOne('product', [
            Query::select(['*', 'store.name'])
        ]);

        if ($product->isEmpty()) {
            throw new Exception('Product not found');
        }

        $this->assertEquals('Store 1', $product->getAttribute('store')->getAttribute('name'));
        $this->assertArrayNotHasKey('opensAt', $product->getAttribute('store'));

        $product = $this->getDatabase()->getDocument('product', 'product1', [
            Query::select(['*', 'store.name'])
        ]);

        $this->assertEquals('Store 1', $product->getAttribute('store')->getAttribute('name'));
        $this->assertArrayNotHasKey('opensAt', $product->getAttribute('store'));

        // Update root document attribute without altering relationship
        $product1 = $this->getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('name', 'Product 1 Updated')
        );

        $this->assertEquals('Product 1 Updated', $product1->getAttribute('name'));
        $product1 = $this->getDatabase()->getDocument('product', 'product1');
        $this->assertEquals('Product 1 Updated', $product1->getAttribute('name'));

        // Update inverse document attribute without altering relationship
        $store1 = $this->getDatabase()->getDocument('store', 'store1');
        $store1 = $this->getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('name', 'Store 1 Updated')
        );

        $this->assertEquals('Store 1 Updated', $store1->getAttribute('name'));
        $store1 = $this->getDatabase()->getDocument('store', 'store1');
        $this->assertEquals('Store 1 Updated', $store1->getAttribute('name'));

        // Update nested document attribute
        $store = $product1->getAttribute('store');
        $store->setAttribute('name', 'Store 1 Updated');

        $product1 = $this->getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('store', $store)
        );

        $this->assertEquals('Store 1 Updated', $product1->getAttribute('store')->getAttribute('name'));
        $product1 = $this->getDatabase()->getDocument('product', 'product1');
        $this->assertEquals('Store 1 Updated', $product1->getAttribute('store')->getAttribute('name'));

        // Update inverse nested document attribute
        $product = $store1->getAttribute('products')[0];
        $product->setAttribute('name', 'Product 1 Updated');

        $store1 = $this->getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('products', [$product])
        );

        $this->assertEquals('Product 1 Updated', $store1->getAttribute('products')[0]->getAttribute('name'));
        $store1 = $this->getDatabase()->getDocument('store', 'store1');
        $this->assertEquals('Product 1 Updated', $store1->getAttribute('products')[0]->getAttribute('name'));

        // Create new document with no relationship
        $product5 = $this->getDatabase()->createDocument('product', new Document([
            '$id' => 'product5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 5',
        ]));

        // Update to relate to created document
        $product5 = $this->getDatabase()->updateDocument(
            'product',
            $product5->getId(),
            $product5->setAttribute('store', new Document([
                '$id' => 'store5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Store 5',
                'opensAt' => '09:00',
            ]))
        );

        $this->assertEquals('Store 5', $product5->getAttribute('store')->getAttribute('name'));
        $product5 = $this->getDatabase()->getDocument('product', 'product5');
        $this->assertEquals('Store 5', $product5->getAttribute('store')->getAttribute('name'));

        // Create new child document with no relationship
        $store6 = $this->getDatabase()->createDocument('store', new Document([
            '$id' => 'store6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 6',
            'opensAt' => '09:00',
        ]));

        // Update inverse to related to newly created document
        $store6 = $this->getDatabase()->updateDocument(
            'store',
            $store6->getId(),
            $store6->setAttribute('products', [new Document([
                '$id' => 'product6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Product 6',
            ])])
        );

        $this->assertEquals('Product 6', $store6->getAttribute('products')[0]->getAttribute('name'));
        $store6 = $this->getDatabase()->getDocument('store', 'store6');
        $this->assertEquals('Product 6', $store6->getAttribute('products')[0]->getAttribute('name'));

        // Update document with new related document
        $this->getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('store', 'store2')
        );

        $store1 = $this->getDatabase()->getDocument('store', 'store1');

        // Update inverse document
        $this->getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('products', ['product1'])
        );

        $store2 = $this->getDatabase()->getDocument('store', 'store2');

        // Update inverse document
        $this->getDatabase()->updateDocument(
            'store',
            $store2->getId(),
            $store2->setAttribute('products', ['product1', 'product2'])
        );

        // Rename relationship keys on both sides
        $this->getDatabase()->updateRelationship(
            'product',
            'store',
            'newStore',
            'newProducts'
        );

        // Get document with new relationship key
        $store = $this->getDatabase()->getDocument('store', 'store2');
        $products = $store->getAttribute('newProducts');
        $this->assertEquals('product1', $products[0]['$id']);

        // Get inverse document with new relationship key
        $product = $this->getDatabase()->getDocument('product', 'product1');
        $store = $product->getAttribute('newStore');
        $this->assertEquals('store2', $store['$id']);

        // Reset relationships
        $store1 = $this->getDatabase()->getDocument('store', 'store1');
        $this->getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('newProducts', ['product1'])
        );

        // Create new document with no relationship
        $this->getDatabase()->createDocument('store', new Document([
            '$id' => 'store7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 7',
            'opensAt' => '09:00',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $this->getDatabase()->deleteDocument('store', 'store7');
        $this->assertEquals(true, $deleted);

        $store7 = $this->getDatabase()->getDocument('store', 'store7');
        $this->assertEquals(true, $store7->isEmpty());

        // Try to delete child while still related to another with on delete: restrict
        try {
            $this->getDatabase()->deleteDocument('store', 'store1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Delete parent while still related to another with on delete: restrict
        $result = $this->getDatabase()->deleteDocument('product', 'product5');
        $this->assertEquals(true, $result);

        // Change on delete to set null
        $this->getDatabase()->updateRelationship(
            collection: 'product',
            id: 'newStore',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete child, set parent relationship to null
        $this->getDatabase()->deleteDocument('store', 'store1');

        // Check relation was set to null
        $this->getDatabase()->getDocument('product', 'product1');
        $this->assertEquals(null, $product1->getAttribute('newStore'));

        // Change on delete to cascade
        $this->getDatabase()->updateRelationship(
            collection: 'product',
            id: 'newStore',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete child, will delete parent
        $this->getDatabase()->deleteDocument('store', 'store2');

        // Check parent and child were deleted
        $library = $this->getDatabase()->getDocument('store', 'store2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $this->getDatabase()->getDocument('product', 'product2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        $this->getDatabase()->deleteRelationship(
            'product',
            'newStore'
        );

        // Try to get document again
        $products = $this->getDatabase()->getDocument('product', 'product1');
        $store = $products->getAttribute('newStore');
        $this->assertEquals(null, $store);

        // Try to get inverse document again
        $store = $this->getDatabase()->getDocument('store', 'store1');
        $products = $store->getAttribute('newProducts');
        $this->assertEquals(null, $products);
    }

    public function testManyToManyOneWayRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('playlist');
        $this->getDatabase()->createCollection('song');

        $this->getDatabase()->createAttribute('playlist', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('song', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('song', 'length', Database::VAR_INTEGER, 0, true);

        $this->getDatabase()->createRelationship(
            collection: 'playlist',
            relatedCollection: 'song',
            type: Database::RELATION_MANY_TO_MANY,
            id: 'songs'
        );

        // Check metadata for collection
        $collection = $this->getDatabase()->getCollection('playlist');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'songs') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('songs', $attribute['$id']);
                $this->assertEquals('songs', $attribute['key']);
                $this->assertEquals('song', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('playlist', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $playlist1 = $this->getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 1',
            'songs' => [
                [
                    '$id' => 'song1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Song 1',
                    'length' => 180,
                ],
            ],
        ]));

        // Create document with relationship with related ID
        $this->getDatabase()->createDocument('song', new Document([
            '$id' => 'song2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Song 2',
            'length' => 140,
        ]));
        $this->getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 2',
            'songs' => [
                'song2'
            ]
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $this->getDatabase()->updateDocument('playlist', 'playlist1', $playlist1->setAttribute('songs', ['song1','no-song']));

        $playlist1Document = $this->getDatabase()->getDocument('playlist', 'playlist1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($playlist1Document->getAttribute('songs')));

        $documents = $this->getDatabase()->find('playlist', [
            Query::select(['name']),
            Query::limit(1)
        ]);

        $this->assertArrayNotHasKey('songs', $documents[0]);

        // Get document with relationship
        $playlist = $this->getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song1', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        $playlist = $this->getDatabase()->getDocument('playlist', 'playlist2');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song2', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        // Get related document
        $library = $this->getDatabase()->getDocument('song', 'song1');
        $this->assertArrayNotHasKey('songs', $library);

        $library = $this->getDatabase()->getDocument('song', 'song2');
        $this->assertArrayNotHasKey('songs', $library);

        $playlists = $this->getDatabase()->find('playlist');

        $this->assertEquals(2, \count($playlists));

        // Select related document attributes
        $playlist = $this->getDatabase()->findOne('playlist', [
            Query::select(['*', 'songs.name'])
        ]);

        if ($playlist->isEmpty()) {
            throw new Exception('Playlist not found');
        }

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        $playlist = $this->getDatabase()->getDocument('playlist', 'playlist1', [
            Query::select(['*', 'songs.name'])
        ]);

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        // Update root document attribute without altering relationship
        $playlist1 = $this->getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('name', 'Playlist 1 Updated')
        );

        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));
        $playlist1 = $this->getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));

        // Update nested document attribute
        $songs = $playlist1->getAttribute('songs', []);
        $songs[0]->setAttribute('name', 'Song 1 Updated');

        $playlist1 = $this->getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', $songs)
        );

        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));
        $playlist1 = $this->getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));

        // Create new document with no relationship
        $playlist5 = $this->getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 5',
        ]));

        // Update to relate to created document
        $playlist5 = $this->getDatabase()->updateDocument(
            'playlist',
            $playlist5->getId(),
            $playlist5->setAttribute('songs', [new Document([
                '$id' => 'song5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Song 5',
                'length' => 180,
            ])])
        );

        // Playlist relating to existing songs that belong to other playlists
        $this->getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 6',
            'songs' => [
                'song1',
                'song2',
                'song5'
            ]
        ]));

        $this->assertEquals('Song 5', $playlist5->getAttribute('songs')[0]->getAttribute('name'));
        $playlist5 = $this->getDatabase()->getDocument('playlist', 'playlist5');
        $this->assertEquals('Song 5', $playlist5->getAttribute('songs')[0]->getAttribute('name'));

        // Update document with new related document
        $this->getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', ['song2'])
        );

        // Rename relationship key
        $this->getDatabase()->updateRelationship(
            'playlist',
            'songs',
            'newSongs'
        );

        // Get document with new relationship key
        $playlist = $this->getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals('song2', $songs[0]['$id']);

        // Create new document with no relationship
        $this->getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 3',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $this->getDatabase()->deleteDocument('playlist', 'playlist3');
        $this->assertEquals(true, $deleted);

        $playlist3 = $this->getDatabase()->getDocument('playlist', 'playlist3');
        $this->assertEquals(true, $playlist3->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $this->getDatabase()->deleteDocument('playlist', 'playlist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $this->getDatabase()->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $playlist1 = $this->getDatabase()->getDocument('playlist', 'playlist1');

        // Reset relationships
        $this->getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('newSongs', ['song1'])
        );

        // Delete child, will delete junction
        $this->getDatabase()->deleteDocument('song', 'song1');

        // Check relation was set to null
        $playlist1 = $this->getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals(0, \count($playlist1->getAttribute('newSongs')));

        // Change on delete to cascade
        $this->getDatabase()->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $this->getDatabase()->deleteDocument('playlist', 'playlist2');

        // Check parent and child were deleted
        $library = $this->getDatabase()->getDocument('playlist', 'playlist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $this->getDatabase()->getDocument('song', 'song2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        $this->getDatabase()->deleteRelationship(
            'playlist',
            'newSongs'
        );

        // Try to get document again
        $playlist = $this->getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals(null, $songs);
    }

    public function testManyToManyTwoWayRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('students');
        $this->getDatabase()->createCollection('classes');

        $this->getDatabase()->createAttribute('students', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('classes', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('classes', 'number', Database::VAR_INTEGER, 0, true);

        $this->getDatabase()->createRelationship(
            collection: 'students',
            relatedCollection: 'classes',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        // Check metadata for collection
        $collection = $this->getDatabase()->getCollection('students');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'students') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('students', $attribute['$id']);
                $this->assertEquals('students', $attribute['key']);
                $this->assertEquals('students', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('classes', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = $this->getDatabase()->getCollection('classes');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'classes') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('classes', $attribute['$id']);
                $this->assertEquals('classes', $attribute['key']);
                $this->assertEquals('classes', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('students', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $student1 = $this->getDatabase()->createDocument('students', new Document([
            '$id' => 'student1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 1',
            'classes' => [
                [
                    '$id' => 'class1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Class 1',
                    'number' => 1,
                ],
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $this->getDatabase()->updateDocument('students', 'student1', $student1->setAttribute('classes', ['class1', 'no-class']));

        $student1Document = $this->getDatabase()->getDocument('students', 'student1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($student1Document->getAttribute('classes')));

        // Create document with relationship with related ID
        $this->getDatabase()->createDocument('classes', new Document([
            '$id' => 'class2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),

            ],
            'name' => 'Class 2',
            'number' => 2,
        ]));
        $this->getDatabase()->createDocument('students', new Document([
            '$id' => 'student2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 2',
            'classes' => [
                'class2'
            ],
        ]));

        // Create from child side
        $this->getDatabase()->createDocument('classes', new Document([
            '$id' => 'class3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Class 3',
            'number' => 3,
            'students' => [
                [
                    '$id' => 'student3',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Student 3',
                ]
            ],
        ]));
        $this->getDatabase()->createDocument('students', new Document([
            '$id' => 'student4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 4'
        ]));
        $this->getDatabase()->createDocument('classes', new Document([
            '$id' => 'class4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),

            ],
            'name' => 'Class 4',
            'number' => 4,
            'students' => [
                'student4'
            ],
        ]));

        // Get document with relationship
        $student = $this->getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class1', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $this->getDatabase()->getDocument('students', 'student2');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class2', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $this->getDatabase()->getDocument('students', 'student3');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class3', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $this->getDatabase()->getDocument('students', 'student4');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class4', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        // Get related document
        $class = $this->getDatabase()->getDocument('classes', 'class1');
        $student = $class->getAttribute('students');
        $this->assertEquals('student1', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = $this->getDatabase()->getDocument('classes', 'class2');
        $student = $class->getAttribute('students');
        $this->assertEquals('student2', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = $this->getDatabase()->getDocument('classes', 'class3');
        $student = $class->getAttribute('students');
        $this->assertEquals('student3', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = $this->getDatabase()->getDocument('classes', 'class4');
        $student = $class->getAttribute('students');
        $this->assertEquals('student4', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        // Select related document attributes
        $student = $this->getDatabase()->findOne('students', [
            Query::select(['*', 'classes.name'])
        ]);

        if ($student->isEmpty()) {
            throw new Exception('Student not found');
        }

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        $student = $this->getDatabase()->getDocument('students', 'student1', [
            Query::select(['*', 'classes.name'])
        ]);

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        // Update root document attribute without altering relationship
        $student1 = $this->getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('name', 'Student 1 Updated')
        );

        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));
        $student1 = $this->getDatabase()->getDocument('students', 'student1');
        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));

        // Update inverse root document attribute without altering relationship
        $class2 = $this->getDatabase()->getDocument('classes', 'class2');
        $class2 = $this->getDatabase()->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('name', 'Class 2 Updated')
        );

        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));
        $class2 = $this->getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));

        // Update nested document attribute
        $classes = $student1->getAttribute('classes', []);
        $classes[0]->setAttribute('name', 'Class 1 Updated');

        $student1 = $this->getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', $classes)
        );

        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));
        $student1 = $this->getDatabase()->getDocument('students', 'student1');
        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $students = $class2->getAttribute('students', []);
        $students[0]->setAttribute('name', 'Student 2 Updated');

        $class2 = $this->getDatabase()->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('students', $students)
        );

        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));
        $class2 = $this->getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));

        // Create new document with no relationship
        $student5 = $this->getDatabase()->createDocument('students', new Document([
            '$id' => 'student5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 5',
        ]));

        // Update to relate to created document
        $student5 = $this->getDatabase()->updateDocument(
            'students',
            $student5->getId(),
            $student5->setAttribute('classes', [new Document([
                '$id' => 'class5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Class 5',
                'number' => 5,
            ])])
        );

        $this->assertEquals('Class 5', $student5->getAttribute('classes')[0]->getAttribute('name'));
        $student5 = $this->getDatabase()->getDocument('students', 'student5');
        $this->assertEquals('Class 5', $student5->getAttribute('classes')[0]->getAttribute('name'));

        // Create child document with no relationship
        $class6 = $this->getDatabase()->createDocument('classes', new Document([
            '$id' => 'class6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Class 6',
            'number' => 6,
        ]));

        // Update to relate to created document
        $class6 = $this->getDatabase()->updateDocument(
            'classes',
            $class6->getId(),
            $class6->setAttribute('students', [new Document([
                '$id' => 'student6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Student 6',
            ])])
        );

        $this->assertEquals('Student 6', $class6->getAttribute('students')[0]->getAttribute('name'));
        $class6 = $this->getDatabase()->getDocument('classes', 'class6');
        $this->assertEquals('Student 6', $class6->getAttribute('students')[0]->getAttribute('name'));

        // Update document with new related document
        $this->getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', ['class2'])
        );

        $class1 = $this->getDatabase()->getDocument('classes', 'class1');

        // Update inverse document
        $this->getDatabase()->updateDocument(
            'classes',
            $class1->getId(),
            $class1->setAttribute('students', ['student1'])
        );

        // Rename relationship keys on both sides
        $this->getDatabase()->updateRelationship(
            'students',
            'classes',
            'newClasses',
            'newStudents'
        );

        // Get document with new relationship key
        $students = $this->getDatabase()->getDocument('students', 'student1');
        $classes = $students->getAttribute('newClasses');
        $this->assertEquals('class2', $classes[0]['$id']);

        // Get inverse document with new relationship key
        $class = $this->getDatabase()->getDocument('classes', 'class1');
        $students = $class->getAttribute('newStudents');
        $this->assertEquals('student1', $students[0]['$id']);

        // Create new document with no relationship
        $this->getDatabase()->createDocument('students', new Document([
            '$id' => 'student7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $this->getDatabase()->deleteDocument('students', 'student7');
        $this->assertEquals(true, $deleted);

        $student6 = $this->getDatabase()->getDocument('students', 'student7');
        $this->assertEquals(true, $student6->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $this->getDatabase()->deleteDocument('students', 'student1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $this->getDatabase()->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $student1 = $this->getDatabase()->getDocument('students', 'student1');

        // Reset relationships
        $this->getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('newClasses', ['class1'])
        );

        // Delete child, will delete junction
        $this->getDatabase()->deleteDocument('classes', 'class1');

        // Check relation was set to null
        $student1 = $this->getDatabase()->getDocument('students', 'student1');
        $this->assertEquals(0, \count($student1->getAttribute('newClasses')));

        // Change on delete to cascade
        $this->getDatabase()->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $this->getDatabase()->deleteDocument('students', 'student2');

        // Check parent and child were deleted
        $library = $this->getDatabase()->getDocument('students', 'student2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete child, should not delete parent
        $this->getDatabase()->deleteDocument('classes', 'class6');

        // Check only child was deleted
        $student6 = $this->getDatabase()->getDocument('students', 'student6');
        $this->assertEquals(false, $student6->isEmpty());
        $this->assertEmpty($student6->getAttribute('newClasses'));

        $library = $this->getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        $this->getDatabase()->deleteRelationship(
            'students',
            'newClasses'
        );

        // Try to get documents again
        $student = $this->getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('newClasses');
        $this->assertEquals(null, $classes);

        // Try to get inverse documents again
        $classes = $this->getDatabase()->getDocument('classes', 'class1');
        $students = $classes->getAttribute('newStudents');
        $this->assertEquals(null, $students);
    }

    public function testSelectRelationshipAttributes(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('make');
        $this->getDatabase()->createCollection('model');

        $this->getDatabase()->createAttribute('make', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('model', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('model', 'year', Database::VAR_INTEGER, 0, true);

        $this->getDatabase()->createRelationship(
            collection: 'make',
            relatedCollection: 'model',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'models'
        );

        $this->getDatabase()->createDocument('make', new Document([
            '$id' => 'ford',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Ford',
            'models' => [
                [
                    '$id' => 'fiesta',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Fiesta',
                    'year' => 2010,
                ],
                [
                    '$id' => 'focus',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Focus',
                    'year' => 2011,
                ],
            ],
        ]));

        // Select some parent attributes, some child attributes
        $make = $this->getDatabase()->findOne('make', [
            Query::select(['name', 'models.name']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));
        $this->assertEquals('Fiesta', $make['models'][0]['name']);
        $this->assertEquals('Focus', $make['models'][1]['name']);
        $this->assertArrayNotHasKey('year', $make['models'][0]);
        $this->assertArrayNotHasKey('year', $make['models'][1]);
        $this->assertArrayNotHasKey('$id', $make);
        $this->assertArrayNotHasKey('$internalId', $make);
        $this->assertArrayNotHasKey('$permissions', $make);
        $this->assertArrayNotHasKey('$collection', $make);
        $this->assertArrayNotHasKey('$createdAt', $make);
        $this->assertArrayNotHasKey('$updatedAt', $make);

        // Select internal attributes
        $make = $this->getDatabase()->findOne('make', [
            Query::select(['name', '$id']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayHasKey('$id', $make);
        $this->assertArrayNotHasKey('$internalId', $make);
        $this->assertArrayNotHasKey('$collection', $make);
        $this->assertArrayNotHasKey('$createdAt', $make);
        $this->assertArrayNotHasKey('$updatedAt', $make);
        $this->assertArrayNotHasKey('$permissions', $make);

        $make = $this->getDatabase()->findOne('make', [
            Query::select(['name', '$internalId']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayNotHasKey('$id', $make);
        $this->assertArrayHasKey('$internalId', $make);
        $this->assertArrayNotHasKey('$collection', $make);
        $this->assertArrayNotHasKey('$createdAt', $make);
        $this->assertArrayNotHasKey('$updatedAt', $make);
        $this->assertArrayNotHasKey('$permissions', $make);

        $make = $this->getDatabase()->findOne('make', [
            Query::select(['name', '$collection']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayNotHasKey('$id', $make);
        $this->assertArrayNotHasKey('$internalId', $make);
        $this->assertArrayHasKey('$collection', $make);
        $this->assertArrayNotHasKey('$createdAt', $make);
        $this->assertArrayNotHasKey('$updatedAt', $make);
        $this->assertArrayNotHasKey('$permissions', $make);

        $make = $this->getDatabase()->findOne('make', [
            Query::select(['name', '$createdAt']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayNotHasKey('$id', $make);
        $this->assertArrayNotHasKey('$internalId', $make);
        $this->assertArrayNotHasKey('$collection', $make);
        $this->assertArrayHasKey('$createdAt', $make);
        $this->assertArrayNotHasKey('$updatedAt', $make);
        $this->assertArrayNotHasKey('$permissions', $make);

        $make = $this->getDatabase()->findOne('make', [
            Query::select(['name', '$updatedAt']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayNotHasKey('$id', $make);
        $this->assertArrayNotHasKey('$internalId', $make);
        $this->assertArrayNotHasKey('$collection', $make);
        $this->assertArrayNotHasKey('$createdAt', $make);
        $this->assertArrayHasKey('$updatedAt', $make);
        $this->assertArrayNotHasKey('$permissions', $make);

        $make = $this->getDatabase()->findOne('make', [
            Query::select(['name', '$permissions']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayNotHasKey('$id', $make);
        $this->assertArrayNotHasKey('$internalId', $make);
        $this->assertArrayNotHasKey('$collection', $make);
        $this->assertArrayNotHasKey('$createdAt', $make);
        $this->assertArrayNotHasKey('$updatedAt', $make);
        $this->assertArrayHasKey('$permissions', $make);

        // Select all parent attributes, some child attributes
        $make = $this->getDatabase()->findOne('make', [
            Query::select(['*', 'models.year']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));
        $this->assertArrayNotHasKey('name', $make['models'][0]);
        $this->assertArrayNotHasKey('name', $make['models'][1]);
        $this->assertEquals(2010, $make['models'][0]['year']);
        $this->assertEquals(2011, $make['models'][1]['year']);

        // Select all parent attributes, all child attributes
        $make = $this->getDatabase()->findOne('make', [
            Query::select(['*', 'models.*']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));
        $this->assertEquals('Fiesta', $make['models'][0]['name']);
        $this->assertEquals('Focus', $make['models'][1]['name']);
        $this->assertEquals(2010, $make['models'][0]['year']);
        $this->assertEquals(2011, $make['models'][1]['year']);

        // Select all parent attributes, all child attributes
        // Must select parent if selecting children
        $make = $this->getDatabase()->findOne('make', [
            Query::select(['models.*']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));
        $this->assertEquals('Fiesta', $make['models'][0]['name']);
        $this->assertEquals('Focus', $make['models'][1]['name']);
        $this->assertEquals(2010, $make['models'][0]['year']);
        $this->assertEquals(2011, $make['models'][1]['year']);

        // Select all parent attributes, no child attributes
        $make = $this->getDatabase()->findOne('make', [
            Query::select(['name']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertArrayNotHasKey('models', $make);
    }

    public function testNestedOneToOne_OneToOneRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('pattern');
        $this->getDatabase()->createCollection('shirt');
        $this->getDatabase()->createCollection('team');

        $this->getDatabase()->createAttribute('pattern', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('shirt', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('team', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'pattern',
            relatedCollection: 'shirt',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'shirt',
            twoWayKey: 'pattern'
        );
        $this->getDatabase()->createRelationship(
            collection: 'shirt',
            relatedCollection: 'team',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'team',
            twoWayKey: 'shirt'
        );

        $this->getDatabase()->createDocument('pattern', new Document([
            '$id' => 'stripes',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Stripes',
            'shirt' => [
                '$id' => 'red',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Red',
                'team' => [
                    '$id' => 'reds',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Reds',
                ],
            ],
        ]));

        $pattern = $this->getDatabase()->getDocument('pattern', 'stripes');
        $this->assertEquals('red', $pattern['shirt']['$id']);
        $this->assertArrayNotHasKey('pattern', $pattern['shirt']);
        $this->assertEquals('reds', $pattern['shirt']['team']['$id']);
        $this->assertArrayNotHasKey('shirt', $pattern['shirt']['team']);

        $this->getDatabase()->createDocument('team', new Document([
            '$id' => 'blues',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Blues',
            'shirt' => [
                '$id' => 'blue',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Blue',
                'pattern' => [
                    '$id' => 'plain',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Plain',
                ],
            ],
        ]));

        $team = $this->getDatabase()->getDocument('team', 'blues');
        $this->assertEquals('blue', $team['shirt']['$id']);
        $this->assertArrayNotHasKey('team', $team['shirt']);
        $this->assertEquals('plain', $team['shirt']['pattern']['$id']);
        $this->assertArrayNotHasKey('shirt', $team['shirt']['pattern']);
    }

    public function testNestedOneToOne_OneToManyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('teachers');
        $this->getDatabase()->createCollection('classrooms');
        $this->getDatabase()->createCollection('children');

        $this->getDatabase()->createAttribute('children', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('teachers', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('classrooms', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'teachers',
            relatedCollection: 'classrooms',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'classroom',
            twoWayKey: 'teacher'
        );
        $this->getDatabase()->createRelationship(
            collection: 'classrooms',
            relatedCollection: 'children',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'classroom'
        );

        $this->getDatabase()->createDocument('teachers', new Document([
            '$id' => 'teacher1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Teacher 1',
            'classroom' => [
                '$id' => 'classroom1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Classroom 1',
                'children' => [
                    [
                        '$id' => 'child1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Child 1',
                    ],
                    [
                        '$id' => 'child2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Child 2',
                    ],
                ],
            ],
        ]));

        $teacher1 = $this->getDatabase()->getDocument('teachers', 'teacher1');
        $this->assertEquals('classroom1', $teacher1['classroom']['$id']);
        $this->assertArrayNotHasKey('teacher', $teacher1['classroom']);
        $this->assertEquals(2, \count($teacher1['classroom']['children']));
        $this->assertEquals('Child 1', $teacher1['classroom']['children'][0]['name']);
        $this->assertEquals('Child 2', $teacher1['classroom']['children'][1]['name']);

        $this->getDatabase()->createDocument('children', new Document([
            '$id' => 'child3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Child 3',
            'classroom' => [
                '$id' => 'classroom2',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Classroom 2',
                'teacher' => [
                    '$id' => 'teacher2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Teacher 2',
                ],
            ],
        ]));

        $child3 = $this->getDatabase()->getDocument('children', 'child3');
        $this->assertEquals('classroom2', $child3['classroom']['$id']);
        $this->assertArrayNotHasKey('children', $child3['classroom']);
        $this->assertEquals('teacher2', $child3['classroom']['teacher']['$id']);
        $this->assertArrayNotHasKey('classroom', $child3['classroom']['teacher']);
    }

    public function testNestedOneToOne_ManyToOneRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('users');
        $this->getDatabase()->createCollection('profiles');
        $this->getDatabase()->createCollection('avatars');

        $this->getDatabase()->createAttribute('users', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('profiles', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('avatars', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'users',
            relatedCollection: 'profiles',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'profile',
            twoWayKey: 'user'
        );
        $this->getDatabase()->createRelationship(
            collection: 'profiles',
            relatedCollection: 'avatars',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'avatar',
        );

        $this->getDatabase()->createDocument('users', new Document([
            '$id' => 'user1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'User 1',
            'profile' => [
                '$id' => 'profile1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Profile 1',
                'avatar' => [
                    '$id' => 'avatar1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Avatar 1',
                ],
            ],
        ]));

        $user1 = $this->getDatabase()->getDocument('users', 'user1');
        $this->assertEquals('profile1', $user1['profile']['$id']);
        $this->assertArrayNotHasKey('user', $user1['profile']);
        $this->assertEquals('avatar1', $user1['profile']['avatar']['$id']);
        $this->assertArrayNotHasKey('profile', $user1['profile']['avatar']);

        $this->getDatabase()->createDocument('avatars', new Document([
            '$id' => 'avatar2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Avatar 2',
            'profiles' => [
                [
                    '$id' => 'profile2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Profile 2',
                    'user' => [
                        '$id' => 'user2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'User 2',
                    ],
                ]
            ],
        ]));

        $avatar2 = $this->getDatabase()->getDocument('avatars', 'avatar2');
        $this->assertEquals('profile2', $avatar2['profiles'][0]['$id']);
        $this->assertArrayNotHasKey('avatars', $avatar2['profiles'][0]);
        $this->assertEquals('user2', $avatar2['profiles'][0]['user']['$id']);
        $this->assertArrayNotHasKey('profiles', $avatar2['profiles'][0]['user']);
    }

    public function testNestedOneToOne_ManyToManyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('addresses');
        $this->getDatabase()->createCollection('houses');
        $this->getDatabase()->createCollection('buildings');

        $this->getDatabase()->createAttribute('addresses', 'street', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('houses', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('buildings', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'addresses',
            relatedCollection: 'houses',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'house',
            twoWayKey: 'address'
        );
        $this->getDatabase()->createRelationship(
            collection: 'houses',
            relatedCollection: 'buildings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $this->getDatabase()->createDocument('addresses', new Document([
            '$id' => 'address1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'street' => 'Street 1',
            'house' => [
                '$id' => 'house1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'House 1',
                'buildings' => [
                    [
                        '$id' => 'building1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Building 1',
                    ],
                    [
                        '$id' => 'building2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Building 2',
                    ],
                ],
            ],
        ]));

        $address1 = $this->getDatabase()->getDocument('addresses', 'address1');
        $this->assertEquals('house1', $address1['house']['$id']);
        $this->assertArrayNotHasKey('address', $address1['house']);
        $this->assertEquals('building1', $address1['house']['buildings'][0]['$id']);
        $this->assertEquals('building2', $address1['house']['buildings'][1]['$id']);
        $this->assertArrayNotHasKey('houses', $address1['house']['buildings'][0]);
        $this->assertArrayNotHasKey('houses', $address1['house']['buildings'][1]);

        $this->getDatabase()->createDocument('buildings', new Document([
            '$id' => 'building3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Building 3',
            'houses' => [
                [
                    '$id' => 'house2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                    'address' => [
                        '$id' => 'address2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'street' => 'Street 2',
                    ],
                ],
            ],
        ]));
    }

    public function testNestedOneToMany_OneToOneRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('countries');
        $this->getDatabase()->createCollection('cities');
        $this->getDatabase()->createCollection('mayors');

        $this->getDatabase()->createAttribute('cities', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('countries', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('mayors', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'countries',
            relatedCollection: 'cities',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'country'
        );
        $this->getDatabase()->createRelationship(
            collection: 'cities',
            relatedCollection: 'mayors',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'mayor',
            twoWayKey: 'city'
        );

        $this->getDatabase()->createDocument('countries', new Document([
            '$id' => 'country1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Country 1',
            'cities' => [
                [
                    '$id' => 'city1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'City 1',
                    'mayor' => [
                        '$id' => 'mayor1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                            Permission::update(Role::any()),
                        ],
                        'name' => 'Mayor 1',
                    ],
                ],
                [
                    '$id' => 'city2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'City 2',
                    'mayor' => [
                        '$id' => 'mayor2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Mayor 2',
                    ],
                ],
            ],
        ]));

        $documents = $this->getDatabase()->find('countries', [
            Query::limit(1)
        ]);
        $this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        $documents = $this->getDatabase()->find('countries', [
            Query::select(['name']),
            Query::limit(1)
        ]);
        $this->assertArrayHasKey('name', $documents[0]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = $this->getDatabase()->find('countries', [
            Query::select(['*']),
            Query::limit(1)
        ]);
        $this->assertArrayHasKey('name', $documents[0]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = $this->getDatabase()->find('countries', [
            Query::select(['*', 'cities.*', 'cities.mayor.*']),
            Query::limit(1)
        ]);

        $this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        // Insert docs to cache:
        $country1 = $this->getDatabase()->getDocument('countries', 'country1');
        $mayor1 = $this->getDatabase()->getDocument('mayors', 'mayor1');
        $this->assertEquals('City 1', $mayor1['city']['name']);
        $this->assertEquals('City 1', $country1['cities'][0]['name']);

        $this->getDatabase()->updateDocument('cities', 'city1', new Document([
            '$id' => 'city1',
            '$collection' => 'cities',
            'name' => 'City 1 updated',
            'mayor' => 'mayor1', // we don't support partial updates at the moment
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        $mayor1 = $this->getDatabase()->getDocument('mayors', 'mayor1');
        $country1 = $this->getDatabase()->getDocument('countries', 'country1');

        $this->assertEquals('City 1 updated', $mayor1['city']['name']);
        $this->assertEquals('City 1 updated', $country1['cities'][0]['name']);
        $this->assertEquals('city1', $country1['cities'][0]['$id']);
        $this->assertEquals('city2', $country1['cities'][1]['$id']);
        $this->assertEquals('mayor1', $country1['cities'][0]['mayor']['$id']);
        $this->assertEquals('mayor2', $country1['cities'][1]['mayor']['$id']);
        $this->assertArrayNotHasKey('city', $country1['cities'][0]['mayor']);
        $this->assertArrayNotHasKey('city', $country1['cities'][1]['mayor']);

        $this->getDatabase()->createDocument('mayors', new Document([
            '$id' => 'mayor3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Mayor 3',
            'city' => [
                '$id' => 'city3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'City 3',
                'country' => [
                    '$id' => 'country2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Country 2',
                ],
            ],
        ]));

        $country2 = $this->getDatabase()->getDocument('countries', 'country2');
        $this->assertEquals('city3', $country2['cities'][0]['$id']);
        $this->assertEquals('mayor3', $country2['cities'][0]['mayor']['$id']);
        $this->assertArrayNotHasKey('country', $country2['cities'][0]);
        $this->assertArrayNotHasKey('city', $country2['cities'][0]['mayor']);
    }

    public function testNestedOneToMany_OneToManyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('dormitories');
        $this->getDatabase()->createCollection('occupants');
        $this->getDatabase()->createCollection('pets');

        $this->getDatabase()->createAttribute('dormitories', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('occupants', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('pets', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'dormitories',
            relatedCollection: 'occupants',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'dormitory'
        );
        $this->getDatabase()->createRelationship(
            collection: 'occupants',
            relatedCollection: 'pets',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'occupant'
        );

        $this->getDatabase()->createDocument('dormitories', new Document([
            '$id' => 'dormitory1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'House 1',
            'occupants' => [
                [
                    '$id' => 'occupant1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 1',
                    'pets' => [
                        [
                            '$id' => 'pet1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 1',
                        ],
                        [
                            '$id' => 'pet2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 2',
                        ],
                    ],
                ],
                [
                    '$id' => 'occupant2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 2',
                    'pets' => [
                        [
                            '$id' => 'pet3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 3',
                        ],
                        [
                            '$id' => 'pet4',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 4',
                        ],
                    ],
                ],
            ],
        ]));

        $dormitory1 = $this->getDatabase()->getDocument('dormitories', 'dormitory1');
        $this->assertEquals('occupant1', $dormitory1['occupants'][0]['$id']);
        $this->assertEquals('occupant2', $dormitory1['occupants'][1]['$id']);
        $this->assertEquals('pet1', $dormitory1['occupants'][0]['pets'][0]['$id']);
        $this->assertEquals('pet2', $dormitory1['occupants'][0]['pets'][1]['$id']);
        $this->assertEquals('pet3', $dormitory1['occupants'][1]['pets'][0]['$id']);
        $this->assertEquals('pet4', $dormitory1['occupants'][1]['pets'][1]['$id']);
        $this->assertArrayNotHasKey('dormitory', $dormitory1['occupants'][0]);
        $this->assertArrayNotHasKey('dormitory', $dormitory1['occupants'][1]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][0]['pets'][0]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][0]['pets'][1]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][1]['pets'][0]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][1]['pets'][1]);

        $this->getDatabase()->createDocument('pets', new Document([
            '$id' => 'pet5',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Pet 5',
            'occupant' => [
                '$id' => 'occupant3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Occupant 3',
                'dormitory' => [
                    '$id' => 'dormitory2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                ],
            ],
        ]));

        $pet5 = $this->getDatabase()->getDocument('pets', 'pet5');
        $this->assertEquals('occupant3', $pet5['occupant']['$id']);
        $this->assertEquals('dormitory2', $pet5['occupant']['dormitory']['$id']);
        $this->assertArrayNotHasKey('pets', $pet5['occupant']);
        $this->assertArrayNotHasKey('occupant', $pet5['occupant']['dormitory']);
    }

    public function testNestedOneToMany_ManyToOneRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('home');
        $this->getDatabase()->createCollection('renters');
        $this->getDatabase()->createCollection('floors');

        $this->getDatabase()->createAttribute('home', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('renters', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('floors', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'home',
            relatedCollection: 'renters',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );
        $this->getDatabase()->createRelationship(
            collection: 'renters',
            relatedCollection: 'floors',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'floor'
        );

        $this->getDatabase()->createDocument('home', new Document([
            '$id' => 'home1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'House 1',
            'renters' => [
                [
                    '$id' => 'renter1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 1',
                    'floor' => [
                        '$id' => 'floor1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Floor 1',
                    ],
                ],
            ],
        ]));

        $home1 = $this->getDatabase()->getDocument('home', 'home1');
        $this->assertEquals('renter1', $home1['renters'][0]['$id']);
        $this->assertEquals('floor1', $home1['renters'][0]['floor']['$id']);
        $this->assertArrayNotHasKey('home', $home1['renters'][0]);
        $this->assertArrayNotHasKey('renters', $home1['renters'][0]['floor']);

        $this->getDatabase()->createDocument('floors', new Document([
            '$id' => 'floor2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Floor 2',
            'renters' => [
                [
                    '$id' => 'renter2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 2',
                    'home' => [
                        '$id' => 'home2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'House 2',
                    ],
                ],
            ],
        ]));

        $floor2 = $this->getDatabase()->getDocument('floors', 'floor2');
        $this->assertEquals('renter2', $floor2['renters'][0]['$id']);
        $this->assertArrayNotHasKey('floor', $floor2['renters'][0]);
        $this->assertEquals('home2', $floor2['renters'][0]['home']['$id']);
        $this->assertArrayNotHasKey('renter', $floor2['renters'][0]['home']);
    }

    public function testNestedOneToMany_ManyToManyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('owners');
        $this->getDatabase()->createCollection('cats');
        $this->getDatabase()->createCollection('toys');

        $this->getDatabase()->createAttribute('owners', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('cats', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('toys', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'owners',
            relatedCollection: 'cats',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'owner'
        );
        $this->getDatabase()->createRelationship(
            collection: 'cats',
            relatedCollection: 'toys',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true
        );

        $this->getDatabase()->createDocument('owners', new Document([
            '$id' => 'owner1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Owner 1',
            'cats' => [
                [
                    '$id' => 'cat1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pet 1',
                    'toys' => [
                        [
                            '$id' => 'toy1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Toy 1',
                        ],
                    ],
                ],
            ],
        ]));

        $owner1 = $this->getDatabase()->getDocument('owners', 'owner1');
        $this->assertEquals('cat1', $owner1['cats'][0]['$id']);
        $this->assertArrayNotHasKey('owner', $owner1['cats'][0]);
        $this->assertEquals('toy1', $owner1['cats'][0]['toys'][0]['$id']);
        $this->assertArrayNotHasKey('cats', $owner1['cats'][0]['toys'][0]);

        $this->getDatabase()->createDocument('toys', new Document([
            '$id' => 'toy2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Toy 2',
            'cats' => [
                [
                    '$id' => 'cat2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pet 2',
                    'owner' => [
                        '$id' => 'owner2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Owner 2',
                    ],
                ],
            ],
        ]));

        $toy2 = $this->getDatabase()->getDocument('toys', 'toy2');
        $this->assertEquals('cat2', $toy2['cats'][0]['$id']);
        $this->assertArrayNotHasKey('toys', $toy2['cats'][0]);
        $this->assertEquals('owner2', $toy2['cats'][0]['owner']['$id']);
        $this->assertArrayNotHasKey('cats', $toy2['cats'][0]['owner']);
    }

    public function testNestedManyToOne_OneToOneRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('towns');
        $this->getDatabase()->createCollection('homelands');
        $this->getDatabase()->createCollection('capitals');

        $this->getDatabase()->createAttribute('towns', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('homelands', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('capitals', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'towns',
            relatedCollection: 'homelands',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'homeland'
        );
        $this->getDatabase()->createRelationship(
            collection: 'homelands',
            relatedCollection: 'capitals',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'capital',
            twoWayKey: 'homeland'
        );

        $this->getDatabase()->createDocument('towns', new Document([
            '$id' => 'town1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'City 1',
            'homeland' => [
                '$id' => 'homeland1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Country 1',
                'capital' => [
                    '$id' => 'capital1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Flag 1',
                ],
            ],
        ]));

        $town1 = $this->getDatabase()->getDocument('towns', 'town1');
        $this->assertEquals('homeland1', $town1['homeland']['$id']);
        $this->assertArrayNotHasKey('towns', $town1['homeland']);
        $this->assertEquals('capital1', $town1['homeland']['capital']['$id']);
        $this->assertArrayNotHasKey('homeland', $town1['homeland']['capital']);

        $this->getDatabase()->createDocument('capitals', new Document([
            '$id' => 'capital2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Flag 2',
            'homeland' => [
                '$id' => 'homeland2',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Country 2',
                'towns' => [
                    [
                        '$id' => 'town2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Town 2',
                    ],
                    [
                        '$id' => 'town3',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Town 3',
                    ],
                ],
            ],
        ]));

        $capital2 = $this->getDatabase()->getDocument('capitals', 'capital2');
        $this->assertEquals('homeland2', $capital2['homeland']['$id']);
        $this->assertArrayNotHasKey('capital', $capital2['homeland']);
        $this->assertEquals(2, \count($capital2['homeland']['towns']));
        $this->assertEquals('town2', $capital2['homeland']['towns'][0]['$id']);
        $this->assertEquals('town3', $capital2['homeland']['towns'][1]['$id']);
    }

    public function testNestedManyToOne_OneToManyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('players');
        $this->getDatabase()->createCollection('teams');
        $this->getDatabase()->createCollection('supporters');

        $this->getDatabase()->createAttribute('players', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('teams', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('supporters', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'players',
            relatedCollection: 'teams',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'team'
        );
        $this->getDatabase()->createRelationship(
            collection: 'teams',
            relatedCollection: 'supporters',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'supporters',
            twoWayKey: 'team'
        );

        $this->getDatabase()->createDocument('players', new Document([
            '$id' => 'player1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Player 1',
            'team' => [
                '$id' => 'team1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Team 1',
                'supporters' => [
                    [
                        '$id' => 'supporter1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Supporter 1',
                    ],
                    [
                        '$id' => 'supporter2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Supporter 2',
                    ],
                ],
            ],
        ]));

        $player1 = $this->getDatabase()->getDocument('players', 'player1');
        $this->assertEquals('team1', $player1['team']['$id']);
        $this->assertArrayNotHasKey('players', $player1['team']);
        $this->assertEquals(2, \count($player1['team']['supporters']));
        $this->assertEquals('supporter1', $player1['team']['supporters'][0]['$id']);
        $this->assertEquals('supporter2', $player1['team']['supporters'][1]['$id']);

        $this->getDatabase()->createDocument('supporters', new Document([
            '$id' => 'supporter3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Supporter 3',
            'team' => [
                '$id' => 'team2',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Team 2',
                'players' => [
                    [
                        '$id' => 'player2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Player 2',
                    ],
                    [
                        '$id' => 'player3',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Player 3',
                    ],
                ],
            ],
        ]));

        $supporter3 = $this->getDatabase()->getDocument('supporters', 'supporter3');
        $this->assertEquals('team2', $supporter3['team']['$id']);
        $this->assertArrayNotHasKey('supporters', $supporter3['team']);
        $this->assertEquals(2, \count($supporter3['team']['players']));
        $this->assertEquals('player2', $supporter3['team']['players'][0]['$id']);
        $this->assertEquals('player3', $supporter3['team']['players'][1]['$id']);
    }

    public function testNestedManyToOne_ManyToOne(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('cows');
        $this->getDatabase()->createCollection('farms');
        $this->getDatabase()->createCollection('farmer');

        $this->getDatabase()->createAttribute('cows', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('farms', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('farmer', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'cows',
            relatedCollection: 'farms',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'farm'
        );
        $this->getDatabase()->createRelationship(
            collection: 'farms',
            relatedCollection: 'farmer',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'farmer'
        );

        $this->getDatabase()->createDocument('cows', new Document([
            '$id' => 'cow1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Cow 1',
            'farm' => [
                '$id' => 'farm1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Farm 1',
                'farmer' => [
                    '$id' => 'farmer1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Farmer 1',
                ],
            ],
        ]));

        $cow1 = $this->getDatabase()->getDocument('cows', 'cow1');
        $this->assertEquals('farm1', $cow1['farm']['$id']);
        $this->assertArrayNotHasKey('cows', $cow1['farm']);
        $this->assertEquals('farmer1', $cow1['farm']['farmer']['$id']);
        $this->assertArrayNotHasKey('farms', $cow1['farm']['farmer']);

        $this->getDatabase()->createDocument('farmer', new Document([
            '$id' => 'farmer2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Farmer 2',
            'farms' => [
                [
                    '$id' => 'farm2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Farm 2',
                    'cows' => [
                        [
                            '$id' => 'cow2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Cow 2',
                        ],
                        [
                            '$id' => 'cow3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Cow 3',
                        ],
                    ],
                ],
            ],
        ]));

        $farmer2 = $this->getDatabase()->getDocument('farmer', 'farmer2');
        $this->assertEquals('farm2', $farmer2['farms'][0]['$id']);
        $this->assertArrayNotHasKey('farmer', $farmer2['farms'][0]);
        $this->assertEquals(2, \count($farmer2['farms'][0]['cows']));
        $this->assertEquals('cow2', $farmer2['farms'][0]['cows'][0]['$id']);
        $this->assertEquals('cow3', $farmer2['farms'][0]['cows'][1]['$id']);
    }

    public function testNestedManyToOne_ManyToManyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('books');
        $this->getDatabase()->createCollection('entrants');
        $this->getDatabase()->createCollection('rooms');

        $this->getDatabase()->createAttribute('books', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('entrants', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('rooms', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'books',
            relatedCollection: 'entrants',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'entrant'
        );
        $this->getDatabase()->createRelationship(
            collection: 'entrants',
            relatedCollection: 'rooms',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $this->getDatabase()->createDocument('books', new Document([
            '$id' => 'book1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Book 1',
            'entrant' => [
                '$id' => 'entrant1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Entrant 1',
                'rooms' => [
                    [
                        '$id' => 'class1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Class 1',
                    ],
                    [
                        '$id' => 'class2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Class 2',
                    ],
                ],
            ],
        ]));

        $book1 = $this->getDatabase()->getDocument('books', 'book1');
        $this->assertEquals('entrant1', $book1['entrant']['$id']);
        $this->assertArrayNotHasKey('books', $book1['entrant']);
        $this->assertEquals(2, \count($book1['entrant']['rooms']));
        $this->assertEquals('class1', $book1['entrant']['rooms'][0]['$id']);
        $this->assertEquals('class2', $book1['entrant']['rooms'][1]['$id']);
    }

    public function testNestedManyToMany_OneToOneRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('stones');
        $this->getDatabase()->createCollection('hearths');
        $this->getDatabase()->createCollection('plots');

        $this->getDatabase()->createAttribute('stones', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('hearths', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('plots', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'stones',
            relatedCollection: 'hearths',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: 'hearths',
            relatedCollection: 'plots',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'plot',
            twoWayKey: 'hearth'
        );

        $this->getDatabase()->createDocument('stones', new Document([
            '$id' => 'stone1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Building 1',
            'hearths' => [
                [
                    '$id' => 'hearth1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 1',
                    'plot' => [
                        '$id' => 'plot1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Address 1',
                    ],
                ],
                [
                    '$id' => 'hearth2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                    'plot' => [
                        '$id' => 'plot2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Address 2',
                    ],
                ],
            ],
        ]));

        $stone1 = $this->getDatabase()->getDocument('stones', 'stone1');
        $this->assertEquals(2, \count($stone1['hearths']));
        $this->assertEquals('hearth1', $stone1['hearths'][0]['$id']);
        $this->assertEquals('hearth2', $stone1['hearths'][1]['$id']);
        $this->assertArrayNotHasKey('stone', $stone1['hearths'][0]);
        $this->assertEquals('plot1', $stone1['hearths'][0]['plot']['$id']);
        $this->assertEquals('plot2', $stone1['hearths'][1]['plot']['$id']);
        $this->assertArrayNotHasKey('hearth', $stone1['hearths'][0]['plot']);

        $this->getDatabase()->createDocument('plots', new Document([
            '$id' => 'plot3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Address 3',
            'hearth' => [
                '$id' => 'hearth3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Hearth 3',
                'stones' => [
                    [
                        '$id' => 'stone2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Stone 2',
                    ],
                ],
            ],
        ]));

        $plot3 = $this->getDatabase()->getDocument('plots', 'plot3');
        $this->assertEquals('hearth3', $plot3['hearth']['$id']);
        $this->assertArrayNotHasKey('plot', $plot3['hearth']);
        $this->assertEquals('stone2', $plot3['hearth']['stones'][0]['$id']);
        $this->assertArrayNotHasKey('hearths', $plot3['hearth']['stones'][0]);
    }

    public function testNestedManyToMany_OneToManyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('groups');
        $this->getDatabase()->createCollection('tounaments');
        $this->getDatabase()->createCollection('prizes');

        $this->getDatabase()->createAttribute('groups', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('tounaments', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('prizes', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'groups',
            relatedCollection: 'tounaments',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: 'tounaments',
            relatedCollection: 'prizes',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'prizes',
            twoWayKey: 'tounament'
        );

        static::getDatabase()->createDocument('groups', new Document([
            '$id' => 'group1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Group 1',
            'tounaments' => [
                [
                    '$id' => 'tounament1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Tounament 1',
                    'prizes' => [
                        [
                            '$id' => 'prize1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Prize 1',
                        ],
                        [
                            '$id' => 'prize2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Prize 2',
                        ],
                    ],
                ],
                [
                    '$id' => 'tounament2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Tounament 2',
                    'prizes' => [
                        [
                            '$id' => 'prize3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Prize 3',
                        ],
                        [
                            '$id' => 'prize4',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Prize 4',
                        ],
                    ],
                ],
            ],
        ]));

        $group1 = $this->getDatabase()->getDocument('groups', 'group1');
        $this->assertEquals(2, \count($group1['tounaments']));
        $this->assertEquals('tounament1', $group1['tounaments'][0]['$id']);
        $this->assertEquals('tounament2', $group1['tounaments'][1]['$id']);
        $this->assertArrayNotHasKey('group', $group1['tounaments'][0]);
        $this->assertEquals(2, \count($group1['tounaments'][0]['prizes']));
        $this->assertEquals('prize1', $group1['tounaments'][0]['prizes'][0]['$id']);
        $this->assertEquals('prize2', $group1['tounaments'][0]['prizes'][1]['$id']);
        $this->assertArrayNotHasKey('tounament', $group1['tounaments'][0]['prizes'][0]);
    }

    public function testNestedManyToMany_ManyToOneRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('platforms');
        $this->getDatabase()->createCollection('games');
        $this->getDatabase()->createCollection('publishers');

        $this->getDatabase()->createAttribute('platforms', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('games', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('publishers', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'platforms',
            relatedCollection: 'games',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: 'games',
            relatedCollection: 'publishers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'publisher',
            twoWayKey: 'games'
        );

        $this->getDatabase()->createDocument('platforms', new Document([
            '$id' => 'platform1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Platform 1',
            'games' => [
                [
                    '$id' => 'game1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 1',
                    'publisher' => [
                        '$id' => 'publisher1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Publisher 1',
                    ],
                ],
                [
                    '$id' => 'game2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 2',
                    'publisher' => [
                        '$id' => 'publisher2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Publisher 2',
                    ],
                ],
            ]
        ]));

        $platform1 = $this->getDatabase()->getDocument('platforms', 'platform1');
        $this->assertEquals(2, \count($platform1['games']));
        $this->assertEquals('game1', $platform1['games'][0]['$id']);
        $this->assertEquals('game2', $platform1['games'][1]['$id']);
        $this->assertArrayNotHasKey('platforms', $platform1['games'][0]);
        $this->assertEquals('publisher1', $platform1['games'][0]['publisher']['$id']);
        $this->assertEquals('publisher2', $platform1['games'][1]['publisher']['$id']);
        $this->assertArrayNotHasKey('games', $platform1['games'][0]['publisher']);

        $this->getDatabase()->createDocument('publishers', new Document([
            '$id' => 'publisher3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Publisher 3',
            'games' => [
                [
                    '$id' => 'game3',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 3',
                    'platforms' => [
                        [
                            '$id' => 'platform2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Platform 2',
                        ]
                    ],
                ],
            ],
        ]));

        $publisher3 = $this->getDatabase()->getDocument('publishers', 'publisher3');
        $this->assertEquals(1, \count($publisher3['games']));
        $this->assertEquals('game3', $publisher3['games'][0]['$id']);
        $this->assertArrayNotHasKey('publisher', $publisher3['games'][0]);
        $this->assertEquals('platform2', $publisher3['games'][0]['platforms'][0]['$id']);
        $this->assertArrayNotHasKey('games', $publisher3['games'][0]['platforms'][0]);
    }

    public function testNestedManyToMany_ManyToManyRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('sauces');
        $this->getDatabase()->createCollection('pizzas');
        $this->getDatabase()->createCollection('toppings');

        $this->getDatabase()->createAttribute('sauces', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('pizzas', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('toppings', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'sauces',
            relatedCollection: 'pizzas',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: 'pizzas',
            relatedCollection: 'toppings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'toppings',
            twoWayKey: 'pizzas'
        );

        $this->getDatabase()->createDocument('sauces', new Document([
            '$id' => 'sauce1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Sauce 1',
            'pizzas' => [
                [
                    '$id' => 'pizza1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pizza 1',
                    'toppings' => [
                        [
                            '$id' => 'topping1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 1',
                        ],
                        [
                            '$id' => 'topping2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 2',
                        ],
                    ],
                ],
                [
                    '$id' => 'pizza2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pizza 2',
                    'toppings' => [
                        [
                            '$id' => 'topping3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 3',
                        ],
                        [
                            '$id' => 'topping4',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 4',
                        ],
                    ],
                ],
            ]
        ]));

        $sauce1 = $this->getDatabase()->getDocument('sauces', 'sauce1');
        $this->assertEquals(2, \count($sauce1['pizzas']));
        $this->assertEquals('pizza1', $sauce1['pizzas'][0]['$id']);
        $this->assertEquals('pizza2', $sauce1['pizzas'][1]['$id']);
        $this->assertArrayNotHasKey('sauces', $sauce1['pizzas'][0]);
        $this->assertEquals(2, \count($sauce1['pizzas'][0]['toppings']));
        $this->assertEquals('topping1', $sauce1['pizzas'][0]['toppings'][0]['$id']);
        $this->assertEquals('topping2', $sauce1['pizzas'][0]['toppings'][1]['$id']);
        $this->assertArrayNotHasKey('pizzas', $sauce1['pizzas'][0]['toppings'][0]);
        $this->assertEquals(2, \count($sauce1['pizzas'][1]['toppings']));
        $this->assertEquals('topping3', $sauce1['pizzas'][1]['toppings'][0]['$id']);
        $this->assertEquals('topping4', $sauce1['pizzas'][1]['toppings'][1]['$id']);
        $this->assertArrayNotHasKey('pizzas', $sauce1['pizzas'][1]['toppings'][0]);
    }

    public function testInheritRelationshipPermissions(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('lawns', permissions: [Permission::create(Role::any())], documentSecurity: true);
        $this->getDatabase()->createCollection('trees', permissions: [Permission::create(Role::any())], documentSecurity: true);
        $this->getDatabase()->createCollection('birds', permissions: [Permission::create(Role::any())], documentSecurity: true);

        $this->getDatabase()->createAttribute('lawns', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('trees', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('birds', 'name', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'lawns',
            relatedCollection: 'trees',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'lawn',
            onDelete: Database::RELATION_MUTATE_CASCADE,
        );
        $this->getDatabase()->createRelationship(
            collection: 'trees',
            relatedCollection: 'birds',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            onDelete: Database::RELATION_MUTATE_SET_NULL,
        );

        $permissions = [
            Permission::read(Role::any()),
            Permission::read(Role::user('user1')),
            Permission::update(Role::user('user1')),
            Permission::delete(Role::user('user2')),
        ];

        $this->getDatabase()->createDocument('lawns', new Document([
            '$id' => 'lawn1',
            '$permissions' => $permissions,
            'name' => 'Lawn 1',
            'trees' => [
                [
                    '$id' => 'tree1',
                    'name' => 'Tree 1',
                    'birds' => [
                        [
                            '$id' => 'bird1',
                            'name' => 'Bird 1',
                        ],
                        [
                            '$id' => 'bird2',
                            'name' => 'Bird 2',
                        ],
                    ],
                ],
            ],
        ]));

        $lawn1 = $this->getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals($permissions, $lawn1->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]['birds'][0]->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]['birds'][1]->getPermissions());

        $tree1 = $this->getDatabase()->getDocument('trees', 'tree1');
        $this->assertEquals($permissions, $tree1->getPermissions());
        $this->assertEquals($permissions, $tree1['lawn']->getPermissions());
        $this->assertEquals($permissions, $tree1['birds'][0]->getPermissions());
        $this->assertEquals($permissions, $tree1['birds'][1]->getPermissions());
    }

    /**
     * @depends testInheritRelationshipPermissions
     */
    public function testEnforceRelationshipPermissions(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());
        $lawn1 = $this->getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals('Lawn 1', $lawn1['name']);

        // Try update root document
        try {
            $this->getDatabase()->updateDocument(
                'lawns',
                $lawn1->getId(),
                $lawn1->setAttribute('name', 'Lawn 1 Updated')
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "update" permission for role "user:user1". Only "["any"]" scopes are allowed and "["user:user1"]" was given.', $e->getMessage());
        }

        // Try delete root document
        try {
            $this->getDatabase()->deleteDocument(
                'lawns',
                $lawn1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        $tree1 = $this->getDatabase()->getDocument('trees', 'tree1');

        // Try update nested document
        try {
            $this->getDatabase()->updateDocument(
                'trees',
                $tree1->getId(),
                $tree1->setAttribute('name', 'Tree 1 Updated')
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "update" permission for role "user:user1". Only "["any"]" scopes are allowed and "["user:user1"]" was given.', $e->getMessage());
        }

        // Try delete nested document
        try {
            $this->getDatabase()->deleteDocument(
                'trees',
                $tree1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        $bird1 = $this->getDatabase()->getDocument('birds', 'bird1');

        // Try update multi-level nested document
        try {
            $this->getDatabase()->updateDocument(
                'birds',
                $bird1->getId(),
                $bird1->setAttribute('name', 'Bird 1 Updated')
            );
            $this->fail('Failed to throw exception when updating document with missing permissions');
        } catch (Exception $e) {
            $this->assertEquals('Missing "update" permission for role "user:user1". Only "["any"]" scopes are allowed and "["user:user1"]" was given.', $e->getMessage());
        }

        // Try delete multi-level nested document
        try {
            $this->getDatabase()->deleteDocument(
                'birds',
                $bird1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        self::$authorization->addRole(Role::user('user1')->toString());

        $bird1 = $this->getDatabase()->getDocument('birds', 'bird1');

        // Try update multi-level nested document
        $bird1 = $this->getDatabase()->updateDocument(
            'birds',
            $bird1->getId(),
            $bird1->setAttribute('name', 'Bird 1 Updated')
        );

        $this->assertEquals('Bird 1 Updated', $bird1['name']);

        self::$authorization->addRole(Role::user('user2')->toString());

        // Try delete multi-level nested document
        $deleted = $this->getDatabase()->deleteDocument(
            'birds',
            $bird1->getId(),
        );

        $this->assertEquals(true, $deleted);
        $tree1 = $this->getDatabase()->getDocument('trees', 'tree1');
        $this->assertEquals(1, count($tree1['birds']));

        // Try update nested document
        $tree1 = $this->getDatabase()->updateDocument(
            'trees',
            $tree1->getId(),
            $tree1->setAttribute('name', 'Tree 1 Updated')
        );

        $this->assertEquals('Tree 1 Updated', $tree1['name']);

        // Try delete nested document
        $deleted = $this->getDatabase()->deleteDocument(
            'trees',
            $tree1->getId(),
        );

        $this->assertEquals(true, $deleted);
        $lawn1 = $this->getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals(0, count($lawn1['trees']));

        // Create document with no permissions
        $this->getDatabase()->createDocument('lawns', new Document([
            '$id' => 'lawn2',
            'name' => 'Lawn 2',
            'trees' => [
                [
                    '$id' => 'tree2',
                    'name' => 'Tree 2',
                    'birds' => [
                        [
                            '$id' => 'bird3',
                            'name' => 'Bird 3',
                        ],
                    ],
                ],
            ],
        ]));

        $lawn2 = $this->getDatabase()->getDocument('lawns', 'lawn2');
        $this->assertEquals(true, $lawn2->isEmpty());

        $tree2 = $this->getDatabase()->getDocument('trees', 'tree2');
        $this->assertEquals(true, $tree2->isEmpty());

        $bird3 = $this->getDatabase()->getDocument('birds', 'bird3');
        $this->assertEquals(true, $bird3->isEmpty());
    }

    public function testExceedMaxDepthOneToMany(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToMany';
        $level2Collection = 'level2OneToMany';
        $level3Collection = 'level3OneToMany';
        $level4Collection = 'level4OneToMany';

        $this->getDatabase()->createCollection($level1Collection);
        $this->getDatabase()->createCollection($level2Collection);
        $this->getDatabase()->createCollection($level3Collection);
        $this->getDatabase()->createCollection($level4Collection);

        $this->getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        // Exceed create depth
        $level1 = $this->getDatabase()->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                [
                    '$id' => 'level2',
                    $level3Collection => [
                        [
                            '$id' => 'level3',
                            $level4Collection => [
                                [
                                    '$id' => 'level4',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ]));
        $this->assertEquals(1, count($level1[$level2Collection]));
        $this->assertEquals('level2', $level1[$level2Collection][0]->getId());
        $this->assertEquals(1, count($level1[$level2Collection][0][$level3Collection]));
        $this->assertEquals('level3', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey('level4', $level1[$level2Collection][0][$level3Collection][0]);

        // Make sure level 4 document was not created
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals(0, count($level3[$level4Collection]));
        $level4 = $this->getDatabase()->getDocument($level4Collection, 'level4');
        $this->assertTrue($level4->isEmpty());

        // Exceed fetch depth
        $level1 = $this->getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertEquals(1, count($level1[$level2Collection]));
        $this->assertEquals('level2', $level1[$level2Collection][0]->getId());
        $this->assertEquals(1, count($level1[$level2Collection][0][$level3Collection]));
        $this->assertEquals('level3', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][0][$level3Collection][0]);


        // Exceed update depth
        $level1 = $this->getDatabase()->updateDocument(
            $level1Collection,
            'level1',
            $level1
            ->setAttribute($level2Collection, [new Document([
                '$id' => 'level2new',
                $level3Collection => [
                    [
                        '$id' => 'level3new',
                        $level4Collection => [
                            [
                                '$id' => 'level4new',
                            ],
                        ],
                    ],
                ],
            ])])
        );
        $this->assertEquals(1, count($level1[$level2Collection]));
        $this->assertEquals('level2new', $level1[$level2Collection][0]->getId());
        $this->assertEquals(1, count($level1[$level2Collection][0][$level3Collection]));
        $this->assertEquals('level3new', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][0][$level3Collection][0]);

        // Make sure level 4 document was not created
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3new');
        $this->assertEquals(0, count($level3[$level4Collection]));
        $level4 = $this->getDatabase()->getDocument($level4Collection, 'level4new');
        $this->assertTrue($level4->isEmpty());
    }

    public function testExceedMaxDepthOneToOne(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToOne';
        $level2Collection = 'level2OneToOne';
        $level3Collection = 'level3OneToOne';
        $level4Collection = 'level4OneToOne';

        $this->getDatabase()->createCollection($level1Collection);
        $this->getDatabase()->createCollection($level2Collection);
        $this->getDatabase()->createCollection($level3Collection);
        $this->getDatabase()->createCollection($level4Collection);

        $this->getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        // Exceed create depth
        $level1 = $this->getDatabase()->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                '$id' => 'level2',
                $level3Collection => [
                    '$id' => 'level3',
                    $level4Collection => [
                        '$id' => 'level4',
                    ],
                ],
            ],
        ]));
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);

        // Confirm the 4th level document does not exist
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertNull($level3[$level4Collection]);

        // Create level 4 document
        $level3->setAttribute($level4Collection, new Document([
            '$id' => 'level4',
        ]));
        $level3 = $this->getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection]->getId());

        // Exceed fetch depth
        $level1 = $this->getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);
    }

    public function testExceedMaxDepthOneToOneNull(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToOneNull';
        $level2Collection = 'level2OneToOneNull';
        $level3Collection = 'level3OneToOneNull';
        $level4Collection = 'level4OneToOneNull';

        $this->getDatabase()->createCollection($level1Collection);
        $this->getDatabase()->createCollection($level2Collection);
        $this->getDatabase()->createCollection($level3Collection);
        $this->getDatabase()->createCollection($level4Collection);

        $this->getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $level1 = $this->getDatabase()->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                '$id' => 'level2',
                $level3Collection => [
                    '$id' => 'level3',
                    $level4Collection => [
                        '$id' => 'level4',
                    ],
                ],
            ],
        ]));
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);

        // Confirm the 4th level document does not exist
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertNull($level3[$level4Collection]);

        // Create level 4 document
        $level3->setAttribute($level4Collection, new Document([
            '$id' => 'level4',
        ]));
        $level3 = $this->getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection]->getId());
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals('level4', $level3[$level4Collection]->getId());

        // Exceed fetch depth
        $level1 = $this->getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);
    }

    public function testExceedMaxDepthManyToOneParent(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1ManyToOneParent';
        $level2Collection = 'level2ManyToOneParent';
        $level3Collection = 'level3ManyToOneParent';
        $level4Collection = 'level4ManyToOneParent';

        $this->getDatabase()->createCollection($level1Collection);
        $this->getDatabase()->createCollection($level2Collection);
        $this->getDatabase()->createCollection($level3Collection);
        $this->getDatabase()->createCollection($level4Collection);

        $this->getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );

        $level1 = $this->getDatabase()->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                '$id' => 'level2',
                $level3Collection => [
                    '$id' => 'level3',
                    $level4Collection => [
                        '$id' => 'level4',
                    ],
                ],
            ],
        ]));
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);

        // Confirm the 4th level document does not exist
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertNull($level3[$level4Collection]);

        // Create level 4 document
        $level3->setAttribute($level4Collection, new Document([
            '$id' => 'level4',
        ]));
        $level3 = $this->getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection]->getId());
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals('level4', $level3[$level4Collection]->getId());

        // Exceed fetch depth
        $level1 = $this->getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);
    }

    public function testExceedMaxDepthOneToManyChild(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToManyChild';
        $level2Collection = 'level2OneToManyChild';
        $level3Collection = 'level3OneToManyChild';
        $level4Collection = 'level4OneToManyChild';

        $this->getDatabase()->createCollection($level1Collection);
        $this->getDatabase()->createCollection($level2Collection);
        $this->getDatabase()->createCollection($level3Collection);
        $this->getDatabase()->createCollection($level4Collection);

        $this->getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $level1 = $this->getDatabase()->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                [
                    '$id' => 'level2',
                    $level3Collection => [
                        [
                            '$id' => 'level3',
                            $level4Collection => [
                                [
                                    '$id' => 'level4',
                                ],
                            ]
                        ],
                    ],
                ],
            ],
        ]));
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection][0]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection][0]);
        $this->assertEquals('level3', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][0][$level3Collection][0]);

        // Confirm the 4th level document does not exist
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals(0, count($level3[$level4Collection]));

        // Create level 4 document
        $level3->setAttribute($level4Collection, [new Document([
            '$id' => 'level4',
        ])]);
        $level3 = $this->getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection][0]->getId());

        // Verify level 4 document is set
        $level3 = $this->getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertArrayHasKey($level4Collection, $level3);
        $this->assertEquals('level4', $level3[$level4Collection][0]->getId());

        // Exceed fetch depth
        $level4 = $this->getDatabase()->getDocument($level4Collection, 'level4');
        $this->assertArrayHasKey($level3Collection, $level4);
        $this->assertEquals('level3', $level4[$level3Collection]->getId());
        $this->assertArrayHasKey($level2Collection, $level4[$level3Collection]);
        $this->assertEquals('level2', $level4[$level3Collection][$level2Collection]->getId());
        $this->assertArrayNotHasKey($level1Collection, $level4[$level3Collection][$level2Collection]);
    }

    public function testCreateRelationshipMissingCollection(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Collection not found');

        $this->getDatabase()->createRelationship(
            collection: 'missing',
            relatedCollection: 'missing',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateRelationshipMissingRelatedCollection(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('test');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Related collection not found');

        $this->getDatabase()->createRelationship(
            collection: 'test',
            relatedCollection: 'missing',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateDuplicateRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('test1');
        $this->getDatabase()->createCollection('test2');

        $this->getDatabase()->createRelationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Attribute already exists');

        $this->getDatabase()->createRelationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateInvalidRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('test3');
        $this->getDatabase()->createCollection('test4');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid relationship type');

        $this->getDatabase()->createRelationship(
            collection: 'test3',
            relatedCollection: 'test4',
            type: 'invalid',
            twoWay: true,
        );
    }

    public function testOneToManyAndManyToOneDeleteRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('relation1');
        static::getDatabase()->createCollection('relation2');

        static::getDatabase()->createRelationship(
            collection: 'relation1',
            relatedCollection: 'relation2',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $relation1 = static::getDatabase()->getCollection('relation1');
        $this->assertCount(1, $relation1->getAttribute('attributes'));
        $this->assertCount(0, $relation1->getAttribute('indexes'));
        $relation2 = static::getDatabase()->getCollection('relation2');
        $this->assertCount(1, $relation2->getAttribute('attributes'));
        $this->assertCount(1, $relation2->getAttribute('indexes'));

        static::getDatabase()->deleteRelationship('relation2', 'relation1');

        $relation1 = static::getDatabase()->getCollection('relation1');
        $this->assertCount(0, $relation1->getAttribute('attributes'));
        $this->assertCount(0, $relation1->getAttribute('indexes'));
        $relation2 = static::getDatabase()->getCollection('relation2');
        $this->assertCount(0, $relation2->getAttribute('attributes'));
        $this->assertCount(0, $relation2->getAttribute('indexes'));

        static::getDatabase()->createRelationship(
            collection: 'relation1',
            relatedCollection: 'relation2',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $relation1 = static::getDatabase()->getCollection('relation1');
        $this->assertCount(1, $relation1->getAttribute('attributes'));
        $this->assertCount(1, $relation1->getAttribute('indexes'));
        $relation2 = static::getDatabase()->getCollection('relation2');
        $this->assertCount(1, $relation2->getAttribute('attributes'));
        $this->assertCount(0, $relation2->getAttribute('indexes'));

        static::getDatabase()->deleteRelationship('relation1', 'relation2');

        $relation1 = static::getDatabase()->getCollection('relation1');
        $this->assertCount(0, $relation1->getAttribute('attributes'));
        $this->assertCount(0, $relation1->getAttribute('indexes'));
        $relation2 = static::getDatabase()->getCollection('relation2');
        $this->assertCount(0, $relation2->getAttribute('attributes'));
        $this->assertCount(0, $relation2->getAttribute('indexes'));
    }

    public function testDeleteMissingRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Attribute not found');

        $this->getDatabase()->deleteRelationship('test', 'test2');
    }

    public function testCreateInvalidIntValueRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('invalid1');
        $this->getDatabase()->createCollection('invalid2');

        $this->getDatabase()->createRelationship(
            collection: 'invalid1',
            relatedCollection: 'invalid2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $this->getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => 10,
        ]));
    }

    /**
     * @depends testCreateInvalidIntValueRelationship
     */
    public function testCreateInvalidObjectValueRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $this->getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => new \stdClass(),
        ]));
    }

    /**
     * @depends testCreateInvalidIntValueRelationship
     */
    public function testCreateInvalidArrayIntValueRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createRelationship(
            collection: 'invalid1',
            relatedCollection: 'invalid2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'invalid3',
            twoWayKey: 'invalid4',
        );

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $this->getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid3' => [10],
        ]));
    }

    public function testCreateEmptyValueRelationship(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('null1');
        $this->getDatabase()->createCollection('null2');

        $this->getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        $this->getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'null3',
            twoWayKey: 'null4',
        );
        $this->getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'null4',
            twoWayKey: 'null5',
        );
        $this->getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'null6',
            twoWayKey: 'null7',
        );

        $document = $this->getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null2' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null2'));

        $document = $this->getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null1' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null1'));

        $document = $this->getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null3' => null,
        ]));

        // One to many will be empty array instead of null
        $this->assertEquals([], $document->getAttribute('null3'));

        $document = $this->getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null4' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null4'));

        $document = $this->getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null4' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null4'));

        $document = $this->getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null5' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null5'));

        $document = $this->getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null6' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null6'));

        $document = $this->getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null7' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null7'));
    }

    public function testDeleteCollectionDeletesRelationships(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('testers');
        $this->getDatabase()->createCollection('devices');

        $this->getDatabase()->createRelationship(
            collection: 'testers',
            relatedCollection: 'devices',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'tester'
        );

        $testers = $this->getDatabase()->getCollection('testers');
        $devices = $this->getDatabase()->getCollection('devices');

        $this->assertEquals(1, \count($testers->getAttribute('attributes')));
        $this->assertEquals(1, \count($devices->getAttribute('attributes')));
        $this->assertEquals(1, \count($devices->getAttribute('indexes')));

        $this->getDatabase()->deleteCollection('testers');

        $testers = $this->getDatabase()->getCollection('testers');
        $devices = $this->getDatabase()->getCollection('devices');

        $this->assertEquals(true, $testers->isEmpty());
        $this->assertEquals(0, \count($devices->getAttribute('attributes')));
        $this->assertEquals(0, \count($devices->getAttribute('indexes')));
    }

    public function testDeleteTwoWayRelationshipFromChild(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('drivers');
        $this->getDatabase()->createCollection('licenses');

        $this->getDatabase()->createRelationship(
            collection: 'drivers',
            relatedCollection: 'licenses',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'license',
            twoWayKey: 'driver'
        );

        $drivers = $this->getDatabase()->getCollection('drivers');
        $licenses = $this->getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(1, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        $this->getDatabase()->deleteRelationship('licenses', 'driver');

        $drivers = $this->getDatabase()->getCollection('drivers');
        $licenses = $this->getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        $this->getDatabase()->createRelationship(
            collection: 'drivers',
            relatedCollection: 'licenses',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'licenses',
            twoWayKey: 'driver'
        );

        $drivers = $this->getDatabase()->getCollection('drivers');
        $licenses = $this->getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        $this->getDatabase()->deleteRelationship('licenses', 'driver');

        $drivers = $this->getDatabase()->getCollection('drivers');
        $licenses = $this->getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        $this->getDatabase()->createRelationship(
            collection: 'licenses',
            relatedCollection: 'drivers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'driver',
            twoWayKey: 'licenses'
        );

        $drivers = $this->getDatabase()->getCollection('drivers');
        $licenses = $this->getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        $this->getDatabase()->deleteRelationship('drivers', 'licenses');

        $drivers = $this->getDatabase()->getCollection('drivers');
        $licenses = $this->getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        $this->getDatabase()->createRelationship(
            collection: 'licenses',
            relatedCollection: 'drivers',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'drivers',
            twoWayKey: 'licenses'
        );

        $drivers = $this->getDatabase()->getCollection('drivers');
        $licenses = $this->getDatabase()->getCollection('licenses');
        $junction = $this->getDatabase()->getCollection('_' . $licenses->getInternalId() . '_' . $drivers->getInternalId());

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));
        $this->assertEquals(2, \count($junction->getAttribute('attributes')));
        $this->assertEquals(2, \count($junction->getAttribute('indexes')));

        $this->getDatabase()->deleteRelationship('drivers', 'licenses');

        $drivers = $this->getDatabase()->getCollection('drivers');
        $licenses = $this->getDatabase()->getCollection('licenses');
        $junction = $this->getDatabase()->getCollection('_licenses_drivers');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        $this->assertEquals(true, $junction->isEmpty());
    }

    public function testUpdateRelationshipToExistingKey(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('ovens');
        $this->getDatabase()->createCollection('cakes');

        $this->getDatabase()->createAttribute('ovens', 'maxTemp', Database::VAR_INTEGER, 0, true);
        $this->getDatabase()->createAttribute('ovens', 'owner', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('cakes', 'height', Database::VAR_INTEGER, 0, true);
        $this->getDatabase()->createAttribute('cakes', 'colour', Database::VAR_STRING, 255, true);

        $this->getDatabase()->createRelationship(
            collection: 'ovens',
            relatedCollection: 'cakes',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'cakes',
            twoWayKey: 'oven'
        );

        try {
            $this->getDatabase()->updateRelationship('ovens', 'cakes', newKey: 'owner');
            $this->fail('Failed to throw exception');
        } catch (DuplicateException $e) {
            $this->assertEquals('Attribute already exists', $e->getMessage());
        }

        try {
            $this->getDatabase()->updateRelationship('ovens', 'cakes', newTwoWayKey: 'height');
            $this->fail('Failed to throw exception');
        } catch (DuplicateException $e) {
            $this->assertEquals('Related attribute already exists', $e->getMessage());
        }
    }

    public function testOneToOneRelationshipKeyWithSymbols(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('$symbols_coll.ection1');
        $this->getDatabase()->createCollection('$symbols_coll.ection2');

        $this->getDatabase()->createRelationship(
            collection: '$symbols_coll.ection1',
            relatedCollection: '$symbols_coll.ection2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $doc1 = $this->getDatabase()->createDocument('$symbols_coll.ection2', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = $this->getDatabase()->createDocument('$symbols_coll.ection1', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection2' => $doc1->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = $this->getDatabase()->getDocument('$symbols_coll.ection2', $doc1->getId());
        $doc2 = $this->getDatabase()->getDocument('$symbols_coll.ection1', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection1')->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection2')->getId());
    }

    public function testOneToManyRelationshipKeyWithSymbols(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('$symbols_coll.ection3');
        $this->getDatabase()->createCollection('$symbols_coll.ection4');

        $this->getDatabase()->createRelationship(
            collection: '$symbols_coll.ection3',
            relatedCollection: '$symbols_coll.ection4',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $doc1 = $this->getDatabase()->createDocument('$symbols_coll.ection4', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = $this->getDatabase()->createDocument('$symbols_coll.ection3', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection4' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = $this->getDatabase()->getDocument('$symbols_coll.ection4', $doc1->getId());
        $doc2 = $this->getDatabase()->getDocument('$symbols_coll.ection3', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection3')->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection4')[0]->getId());
    }

    public function testManyToOneRelationshipKeyWithSymbols(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('$symbols_coll.ection5');
        $this->getDatabase()->createCollection('$symbols_coll.ection6');

        $this->getDatabase()->createRelationship(
            collection: '$symbols_coll.ection5',
            relatedCollection: '$symbols_coll.ection6',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );

        $doc1 = $this->getDatabase()->createDocument('$symbols_coll.ection6', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = $this->getDatabase()->createDocument('$symbols_coll.ection5', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection6' => $doc1->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = $this->getDatabase()->getDocument('$symbols_coll.ection6', $doc1->getId());
        $doc2 = $this->getDatabase()->getDocument('$symbols_coll.ection5', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection5')[0]->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection6')->getId());
    }

    public function testManyToManyRelationshipKeyWithSymbols(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('$symbols_coll.ection7');
        $this->getDatabase()->createCollection('$symbols_coll.ection8');

        $this->getDatabase()->createRelationship(
            collection: '$symbols_coll.ection7',
            relatedCollection: '$symbols_coll.ection8',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $doc1 = $this->getDatabase()->createDocument('$symbols_coll.ection8', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = $this->getDatabase()->createDocument('$symbols_coll.ection7', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection8' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = $this->getDatabase()->getDocument('$symbols_coll.ection8', $doc1->getId());
        $doc2 = $this->getDatabase()->getDocument('$symbols_coll.ection7', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection7')[0]->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection8')[0]->getId());
    }

    public function testCascadeMultiDelete(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('cascadeMultiDelete1');
        $this->getDatabase()->createCollection('cascadeMultiDelete2');
        $this->getDatabase()->createCollection('cascadeMultiDelete3');

        $this->getDatabase()->createRelationship(
            collection: 'cascadeMultiDelete1',
            relatedCollection: 'cascadeMultiDelete2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        $this->getDatabase()->createRelationship(
            collection: 'cascadeMultiDelete2',
            relatedCollection: 'cascadeMultiDelete3',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        $root = $this->getDatabase()->createDocument('cascadeMultiDelete1', new Document([
            '$id' => 'cascadeMultiDelete1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any())
            ],
            'cascadeMultiDelete2' => [
                [
                    '$id' => 'cascadeMultiDelete2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::delete(Role::any())
                    ],
                    'cascadeMultiDelete3' => [
                        [
                            '$id' => 'cascadeMultiDelete3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                                Permission::delete(Role::any())
                            ],
                        ],
                    ],
                ],
            ],
        ]));

        $this->assertCount(1, $root->getAttribute('cascadeMultiDelete2'));
        $this->assertCount(1, $root->getAttribute('cascadeMultiDelete2')[0]->getAttribute('cascadeMultiDelete3'));

        $this->assertEquals(true, $this->getDatabase()->deleteDocument('cascadeMultiDelete1', $root->getId()));

        $multi2 = $this->getDatabase()->getDocument('cascadeMultiDelete2', 'cascadeMultiDelete2');
        $this->assertEquals(true, $multi2->isEmpty());

        $multi3 = $this->getDatabase()->getDocument('cascadeMultiDelete3', 'cascadeMultiDelete3');
        $this->assertEquals(true, $multi3->isEmpty());
    }

    public function testCollectionUpdate(): Document
    {
        $collection = $this->getDatabase()->createCollection('collectionUpdate', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: false);

        $this->assertInstanceOf(Document::class, $collection);

        $collection = $this->getDatabase()->getCollection('collectionUpdate');

        $this->assertFalse($collection->getAttribute('documentSecurity'));
        $this->assertIsArray($collection->getPermissions());
        $this->assertCount(4, $collection->getPermissions());

        $collection = $this->getDatabase()->updateCollection('collectionUpdate', [], true);

        $this->assertTrue($collection->getAttribute('documentSecurity'));
        $this->assertIsArray($collection->getPermissions());
        $this->assertEmpty($collection->getPermissions());

        $collection = $this->getDatabase()->getCollection('collectionUpdate');

        $this->assertTrue($collection->getAttribute('documentSecurity'));
        $this->assertIsArray($collection->getPermissions());
        $this->assertEmpty($collection->getPermissions());

        return $collection;
    }

    /**
     * @depends testCollectionUpdate
     */
    public function testCollectionUpdatePermissionsThrowException(Document $collection): void
    {
        $this->expectException(DatabaseException::class);
        $this->getDatabase()->updateCollection($collection->getId(), permissions: [
            'i dont work'
        ], documentSecurity: false);
    }

    public function testCollectionPermissions(): Document
    {
        $collection = $this->getDatabase()->createCollection('collectionSecurity', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: false);

        $this->assertInstanceOf(Document::class, $collection);

        $this->assertTrue($this->getDatabase()->createAttribute(
            collection: $collection->getId(),
            id: 'test',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        return $collection;
    }

    public function testCollectionPermissionsExceptions(): void
    {
        $this->expectException(DatabaseException::class);
        $this->getDatabase()->createCollection('collectionSecurity', permissions: [
            'i dont work'
        ]);
    }

    /**
     * @depends testCollectionPermissions
     * @return array<Document>
     */
    public function testCollectionPermissionsCreateWorks(Document $collection): array
    {
        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $document = $this->getDatabase()->createDocument($collection->getId(), new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::user('random')),
                Permission::update(Role::user('random')),
                Permission::delete(Role::user('random'))
            ],
            'test' => 'lorem'
        ]));
        $this->assertInstanceOf(Document::class, $document);

        return [$collection, $document];
    }


    /**
     * @depends testCollectionPermissions
     */
    public function testCollectionPermissionsCreateThrowsException(Document $collection): void
    {
        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());
        $this->expectException(AuthorizationException::class);

        $this->getDatabase()->createDocument($collection->getId(), new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any())
            ],
            'test' => 'lorem ipsum'
        ]));
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsGetWorks(array $data): array
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $document = $this->getDatabase()->getDocument(
            $collection->getId(),
            $document->getId()
        );
        $this->assertInstanceOf(Document::class, $document);
        $this->assertFalse($document->isEmpty());

        return $data;
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     */
    public function testCollectionPermissionsGetThrowsException(array $data): void
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $document = $this->getDatabase()->getDocument(
            $collection->getId(),
            $document->getId(),
        );
        $this->assertInstanceOf(Document::class, $document);
        $this->assertTrue($document->isEmpty());
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsFindWorks(array $data): array
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $documents = $this->getDatabase()->find($collection->getId());
        $this->assertNotEmpty($documents);

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::user('random')->toString());

        try {
            $this->getDatabase()->find($collection->getId());
            $this->fail('Failed to throw exception');
        } catch (AuthorizationException) {
        }

        return $data;
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsCreateWorks
     */
    public function testCollectionPermissionsFindThrowsException(array $data): void
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);
        $this->getDatabase()->find($collection->getId());
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsCountWorks(array $data): array
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $count = $this->getDatabase()->count(
            $collection->getId()
        );

        $this->assertNotEmpty($count);

        return $data;
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsCreateWorks
     */
    public function testCollectionPermissionsCountThrowsException(array $data): void
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $count = $this->getDatabase()->count(
            $collection->getId()
        );
        $this->assertEmpty($count);
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsUpdateWorks(array $data): array
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $this->assertInstanceOf(Document::class, $this->getDatabase()->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document->setAttribute('test', 'ipsum')
        ));

        return $data;
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsCreateWorks
     */
    public function testCollectionPermissionsUpdateThrowsException(array $data): void
    {
        [$collection, $document] = $data;
        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);
        $document = $this->getDatabase()->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document->setAttribute('test', 'lorem')
        );
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsUpdateWorks
     */
    public function testCollectionPermissionsDeleteThrowsException(array $data): void
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);
        $this->getDatabase()->deleteDocument(
            $collection->getId(),
            $document->getId()
        );
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsUpdateWorks
     */
    public function testCollectionPermissionsDeleteWorks(array $data): void
    {
        [$collection, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $this->assertTrue($this->getDatabase()->deleteDocument(
            $collection->getId(),
            $document->getId()
        ));
    }

    /**
     * @return array<Document>
     */
    public function testCollectionPermissionsRelationships(): array
    {
        $collection = $this->getDatabase()->createCollection('collectionSecurity.Parent', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collection);

        $this->assertTrue($this->getDatabase()->createAttribute(
            collection: $collection->getId(),
            id: 'test',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $collectionOneToOne = $this->getDatabase()->createCollection('collectionSecurity.OneToOne', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collectionOneToOne);

        $this->assertTrue($this->getDatabase()->createAttribute(
            collection: $collectionOneToOne->getId(),
            id: 'test',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $this->assertTrue($this->getDatabase()->createRelationship(
            collection: $collection->getId(),
            relatedCollection: $collectionOneToOne->getId(),
            type: Database::RELATION_ONE_TO_ONE,
            id: Database::RELATION_ONE_TO_ONE,
            onDelete: Database::RELATION_MUTATE_CASCADE
        ));

        $collectionOneToMany = $this->getDatabase()->createCollection('collectionSecurity.OneToMany', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collectionOneToMany);

        $this->assertTrue($this->getDatabase()->createAttribute(
            collection: $collectionOneToMany->getId(),
            id: 'test',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $this->assertTrue($this->getDatabase()->createRelationship(
            collection: $collection->getId(),
            relatedCollection: $collectionOneToMany->getId(),
            type: Database::RELATION_ONE_TO_MANY,
            id: Database::RELATION_ONE_TO_MANY,
            onDelete: Database::RELATION_MUTATE_CASCADE
        ));

        return [$collection, $collectionOneToOne, $collectionOneToMany];
    }

    /**
     * @depends testCollectionPermissionsRelationships
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsRelationshipsCreateWorks(array $data): array
    {
        [$collection, $collectionOneToOne, $collectionOneToMany] = $data;
        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $document = $this->getDatabase()->createDocument($collection->getId(), new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::user('random')),
                Permission::update(Role::user('random')),
                Permission::delete(Role::user('random'))
            ],
            'test' => 'lorem',
            Database::RELATION_ONE_TO_ONE => [
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::user('random')),
                    Permission::update(Role::user('random')),
                    Permission::delete(Role::user('random'))
                ],
                'test' => 'lorem ipsum'
            ],
            Database::RELATION_ONE_TO_MANY => [
                [
                    '$id' => ID::unique(),
                    '$permissions' => [
                        Permission::read(Role::user('random')),
                        Permission::update(Role::user('random')),
                        Permission::delete(Role::user('random'))
                    ],
                    'test' => 'lorem ipsum'
                ], [
                    '$id' => ID::unique(),
                    '$permissions' => [
                        Permission::read(Role::user('torsten')),
                        Permission::update(Role::user('random')),
                        Permission::delete(Role::user('random'))
                    ],
                    'test' => 'dolor'
                ]
            ],
        ]));
        $this->assertInstanceOf(Document::class, $document);

        return [...$data, $document];
    }

    /**
     * @depends testCollectionPermissionsRelationships
     * @param array<Document> $data
     */
    public function testCollectionPermissionsRelationshipsCreateThrowsException(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());
        $this->expectException(AuthorizationException::class);

        $this->getDatabase()->createDocument($collection->getId(), new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ],
            'test' => 'lorem ipsum'
        ]));
    }

    /**
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsRelationshipsGetWorks(array $data): array
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $document = $this->getDatabase()->getDocument(
            $collection->getId(),
            $document->getId()
        );

        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(Database::RELATION_ONE_TO_ONE));
        $this->assertIsArray($document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertCount(2, $document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertFalse($document->isEmpty());

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::user('random')->toString());

        $document = $this->getDatabase()->getDocument(
            $collection->getId(),
            $document->getId()
        );

        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(Database::RELATION_ONE_TO_ONE));
        $this->assertIsArray($document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertCount(1, $document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertFalse($document->isEmpty());

        return $data;
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     */
    public function testCollectionPermissionsRelationshipsGetThrowsException(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $document = $this->getDatabase()->getDocument(
            $collection->getId(),
            $document->getId(),
        );
        $this->assertInstanceOf(Document::class, $document);
        $this->assertTrue($document->isEmpty());
    }

    /**
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     * @param array<Document> $data
     */
    public function testCollectionPermissionsRelationshipsFindWorks(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $documents = $this->getDatabase()->find(
            $collection->getId()
        );

        $this->assertIsArray($documents);
        $this->assertCount(1, $documents);
        $document = $documents[0];
        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(Database::RELATION_ONE_TO_ONE));
        $this->assertIsArray($document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertCount(2, $document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertFalse($document->isEmpty());

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::user('random')->toString());

        $documents = $this->getDatabase()->find(
            $collection->getId()
        );

        $this->assertIsArray($documents);
        $this->assertCount(1, $documents);
        $document = $documents[0];
        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(Database::RELATION_ONE_TO_ONE));
        $this->assertIsArray($document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertCount(1, $document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertFalse($document->isEmpty());

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::user('unknown')->toString());

        $documents = $this->getDatabase()->find(
            $collection->getId()
        );

        $this->assertIsArray($documents);
        $this->assertCount(0, $documents);
    }

    /**
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     * @param array<Document> $data
     */
    public function testCollectionPermissionsRelationshipsCountWorks(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $documents = $this->getDatabase()->count(
            $collection->getId()
        );

        $this->assertEquals(1, $documents);

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::user('random')->toString());

        $documents = $this->getDatabase()->count(
            $collection->getId()
        );

        $this->assertEquals(1, $documents);

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::user('unknown')->toString());

        $documents = $this->getDatabase()->count(
            $collection->getId()
        );

        $this->assertEquals(0, $documents);
    }

    /**
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsRelationshipsUpdateWorks(array $data): array
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());
        $this->getDatabase()->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document
        );

        $this->assertTrue(true);

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::user('random')->toString());

        $this->getDatabase()->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document->setAttribute('test', 'ipsum')
        );

        $this->assertTrue(true);

        return $data;
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     */
    public function testCollectionPermissionsRelationshipsUpdateThrowsException(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);
        $document = $this->getDatabase()->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document->setAttribute('test', $document->getAttribute('test').'new_value')
        );
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsRelationshipsUpdateWorks
     */
    public function testCollectionPermissionsRelationshipsDeleteThrowsException(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);
        $document = $this->getDatabase()->deleteDocument(
            $collection->getId(),
            $document->getId()
        );
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsRelationshipsUpdateWorks
     */
    public function testCollectionPermissionsRelationshipsDeleteWorks(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::users()->toString());

        $this->assertTrue($this->getDatabase()->deleteDocument(
            $collection->getId(),
            $document->getId()
        ));
    }

    public function testCreateRelationDocumentWithoutUpdatePermission(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        self::$authorization->cleanRoles();
        self::$authorization->addRole(Role::user('a')->toString());

        $this->getDatabase()->createCollection('parentRelationTest', [], [], [
            Permission::read(Role::user('a')),
            Permission::create(Role::user('a')),
            Permission::update(Role::user('a')),
            Permission::delete(Role::user('a'))
        ]);
        $this->getDatabase()->createCollection('childRelationTest', [], [], [
            Permission::create(Role::user('a')),
            Permission::read(Role::user('a')),
        ]);
        $this->getDatabase()->createAttribute('parentRelationTest', 'name', Database::VAR_STRING, 255, false);
        $this->getDatabase()->createAttribute('childRelationTest', 'name', Database::VAR_STRING, 255, false);

        $this->getDatabase()->createRelationship(
            collection: 'parentRelationTest',
            relatedCollection: 'childRelationTest',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'children'
        );

        // Create document with relationship with nested data
        $parent = $this->getDatabase()->createDocument('parentRelationTest', new Document([
            '$id' => 'parent1',
            'name' => 'Parent 1',
            'children' => [
                [
                    '$id' => 'child1',
                    'name' => 'Child 1',
                ],
            ],
        ]));
        $this->assertEquals('child1', $parent->getAttribute('children')[0]->getId());
        $parent->setAttribute('children', [
            [
                '$id' => 'child2',
            ],
        ]);
        $updatedParent = $this->getDatabase()->updateDocument('parentRelationTest', 'parent1', $parent);

        $this->assertEquals('child2', $updatedParent->getAttribute('children')[0]->getId());

        $this->getDatabase()->deleteCollection('parentRelationTest');
        $this->getDatabase()->deleteCollection('childRelationTest');
    }

    public function testUpdateDocumentWithRelationships(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $this->getDatabase()->createCollection('userProfiles', [
            new Document([
                '$id' => ID::custom('username'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $this->getDatabase()->createCollection('links', [
            new Document([
                '$id' => ID::custom('title'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $this->getDatabase()->createCollection('videos', [
            new Document([
                '$id' => ID::custom('title'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $this->getDatabase()->createCollection('products', [
            new Document([
                '$id' => ID::custom('title'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $this->getDatabase()->createCollection('settings', [
            new Document([
                '$id' => ID::custom('metaTitle'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $this->getDatabase()->createCollection('appearance', [
            new Document([
                '$id' => ID::custom('metaTitle'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $this->getDatabase()->createCollection('group', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $this->getDatabase()->createCollection('community', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 700,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $this->getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'links',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'links'
        );

        $this->getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'videos',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'videos'
        );

        $this->getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'products',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'products',
            twoWayKey: 'userProfile',
        );

        $this->getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'settings',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'settings'
        );

        $this->getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'appearance',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'appearance'
        );

        $this->getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'group',
            type: Database::RELATION_MANY_TO_ONE,
            id: 'group'
        );

        $this->getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'community',
            type: Database::RELATION_MANY_TO_ONE,
            id: 'community'
        );

        $profile = $this->getDatabase()->createDocument('userProfiles', new Document([
            '$id' => '1',
            'username' => 'user1',
            'links' => [
                [
                    '$id' => 'link1',
                    'title' => 'Link 1',
                ],
            ],
            'videos' => [
                [
                    '$id' => 'video1',
                    'title' => 'Video 1',
                ],
            ],
            'products' => [
                [
                    '$id' => 'product1',
                    'title' => 'Product 1',
                ],
            ],
            'settings' => [
                '$id' => 'settings1',
                'metaTitle' => 'Meta Title',
            ],
            'appearance' => [
                '$id' => 'appearance1',
                'metaTitle' => 'Meta Title',
            ],
            'group' => [
                '$id' => 'group1',
                'name' => 'Group 1',
            ],
            'community' => [
                '$id' => 'community1',
                'name' => 'Community 1',
            ],
        ]));
        $this->assertEquals('link1', $profile->getAttribute('links')[0]->getId());
        $this->assertEquals('settings1', $profile->getAttribute('settings')->getId());
        $this->assertEquals('group1', $profile->getAttribute('group')->getId());
        $this->assertEquals('community1', $profile->getAttribute('community')->getId());
        $this->assertEquals('video1', $profile->getAttribute('videos')[0]->getId());
        $this->assertEquals('product1', $profile->getAttribute('products')[0]->getId());
        $this->assertEquals('appearance1', $profile->getAttribute('appearance')->getId());

        $profile->setAttribute('links', [
            [
                '$id' => 'link1',
                'title' => 'New Link Value',
            ],
        ]);

        $profile->setAttribute('settings', [
            '$id' => 'settings1',
            'metaTitle' => 'New Meta Title',
        ]);

        $profile->setAttribute('group', [
            '$id' => 'group1',
            'name' => 'New Group Name',
        ]);

        $updatedProfile = $this->getDatabase()->updateDocument('userProfiles', '1', $profile);

        $this->assertEquals('New Link Value', $updatedProfile->getAttribute('links')[0]->getAttribute('title'));
        $this->assertEquals('New Meta Title', $updatedProfile->getAttribute('settings')->getAttribute('metaTitle'));
        $this->assertEquals('New Group Name', $updatedProfile->getAttribute('group')->getAttribute('name'));

        // This is the point of test, related documents should be present if they are not updated
        $this->assertEquals('Video 1', $updatedProfile->getAttribute('videos')[0]->getAttribute('title'));
        $this->assertEquals('Product 1', $updatedProfile->getAttribute('products')[0]->getAttribute('title'));
        $this->assertEquals('Meta Title', $updatedProfile->getAttribute('appearance')->getAttribute('metaTitle'));
        $this->assertEquals('Community 1', $updatedProfile->getAttribute('community')->getAttribute('name'));

        // updating document using two way key in one to many relationship
        $product = $this->getDatabase()->getDocument('products', 'product1');
        $product->setAttribute('userProfile', [
            '$id' => '1',
            'username' => 'updated user value',
        ]);
        $updatedProduct = $this->getDatabase()->updateDocument('products', 'product1', $product);
        $this->assertEquals('updated user value', $updatedProduct->getAttribute('userProfile')->getAttribute('username'));
        $this->assertEquals('Product 1', $updatedProduct->getAttribute('title'));
        $this->assertEquals('product1', $updatedProduct->getId());
        $this->assertEquals('1', $updatedProduct->getAttribute('userProfile')->getId());

        $this->getDatabase()->deleteCollection('userProfiles');
        $this->getDatabase()->deleteCollection('links');
        $this->getDatabase()->deleteCollection('settings');
        $this->getDatabase()->deleteCollection('group');
        $this->getDatabase()->deleteCollection('community');
        $this->getDatabase()->deleteCollection('videos');
        $this->getDatabase()->deleteCollection('products');
        $this->getDatabase()->deleteCollection('appearance');
    }

    public function testRecreateOneToOneOneWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToOneOneWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToOneTwoWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToOneTwoWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToManyOneWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToManyOneWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToManyTwoWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToManyTwoWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToOneOneWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_ONE,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToOneOneWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_ONE,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToOneTwoWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToOneTwoWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToManyOneWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToManyOneWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToManyTwoWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToManyTwoWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        static::getDatabase()->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testLabels(): void
    {
        $this->assertInstanceOf('Utopia\Database\Document', $this->getDatabase()->createCollection(
            'labels_test',
        ));
        $this->getDatabase()->createAttribute('labels_test', 'attr1', Database::VAR_STRING, 10, false);

        $this->getDatabase()->createDocument('labels_test', new Document([
            '$id' => 'doc1',
            'attr1' => 'value1',
            '$permissions' => [
                Permission::read(Role::label('reader')),
            ],
        ]));

        $documents = $this->getDatabase()->find('labels_test');

        $this->assertEmpty($documents);

        self::$authorization->addRole(Role::label('reader')->toString());

        $documents = $this->getDatabase()->find('labels_test');

        $this->assertCount(1, $documents);
    }

    public function testEnableDisableValidation(): void
    {
        $database = $this->getDatabase();

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
    }

    public function testMetadata(): void
    {
        $this->getDatabase()->setMetadata('key', 'value');

        $this->getDatabase()->createCollection('testers');

        $this->assertEquals(['key' => 'value'], $this->getDatabase()->getMetadata());

        $this->getDatabase()->resetMetadata();

        $this->assertEquals([], $this->getDatabase()->getMetadata());
    }

    public function testEmptyOperatorValues(): void
    {
        try {
            $this->getDatabase()->findOne('documents', [
                Query::equal('string', []),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals('Invalid query: Equal queries require at least one value.', $e->getMessage());
        }

        try {
            $this->getDatabase()->findOne('documents', [
                Query::contains('string', []),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(Exception::class, $e);
            $this->assertEquals('Invalid query: Contains queries require at least one value.', $e->getMessage());
        }
    }

    /**
     * @throws AuthorizationException
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     * @throws QueryException
     * @throws StructureException
     * @throws TimeoutException
     */
    public function testSharedTables(): void
    {
        /**
         * Default mode already tested, we'll test 'schema' and 'table' isolation here
         */
        $database = $this->getDatabase();

        if (!$database->getAdapter()->getSupportForSchemas()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if ($database->exists('schema1')) {
            $database->setDatabase('schema1')->delete();
        }
        if ($database->exists('schema2')) {
            $database->setDatabase('schema2')->delete();
        }
        if ($database->exists('sharedTables')) {
            $database->setDatabase('sharedTables')->delete();
        }

        /**
         * Schema
         */
        $database
            ->setDatabase('schema1')
            ->setNamespace('')
            ->create();

        $this->assertEquals(true, $database->exists('schema1'));

        $database
            ->setDatabase('schema2')
            ->setNamespace('')
            ->create();

        $this->assertEquals(true, $database->exists('schema2'));

        /**
         * Table
         */

        $tenant1 = 1;
        $tenant2 = 2;

        $database
            ->setDatabase('sharedTables')
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant($tenant1)
            ->create();

        $this->assertEquals(true, $database->exists('sharedTables'));

        $database->createCollection('people', [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 128,
                'required' => true,
            ]),
            new Document([
                '$id' => 'lifeStory',
                'type' => Database::VAR_STRING,
                'size' => 65536,
                'required' => true,
            ])
        ], [
            new Document([
                '$id' => 'idx_name',
                'type' => Database::INDEX_KEY,
                'attributes' => ['name']
            ])
        ], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $this->assertCount(1, $database->listCollections());

        if ($database->getAdapter()->getSupportForFulltextIndex()) {
            $database->createIndex(
                collection: 'people',
                id: 'idx_lifeStory',
                type: Database::INDEX_FULLTEXT,
                attributes: ['lifeStory']
            );
        }

        $docId = ID::unique();

        $database->createDocument('people', new Document([
            '$id' => $docId,
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Spiderman',
            'lifeStory' => 'Spider-Man is a superhero appearing in American comic books published by Marvel Comics.'
        ]));

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals('Spiderman', $doc['name']);
        $this->assertEquals($tenant1, $doc->getAttribute('$tenant'));

        /**
         * Remove Permissions
         */
        $doc->setAttribute('$permissions', [
            Permission::read(Role::any())
        ]);

        $database->updateDocument('people', $docId, $doc);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals([Permission::read(Role::any())], $doc['$permissions']);
        $this->assertEquals($tenant1, $doc->getAttribute('$tenant'));

        /**
         * Add Permissions
         */
        $doc->setAttribute('$permissions', [
            Permission::read(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->updateDocument('people', $docId, $doc);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals([Permission::read(Role::any()), Permission::delete(Role::any())], $doc['$permissions']);

        $docs = $database->find('people');
        $this->assertCount(1, $docs);

        // Swap to tenant 2, no access
        $database->setTenant($tenant2);

        try {
            $database->getDocument('people', $docId);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        try {
            $database->find('people');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Collection not found', $e->getMessage());
        }

        $this->assertCount(0, $database->listCollections());

        // Swap back to tenant 1, allowed
        $database->setTenant($tenant1);

        $doc = $database->getDocument('people', $docId);
        $this->assertEquals('Spiderman', $doc['name']);
        $docs = $database->find('people');
        $this->assertEquals(1, \count($docs));

        // Remove tenant but leave shared tables enabled
        $database->setTenant(null);

        try {
            $database->getDocument('people', $docId);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing tenant. Tenant must be set when table sharing is enabled.', $e->getMessage());
        }

        // Reset state
        $database->setSharedTables(false);
        $database->setNamespace(static::$namespace);
        $database->setDatabase($this->testDatabase);
    }

    public function testSharedTablesDuplicatesDontThrow(): void
    {
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        if ($database->exists('sharedTables')) {
            $database->setDatabase('sharedTables')->delete();
        }

        $database
            ->setDatabase('sharedTables')
            ->setNamespace('')
            ->setSharedTables(true)
            ->setTenant(1)
            ->create();

        // Create collection
        $database->createCollection('duplicates', documentSecurity: false);
        $database->createAttribute('duplicates', 'name', Database::VAR_STRING, 10, false);
        $database->createIndex('duplicates', 'nameIndex', Database::INDEX_KEY, ['name']);

        $database->setTenant(2);

        try {
            $database->createCollection('duplicates', documentSecurity: false);
        } catch (DuplicateException) {
            // Ignore
        }

        $database->createAttribute('duplicates', 'name', Database::VAR_STRING, 10, false);
        $database->createIndex('duplicates', 'nameIndex', Database::INDEX_KEY, ['name']);

        $collection = $database->getCollection('duplicates');
        $this->assertEquals(1, \count($collection->getAttribute('attributes')));
        $this->assertEquals(1, \count($collection->getAttribute('indexes')));

        $database->setTenant(1);

        $collection = $database->getCollection('duplicates');
        $this->assertEquals(1, \count($collection->getAttribute('attributes')));
        $this->assertEquals(1, \count($collection->getAttribute('indexes')));

        $database->setSharedTables(false);
        $database->setNamespace(static::$namespace);
        $database->setDatabase($this->testDatabase);
    }

    public function testTransformations(): void
    {
        $this->getDatabase()->createCollection('docs', attributes: [
            new Document([
                '$id' => 'name',
                'type' => Database::VAR_STRING,
                'size' => 767,
                'required' => true,
            ])
        ]);

        $this->getDatabase()->createDocument('docs', new Document([
            '$id' => 'doc1',
            'name' => 'value1',
        ]));

        $this->getDatabase()->before(Database::EVENT_DOCUMENT_READ, 'test', function (string $query) {
            return "SELECT 1";
        });

        $result = $this->getDatabase()->getDocument('docs', 'doc1');

        $this->assertTrue($result->isEmpty());
    }

    public function testEvents(): void
    {
        self::$authorization->skip(function () {
            $database = $this->getDatabase();

            $events = [
                Database::EVENT_DATABASE_CREATE,
                Database::EVENT_DATABASE_LIST,
                Database::EVENT_COLLECTION_CREATE,
                Database::EVENT_COLLECTION_LIST,
                Database::EVENT_COLLECTION_READ,
                Database::EVENT_ATTRIBUTE_CREATE,
                Database::EVENT_ATTRIBUTE_UPDATE,
                Database::EVENT_INDEX_CREATE,
                Database::EVENT_DOCUMENT_CREATE,
                Database::EVENT_DOCUMENT_UPDATE,
                Database::EVENT_DOCUMENT_READ,
                Database::EVENT_DOCUMENT_FIND,
                Database::EVENT_DOCUMENT_FIND,
                Database::EVENT_DOCUMENT_COUNT,
                Database::EVENT_DOCUMENT_SUM,
                Database::EVENT_DOCUMENT_INCREASE,
                Database::EVENT_DOCUMENT_DECREASE,
                Database::EVENT_INDEX_DELETE,
                Database::EVENT_DOCUMENT_DELETE,
                Database::EVENT_ATTRIBUTE_DELETE,
                Database::EVENT_COLLECTION_DELETE,
                Database::EVENT_DATABASE_DELETE,
            ];

            $database->on(Database::EVENT_ALL, 'test', function ($event, $data) use (&$events) {
                $shifted = array_shift($events);

                $this->assertEquals($shifted, $event);
            });

            if ($this->getDatabase()->getAdapter()->getSupportForSchemas()) {
                $database->setDatabase('hellodb');
                $database->create();
            } else {
                array_shift($events);
            }

            $database->list();

            $database->setDatabase($this->testDatabase);

            $collectionId = ID::unique();
            $database->createCollection($collectionId);
            $database->listCollections();
            $database->getCollection($collectionId);
            $database->createAttribute($collectionId, 'attr1', Database::VAR_INTEGER, 2, false);
            $database->updateAttributeRequired($collectionId, 'attr1', true);
            $indexId1 = 'index2_' . uniqid();
            $database->createIndex($collectionId, $indexId1, Database::INDEX_KEY, ['attr1']);

            $document = $database->createDocument($collectionId, new Document([
                '$id' => 'doc1',
                'attr1' => 10,
                '$permissions' => [
                    Permission::delete(Role::any()),
                    Permission::update(Role::any()),
                    Permission::read(Role::any()),
                ],
            ]));

            $executed = false;
            $database->on(Database::EVENT_ALL, 'should-not-execute', function ($event, $data) use (&$executed) {
                $executed = true;
            });

            $database->silent(function () use ($database, $collectionId, $document) {
                $database->updateDocument($collectionId, 'doc1', $document->setAttribute('attr1', 15));
                $database->getDocument($collectionId, 'doc1');
                $database->find($collectionId);
                $database->findOne($collectionId);
                $database->count($collectionId);
                $database->sum($collectionId, 'attr1');
                $database->increaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
                $database->decreaseDocumentAttribute($collectionId, $document->getId(), 'attr1');
            }, ['should-not-execute']);

            $this->assertFalse($executed);

            $database->deleteIndex($collectionId, $indexId1);
            $database->deleteDocument($collectionId, 'doc1');
            $database->deleteAttribute($collectionId, 'attr1');
            $database->deleteCollection($collectionId);
            $database->delete('hellodb');
        });
    }
}

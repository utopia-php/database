<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

trait RelationshipTests
{
    public function testDeleteRelatedCollection(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('c1');
        static::getDatabase()->createCollection('c2');

        // ONE_TO_ONE
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c1'));
        $collection = static::getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c1');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c2'));
        $collection = static::getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c2');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c1'));
        $collection = static::getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c1');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c2'));
        $collection = static::getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // ONE_TO_MANY
        static::getDatabase()->createCollection('c2');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c1'));
        $collection = static::getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c1');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c2'));
        $collection = static::getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c2');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c1'));
        $collection = static::getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c1');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c2'));
        $collection = static::getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // RELATION_MANY_TO_ONE
        static::getDatabase()->createCollection('c2');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c1'));
        $collection = static::getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c1');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c2'));
        $collection = static::getDatabase()->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c2');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c1'));
        $collection = static::getDatabase()->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        static::getDatabase()->createCollection('c1');
        static::getDatabase()->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, static::getDatabase()->deleteCollection('c2'));
        $collection = static::getDatabase()->getCollection('c1');
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
            $this->assertInstanceOf(RelationshipException::class, $e);
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

    public function testStructureValidationAfterRelationsAttribute(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection("structure_1", [], [], [Permission::create(Role::any())]);
        static::getDatabase()->createCollection("structure_2", [], [], [Permission::create(Role::any())]);

        static::getDatabase()->createRelationship(
            collection: "structure_1",
            relatedCollection: "structure_2",
            type: Database::RELATION_ONE_TO_ONE,
        );

        try {
            static::getDatabase()->createDocument('structure_1', new Document([
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
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
            static::getDatabase()->createCollection("level{$i}", [$attribute], [], $permissions);
        }

        for ($i = 1; $i < 5; $i++) {
            $collectionId = $i;
            $relatedCollectionId = $i + 1;
            static::getDatabase()->createRelationship(
                collection: "level{$collectionId}",
                relatedCollection: "level{$relatedCollectionId}",
                type: Database::RELATION_ONE_TO_ONE,
                id: "level{$relatedCollectionId}"
            );
        }

        // Create document with relationship with nested data
        $level1 = static::getDatabase()->createDocument('level1', new Document([
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
        static::getDatabase()->updateDocument('level1', $level1->getId(), new Document($level1->getArrayCopy()));
        $updatedLevel1 = static::getDatabase()->getDocument('level1', $level1->getId());
        $this->assertEquals($level1, $updatedLevel1);

        try {
            static::getDatabase()->updateDocument('level1', $level1->getId(), $level1->setAttribute('name', 'haha'));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }
        $level1->setAttribute('name', 'Level 1');
        static::getDatabase()->updateCollection('level3', [
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

        $level1 = static::getDatabase()->updateDocument('level1', $level1->getId(), $level1);
        $this->assertEquals('updated value', $level1['level2']['level3']['name']);

        for ($i = 1; $i < 6; $i++) {
            static::getDatabase()->deleteCollection("level{$i}");
        }
    }

    public function testDeleteBulkDocumentsOneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships() || !static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('bulk_delete_person_o2o');
        $this->getDatabase()->createCollection('bulk_delete_library_o2o');

        $this->getDatabase()->createAttribute('bulk_delete_person_o2o', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_o2o', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_o2o', 'area', Database::VAR_STRING, 255, true);

        // Restrict
        $this->getDatabase()->createRelationship(
            collection: 'bulk_delete_person_o2o',
            relatedCollection: 'bulk_delete_library_o2o',
            type: Database::RELATION_ONE_TO_ONE,
            onDelete: Database::RELATION_MUTATE_RESTRICT
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => [
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

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_o2o');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('bulk_delete_person_o2o', $library);

        // Delete person
        try {
            $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
            $this->fail('Failed to throw exception');
        } catch (RestrictedException $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        $this->getDatabase()->updateDocument('bulk_delete_person_o2o', 'person1', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => null,
        ]));

        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2o'));
        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));

        // NULL
        $this->getDatabase()->updateRelationship(
            collection: 'bulk_delete_person_o2o',
            id: 'bulk_delete_library_o2o',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => [
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

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_o2o');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('bulk_delete_person_o2o', $library);

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');

        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));
        $this->assertCount(1, $this->getDatabase()->find('bulk_delete_person_o2o'));

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person->getAttribute('bulk_delete_library_o2o');
        $this->assertNull($library);

        // NULL - Cleanup
        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2o'));
        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));

        // Cascade
        $this->getDatabase()->updateRelationship(
            collection: 'bulk_delete_person_o2o',
            id: 'bulk_delete_library_o2o',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => [
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

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_o2o');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('bulk_delete_person_o2o', $library);

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');

        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));
        $this->assertCount(1, $this->getDatabase()->find('bulk_delete_person_o2o'));

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person->getAttribute('bulk_delete_library_o2o');
        $this->assertEmpty($library);
        $this->assertNotNull($library);

        // Test Bulk delete parent
        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2o'));

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => [
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

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_o2o');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('bulk_delete_person_o2o', $library);

        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2o'));
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));
    }

    public function testDeleteBulkDocumentsOneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships() || !static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('bulk_delete_person_o2m');
        $this->getDatabase()->createCollection('bulk_delete_library_o2m');

        $this->getDatabase()->createAttribute('bulk_delete_person_o2m', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_o2m', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_o2m', 'area', Database::VAR_STRING, 255, true);

        // Restrict
        $this->getDatabase()->createRelationship(
            collection: 'bulk_delete_person_o2m',
            relatedCollection: 'bulk_delete_library_o2m',
            type: Database::RELATION_ONE_TO_MANY,
            onDelete: Database::RELATION_MUTATE_RESTRICT
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2m', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2m' => [
                [
                    '$id' => 'library1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 1',
                    'area' => 'Area 1',
                ],
                [
                    '$id' => 'library2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 2',
                    'area' => 'Area 2',
                ],
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person1->getAttribute('bulk_delete_library_o2m');
        $this->assertCount(2, $libraries);

        // Delete person
        try {
            $this->getDatabase()->deleteDocuments('bulk_delete_person_o2m');
            $this->fail('Failed to throw exception');
        } catch (RestrictedException $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Restrict Cleanup
        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2m'));

        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2m'));

        // NULL
        $this->getDatabase()->updateRelationship(
            collection: 'bulk_delete_person_o2m',
            id: 'bulk_delete_library_o2m',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2m', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2m' => [
                [
                    '$id' => 'library1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 1',
                    'area' => 'Area 1',
                ],
                [
                    '$id' => 'library2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 2',
                    'area' => 'Area 2',
                ],
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person1->getAttribute('bulk_delete_library_o2m');
        $this->assertCount(2, $libraries);

        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2m'));

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person->getAttribute('bulk_delete_library_o2m');
        $this->assertEmpty($libraries);

        // NULL - Cleanup
        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2m'));


        // Cascade
        $this->getDatabase()->updateRelationship(
            collection: 'bulk_delete_person_o2m',
            id: 'bulk_delete_library_o2m',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2m', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2m' => [
                [
                    '$id' => 'library1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 1',
                    'area' => 'Area 1',
                ],
                [
                    '$id' => 'library2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 2',
                    'area' => 'Area 2',
                ],
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person1->getAttribute('bulk_delete_library_o2m');
        $this->assertCount(2, $libraries);

        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2m'));

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person->getAttribute('bulk_delete_library_o2m');
        $this->assertEmpty($libraries);
    }

    public function testDeleteBulkDocumentsManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships() || !static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('bulk_delete_person_m2m');
        $this->getDatabase()->createCollection('bulk_delete_library_m2m');

        $this->getDatabase()->createAttribute('bulk_delete_person_m2m', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_m2m', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_m2m', 'area', Database::VAR_STRING, 255, true);

        // Many-to-Many Relationship
        $this->getDatabase()->createRelationship(
            collection: 'bulk_delete_person_m2m',
            relatedCollection: 'bulk_delete_library_m2m',
            type: Database::RELATION_MANY_TO_MANY,
            onDelete: Database::RELATION_MUTATE_RESTRICT
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_m2m', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_m2m' => [
                [
                    '$id' => 'library1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 1',
                    'area' => 'Area 1',
                ],
                [
                    '$id' => 'library2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 2',
                    'area' => 'Area 2',
                ],
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_m2m', 'person1');
        $libraries = $person1->getAttribute('bulk_delete_library_m2m');
        $this->assertCount(2, $libraries);

        // Delete person
        try {
            $this->getDatabase()->deleteDocuments('bulk_delete_person_m2m');
            $this->fail('Failed to throw exception');
        } catch (RestrictedException $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Restrict Cleanup
        $this->getDatabase()->deleteRelationship('bulk_delete_person_m2m', 'bulk_delete_library_m2m');
        $this->getDatabase()->deleteDocuments('bulk_delete_library_m2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_m2m'));

        $this->getDatabase()->deleteDocuments('bulk_delete_person_m2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_m2m'));
    }

    public function testDeleteBulkDocumentsManyToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships() || !static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('bulk_delete_person_m2o');
        $this->getDatabase()->createCollection('bulk_delete_library_m2o');

        $this->getDatabase()->createAttribute('bulk_delete_person_m2o', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_m2o', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_m2o', 'area', Database::VAR_STRING, 255, true);

        // Many-to-One Relationship
        $this->getDatabase()->createRelationship(
            collection: 'bulk_delete_person_m2o',
            relatedCollection: 'bulk_delete_library_m2o',
            type: Database::RELATION_MANY_TO_ONE,
            onDelete: Database::RELATION_MUTATE_RESTRICT
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_m2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_m2o' => [
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

        $person2 = $this->getDatabase()->createDocument('bulk_delete_person_m2o', new Document([
            '$id' => 'person2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 2',
            'bulk_delete_library_m2o' => [
                '$id' => 'library1',
            ]
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_m2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_m2o');
        $this->assertEquals('library1', $library['$id']);

        // Delete library
        try {
            $this->getDatabase()->deleteDocuments('bulk_delete_library_m2o');
            $this->fail('Failed to throw exception');
        } catch (RestrictedException $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        $this->assertEquals(2, count($this->getDatabase()->find('bulk_delete_person_m2o')));

        // Test delete people
        $this->getDatabase()->deleteDocuments('bulk_delete_person_m2o');
        $this->assertEquals(0, count($this->getDatabase()->find('bulk_delete_person_m2o')));

        // Restrict Cleanup
        $this->getDatabase()->deleteDocuments('bulk_delete_library_m2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_m2o'));

        $this->getDatabase()->deleteDocuments('bulk_delete_person_m2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_m2o'));
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

    public function testNoInvalidKeysWithRelationships(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('species');
        static::getDatabase()->createCollection('creatures');
        static::getDatabase()->createCollection('characteristics');

        static::getDatabase()->createAttribute('species', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('creatures', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('characteristics', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'species',
            relatedCollection: 'creatures',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'creature',
            twoWayKey:'species'
        );
        static::getDatabase()->createRelationship(
            collection: 'creatures',
            relatedCollection: 'characteristics',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'characteristic',
            twoWayKey:'creature'
        );

        $species = static::getDatabase()->createDocument('species', new Document([
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
                'characteristic' => [
                    '$id' => ID::custom('1'),
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'active',
                ]
            ]
        ]));
        static::getDatabase()->updateDocument('species', $species->getId(), new Document([
            '$id' => ID::custom('1'),
            '$collection' => 'species',
            'creature' => [
                '$id' => ID::custom('1'),
                '$collection' => 'creatures',
                'characteristic' => [
                    '$id' => ID::custom('1'),
                    'name' => 'active',
                    '$collection' => 'characteristics',
                ]
            ]
        ]));

        $updatedSpecies = static::getDatabase()->getDocument('species', $species->getId());

        $this->assertEquals($species, $updatedSpecies);
    }

    // Relationships
    public function testOneToOneOneWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('person');
        static::getDatabase()->createCollection('library');

        static::getDatabase()->createAttribute('person', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('library', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('library', 'area', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'person',
            relatedCollection: 'library',
            type: Database::RELATION_ONE_TO_ONE
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('person');
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
            static::getDatabase()->deleteAttribute('person', 'library');
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete relationship as an attribute', $e->getMessage());
        }

        // Create document with relationship with nested data
        $person1 = static::getDatabase()->createDocument('person', new Document([
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
        static::getDatabase()->updateDocument(
            'person',
            'person1',
            $person1->setAttribute('library', 'no-library')
        );

        $person1Document = static::getDatabase()->getDocument('person', 'person1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $person1Document->getAttribute('library'));

        static::getDatabase()->updateDocument(
            'person',
            'person1',
            $person1->setAttribute('library', 'library1')
        );

        // Update through create
        $library10 = static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Library 10',
            'area' => 'Area 10',
        ]));
        $person10 = static::getDatabase()->createDocument('person', new Document([
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
        $library10 = static::getDatabase()->getDocument('library', $library10->getId());
        $this->assertEquals('Library 10 Updated', $library10->getAttribute('name'));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Library 2',
            'area' => 'Area 2',
        ]));
        static::getDatabase()->createDocument('person', new Document([
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
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $library = $person1->getAttribute('library');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('person', $library);

        $person = static::getDatabase()->getDocument('person', 'person2');
        $library = $person->getAttribute('library');
        $this->assertEquals('library2', $library['$id']);
        $this->assertArrayNotHasKey('person', $library);

        // Get related documents
        $library = static::getDatabase()->getDocument('library', 'library1');
        $this->assertArrayNotHasKey('person', $library);

        $library = static::getDatabase()->getDocument('library', 'library2');
        $this->assertArrayNotHasKey('person', $library);

        $people = static::getDatabase()->find('person', [
            Query::select(['name'])
        ]);

        $this->assertArrayNotHasKey('library', $people[0]);

        $people = static::getDatabase()->find('person');
        $this->assertEquals(3, \count($people));

        // Select related document attributes
        $person = static::getDatabase()->findOne('person', [
            Query::select(['*', 'library.name'])
        ]);

        if ($person->isEmpty()) {
            throw new Exception('Person not found');
        }

        $this->assertEquals('Library 1', $person->getAttribute('library')->getAttribute('name'));
        $this->assertArrayNotHasKey('area', $person->getAttribute('library'));

        $person = static::getDatabase()->getDocument('person', 'person1', [
            Query::select(['*', 'library.name', '$id'])
        ]);

        $this->assertEquals('Library 1', $person->getAttribute('library')->getAttribute('name'));
        $this->assertArrayNotHasKey('area', $person->getAttribute('library'));



        $document = static::getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['name']),
        ]);
        $this->assertArrayNotHasKey('library', $document);
        $this->assertEquals('Person 1', $document['name']);

        $document = static::getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['*']),
        ]);
        $this->assertEquals('library1', $document['library']);

        $document = static::getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['library.*']),
        ]);
        $this->assertEquals('Library 1', $document['library']['name']);
        $this->assertArrayNotHasKey('name', $document);

        // Update root document attribute without altering relationship
        $person1 = static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('name', 'Person 1 Updated')
        );

        $this->assertEquals('Person 1 Updated', $person1->getAttribute('name'));
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $this->assertEquals('Person 1 Updated', $person1->getAttribute('name'));

        // Update nested document attribute
        $person1 = static::getDatabase()->updateDocument(
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
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $this->assertEquals('Library 1 Updated', $person1->getAttribute('library')->getAttribute('name'));

        // Create new document with no relationship
        $person3 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 3',
        ]));

        // Update to relate to created document
        $person3 = static::getDatabase()->updateDocument(
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
        $person3 = static::getDatabase()->getDocument('person', 'person3');
        $this->assertEquals('Library 3', $person3['library']['name']);

        $libraryDocument = static::getDatabase()->getDocument('library', 'library3');
        $libraryDocument->setAttribute('name', 'Library 3 updated');
        static::getDatabase()->updateDocument('library', 'library3', $libraryDocument);
        $libraryDocument = static::getDatabase()->getDocument('library', 'library3');
        $this->assertEquals('Library 3 updated', $libraryDocument['name']);

        $person3 = static::getDatabase()->getDocument('person', 'person3');
        // Todo: This is failing
        $this->assertEquals($libraryDocument['name'], $person3['library']['name']);
        $this->assertEquals('library3', $person3->getAttribute('library')['$id']);

        // One to one can't relate to multiple documents, unique index throws duplicate
        try {
            static::getDatabase()->updateDocument(
                'person',
                $person1->getId(),
                $person1->setAttribute('library', 'library2')
            );
            $this->fail('Failed to throw duplicate exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Create new document
        $library4 = static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Library 4',
            'area' => 'Area 4',
        ]));

        // Relate existing document to new document
        static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('library', 'library4')
        );

        // Relate existing document to new document as nested data
        static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('library', $library4)
        );

        // Rename relationship key
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'library',
            newKey: 'newLibrary'
        );

        // Get document with again
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary');
        $this->assertEquals('library4', $library['$id']);

        // Create person with no relationship
        static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 4',
        ]));

        // Can delete parent document with no relation with on delete set to restrict
        $deleted = static::getDatabase()->deleteDocument('person', 'person4');
        $this->assertEquals(true, $deleted);

        $person4 = static::getDatabase()->getDocument('person', 'person4');
        $this->assertEquals(true, $person4->isEmpty());

        // Cannot delete document while still related to another with on delete set to restrict
        try {
            static::getDatabase()->deleteDocument('person', 'person1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Can delete child document while still related to another with on delete set to restrict
        $person5 = static::getDatabase()->createDocument('person', new Document([
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
        $deleted = static::getDatabase()->deleteDocument('library', 'library5');
        $this->assertEquals(true, $deleted);
        $person5 = static::getDatabase()->getDocument('person', 'person5');
        $this->assertEquals(null, $person5->getAttribute('newLibrary'));

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'newLibrary',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, no effect on children for one-way
        static::getDatabase()->deleteDocument('person', 'person1');

        // Delete child, set parent relating attribute to null for one-way
        static::getDatabase()->deleteDocument('library', 'library2');

        // Check relation was set to null
        $person2 = static::getDatabase()->getDocument('person', 'person2');
        $this->assertEquals(null, $person2->getAttribute('newLibrary', ''));

        // Relate to another document
        static::getDatabase()->updateDocument(
            'person',
            $person2->getId(),
            $person2->setAttribute('newLibrary', 'library4')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'newLibrary',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('person', 'person2');

        // Check parent and child were deleted
        $person = static::getDatabase()->getDocument('person', 'person2');
        $this->assertEquals(true, $person->isEmpty());

        $library = static::getDatabase()->getDocument('library', 'library4');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'person',
            'newLibrary'
        );

        // Check parent doesn't have relationship anymore
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary', '');
        $this->assertEquals(null, $library);
    }

    /**
     * @throws AuthorizationException
     * @throws LimitException
     * @throws DuplicateException
     * @throws StructureException
     * @throws \Throwable
     */
    public function testOneToOneTwoWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('country');
        static::getDatabase()->createCollection('city');

        static::getDatabase()->createAttribute('country', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('city', 'code', Database::VAR_STRING, 3, true);
        static::getDatabase()->createAttribute('city', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'country',
            relatedCollection: 'city',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $collection = static::getDatabase()->getCollection('country');
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

        $collection = static::getDatabase()->getCollection('city');
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

        static::getDatabase()->createDocument('country', new Document($doc->getArrayCopy()));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('London', $country1->getAttribute('city')->getAttribute('name'));

        // Update a document with non existing related document. It should not get added to the list.
        static::getDatabase()->updateDocument('country', 'country1', (new Document($doc->getArrayCopy()))->setAttribute('city', 'no-city'));

        $country1Document = static::getDatabase()->getDocument('country', 'country1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $country1Document->getAttribute('city'));
        static::getDatabase()->updateDocument('country', 'country1', (new Document($doc->getArrayCopy()))->setAttribute('city', 'city1'));
        try {
            static::getDatabase()->deleteDocument('country', 'country1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(RestrictedException::class, $e);
        }

        $this->assertTrue(static::getDatabase()->deleteDocument('city', 'city1'));

        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertTrue($city1->isEmpty());

        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertTrue($country1->getAttribute('city')->isEmpty());

        $this->assertTrue(static::getDatabase()->deleteDocument('country', 'country1'));

        static::getDatabase()->createDocument('country', new Document($doc->getArrayCopy()));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('London', $country1->getAttribute('city')->getAttribute('name'));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Paris',
            'code' => 'PAR',
        ]));
        static::getDatabase()->createDocument('country', new Document([
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
        static::getDatabase()->createDocument('city', new Document([
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
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Australia',
        ]));
        static::getDatabase()->createDocument('city', new Document([
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
        $city = static::getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('country');
        $this->assertEquals('country1', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city2');
        $country = $city->getAttribute('country');
        $this->assertEquals('country2', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city3');
        $country = $city->getAttribute('country');
        $this->assertEquals('country3', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city4');
        $country = $city->getAttribute('country');
        $this->assertEquals('country4', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        // Get inverse document with relationship
        $country = static::getDatabase()->getDocument('country', 'country1');
        $city = $country->getAttribute('city');
        $this->assertEquals('city1', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country2');
        $city = $country->getAttribute('city');
        $this->assertEquals('city2', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country3');
        $city = $country->getAttribute('city');
        $this->assertEquals('city3', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country4');
        $city = $country->getAttribute('city');
        $this->assertEquals('city4', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $countries = static::getDatabase()->find('country');

        $this->assertEquals(4, \count($countries));

        // Select related document attributes
        $country = static::getDatabase()->findOne('country', [
            Query::select(['*', 'city.name'])
        ]);

        if ($country->isEmpty()) {
            throw new Exception('Country not found');
        }

        $this->assertEquals('London', $country->getAttribute('city')->getAttribute('name'));
        $this->assertArrayNotHasKey('code', $country->getAttribute('city'));

        $country = static::getDatabase()->getDocument('country', 'country1', [
            Query::select(['*', 'city.name'])
        ]);

        $this->assertEquals('London', $country->getAttribute('city')->getAttribute('name'));
        $this->assertArrayNotHasKey('code', $country->getAttribute('city'));

        $country1 = static::getDatabase()->getDocument('country', 'country1');

        // Update root document attribute without altering relationship
        $country1 = static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('name', 'Country 1 Updated')
        );

        $this->assertEquals('Country 1 Updated', $country1->getAttribute('name'));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('Country 1 Updated', $country1->getAttribute('name'));

        $city2 = static::getDatabase()->getDocument('city', 'city2');

        // Update inverse root document attribute without altering relationship
        $city2 = static::getDatabase()->updateDocument(
            'city',
            $city2->getId(),
            $city2->setAttribute('name', 'City 2 Updated')
        );

        $this->assertEquals('City 2 Updated', $city2->getAttribute('name'));
        $city2 = static::getDatabase()->getDocument('city', 'city2');
        $this->assertEquals('City 2 Updated', $city2->getAttribute('name'));

        // Update nested document attribute
        $country1 = static::getDatabase()->updateDocument(
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
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('City 1 Updated', $country1->getAttribute('city')->getAttribute('name'));

        // Update inverse nested document attribute
        $city2 = static::getDatabase()->updateDocument(
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
        $city2 = static::getDatabase()->getDocument('city', 'city2');
        $this->assertEquals('Country 2 Updated', $city2->getAttribute('country')->getAttribute('name'));

        // Create new document with no relationship
        $country5 = static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Country 5',
        ]));

        // Update to relate to created document
        $country5 = static::getDatabase()->updateDocument(
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
        $country5 = static::getDatabase()->getDocument('country', 'country5');
        $this->assertEquals('city5', $country5->getAttribute('city')['$id']);

        // Create new document with no relationship
        $city6 = static::getDatabase()->createDocument('city', new Document([
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
        $city6 = static::getDatabase()->updateDocument(
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
        $city6 = static::getDatabase()->getDocument('city', 'city6');
        $this->assertEquals('country6', $city6->getAttribute('country')['$id']);

        // One to one can't relate to multiple documents, unique index throws duplicate
        try {
            static::getDatabase()->updateDocument(
                'country',
                $country1->getId(),
                $country1->setAttribute('city', 'city2')
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $city1 = static::getDatabase()->getDocument('city', 'city1');

        // Set relationship to null
        $city1 = static::getDatabase()->updateDocument(
            'city',
            $city1->getId(),
            $city1->setAttribute('country', null)
        );

        $this->assertEquals(null, $city1->getAttribute('country'));
        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertEquals(null, $city1->getAttribute('country'));

        // Create a new city with no relation
        $city7 = static::getDatabase()->createDocument('city', new Document([
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
        static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('city', 'city7')
        );

        // Relate existing document to new document as nested data
        static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('city', $city7)
        );

        // Create a new country with no relation
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Denmark'
        ]));

        // Update inverse document with new related document
        static::getDatabase()->updateDocument(
            'city',
            $city1->getId(),
            $city1->setAttribute('country', 'country7')
        );

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'country',
            'city',
            'newCity',
            'newCountry'
        );

        // Get document with new relationship key
        $city = static::getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals('country7', $country['$id']);

        // Get inverse document with new relationship key
        $country = static::getDatabase()->getDocument('country', 'country7');
        $city = $country->getAttribute('newCity');
        $this->assertEquals('city1', $city['$id']);

        // Create a new country with no relation
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country8',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Denmark'
        ]));

        // Can delete parent document with no relation with on delete set to restrict
        $deleted = static::getDatabase()->deleteDocument('country', 'country8');
        $this->assertEquals(1, $deleted);

        $country8 = static::getDatabase()->getDocument('country', 'country8');
        $this->assertEquals(true, $country8->isEmpty());


        // Cannot delete document while still related to another with on delete set to restrict
        try {
            static::getDatabase()->deleteDocument('country', 'country1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'country',
            id: 'newCity',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        static::getDatabase()->updateDocument('city', 'city1', new Document(['newCountry' => null, '$id' => 'city1']));
        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertNull($city1->getAttribute('newCountry'));

        // Check Delete TwoWay TRUE && RELATION_MUTATE_SET_NULL && related value NULL
        $this->assertTrue(static::getDatabase()->deleteDocument('city', 'city1'));
        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertTrue($city1->isEmpty());

        // Delete parent, will set child relationship to null for two-way
        static::getDatabase()->deleteDocument('country', 'country1');

        // Check relation was set to null
        $city7 = static::getDatabase()->getDocument('city', 'city7');
        $this->assertEquals(null, $city7->getAttribute('country', ''));

        // Delete child, set parent relationship to null for two-way
        static::getDatabase()->deleteDocument('city', 'city2');

        // Check relation was set to null
        $country2 = static::getDatabase()->getDocument('country', 'country2');
        $this->assertEquals(null, $country2->getAttribute('city', ''));

        // Relate again
        static::getDatabase()->updateDocument(
            'city',
            $city7->getId(),
            $city7->setAttribute('newCountry', 'country2')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'country',
            id: 'newCity',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('country', 'country7');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('country', 'country7');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('city', 'city1');
        $this->assertEquals(true, $library->isEmpty());

        // Delete child, will delete parent for two-way
        static::getDatabase()->deleteDocument('city', 'city7');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('city', 'city7');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('country', 'country2');
        $this->assertEquals(true, $library->isEmpty());

        // Create new document to check after deleting relationship
        static::getDatabase()->createDocument('city', new Document([
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
        static::getDatabase()->deleteRelationship(
            'country',
            'newCity'
        );

        // Try to get document again
        $country = static::getDatabase()->getDocument('country', 'country4');
        $city = $country->getAttribute('newCity');
        $this->assertEquals(null, $city);

        // Try to get inverse document again
        $city = static::getDatabase()->getDocument('city', 'city7');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals(null, $country);
    }

    public function testIdenticalTwoWayKeyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('parent');
        static::getDatabase()->createCollection('child');

        static::getDatabase()->createRelationship(
            collection: 'parent',
            relatedCollection: 'child',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'child1'
        );

        try {
            static::getDatabase()->createRelationship(
                collection: 'parent',
                relatedCollection: 'child',
                type: Database::RELATION_ONE_TO_MANY,
                id: 'children',
            );
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Related attribute already exists', $e->getMessage());
        }

        static::getDatabase()->createRelationship(
            collection: 'parent',
            relatedCollection: 'child',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'children',
            twoWayKey: 'parent_id'
        );

        $collection = static::getDatabase()->getCollection('parent');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'child1') {
                $this->assertEquals('parent', $attribute['options']['twoWayKey']);
            }

            if ($attribute['key'] === 'children') {
                $this->assertEquals('parent_id', $attribute['options']['twoWayKey']);
            }
        }

        static::getDatabase()->createDocument('parent', new Document([
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

        $documents = static::getDatabase()->find('parent', []);
        $document  = array_pop($documents);
        $this->assertArrayHasKey('child1', $document);
        $this->assertEquals('foo', $document->getAttribute('child1')->getId());
        $this->assertArrayHasKey('children', $document);
        $this->assertEquals('bar', $document->getAttribute('children')[0]->getId());

        try {
            static::getDatabase()->updateRelationship(
                collection: 'parent',
                id: 'children',
                newKey: 'child1'
            );
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Relationship already exists', $e->getMessage());
        }

        try {
            static::getDatabase()->updateRelationship(
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('artist');
        static::getDatabase()->createCollection('album');

        static::getDatabase()->createAttribute('artist', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('album', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('album', 'price', Database::VAR_FLOAT, 0, true);

        static::getDatabase()->createRelationship(
            collection: 'artist',
            relatedCollection: 'album',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'albums'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('artist');
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
        $artist1 = static::getDatabase()->createDocument('artist', new Document([
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
        static::getDatabase()->updateDocument('artist', 'artist1', $artist1->setAttribute('albums', ['album1', 'no-album']));

        $artist1Document = static::getDatabase()->getDocument('artist', 'artist1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($artist1Document->getAttribute('albums')));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('album', new Document([
            '$id' => 'album2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Album 2',
            'price' => 19.99,
        ]));
        static::getDatabase()->createDocument('artist', new Document([
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

        $documents = static::getDatabase()->find('artist', [
            Query::select(['name']),
            Query::limit(1)
        ]);
        $this->assertArrayNotHasKey('albums', $documents[0]);

        // Get document with relationship
        $artist = static::getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album1', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);

        $artist = static::getDatabase()->getDocument('artist', 'artist2');
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album2', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);
        $this->assertEquals('album33', $albums[1]['$id']);
        $this->assertCount(2, $albums);

        // Get related document
        $album = static::getDatabase()->getDocument('album', 'album1');
        $this->assertArrayNotHasKey('artist', $album);

        $album = static::getDatabase()->getDocument('album', 'album2');
        $this->assertArrayNotHasKey('artist', $album);

        $artists = static::getDatabase()->find('artist');

        $this->assertEquals(2, \count($artists));

        // Select related document attributes
        $artist = static::getDatabase()->findOne('artist', [
            Query::select(['*', 'albums.name'])
        ]);

        if ($artist->isEmpty()) {
            $this->fail('Artist not found');
        }

        $this->assertEquals('Album 1', $artist->getAttribute('albums')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('price', $artist->getAttribute('albums')[0]);

        $artist = static::getDatabase()->getDocument('artist', 'artist1', [
            Query::select(['*', 'albums.name'])
        ]);

        $this->assertEquals('Album 1', $artist->getAttribute('albums')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('price', $artist->getAttribute('albums')[0]);

        // Update root document attribute without altering relationship
        $artist1 = static::getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('name', 'Artist 1 Updated')
        );

        $this->assertEquals('Artist 1 Updated', $artist1->getAttribute('name'));
        $artist1 = static::getDatabase()->getDocument('artist', 'artist1');
        $this->assertEquals('Artist 1 Updated', $artist1->getAttribute('name'));

        // Update nested document attribute
        $albums = $artist1->getAttribute('albums', []);
        $albums[0]->setAttribute('name', 'Album 1 Updated');

        $artist1 = static::getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', $albums)
        );

        $this->assertEquals('Album 1 Updated', $artist1->getAttribute('albums')[0]->getAttribute('name'));
        $artist1 = static::getDatabase()->getDocument('artist', 'artist1');
        $this->assertEquals('Album 1 Updated', $artist1->getAttribute('albums')[0]->getAttribute('name'));

        $albumId = $artist1->getAttribute('albums')[0]->getAttribute('$id');
        $albumDocument = static::getDatabase()->getDocument('album', $albumId);
        $albumDocument->setAttribute('name', 'Album 1 Updated!!!');
        static::getDatabase()->updateDocument('album', $albumDocument->getId(), $albumDocument);
        $albumDocument = static::getDatabase()->getDocument('album', $albumDocument->getId());
        $artist1 = static::getDatabase()->getDocument('artist', $artist1->getId());

        $this->assertEquals('Album 1 Updated!!!', $albumDocument['name']);
        $this->assertEquals($albumDocument->getId(), $artist1->getAttribute('albums')[0]->getId());
        $this->assertEquals($albumDocument->getAttribute('name'), $artist1->getAttribute('albums')[0]->getAttribute('name'));

        // Create new document with no relationship
        $artist3 = static::getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 3',
        ]));

        // Update to relate to created document
        $artist3 = static::getDatabase()->updateDocument(
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
        $artist3 = static::getDatabase()->getDocument('artist', 'artist3');
        $this->assertEquals('Album 3', $artist3->getAttribute('albums')[0]->getAttribute('name'));

        // Update document with new related documents, will remove existing relations
        static::getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', ['album2'])
        );

        // Update document with new related documents, will remove existing relations
        static::getDatabase()->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', ['album1', 'album2'])
        );

        // Rename relationship key
        static::getDatabase()->updateRelationship(
            'artist',
            'albums',
            'newAlbums'
        );

        // Get document with new relationship key
        $artist = static::getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('newAlbums');
        $this->assertEquals('album1', $albums[0]['$id']);

        // Create new document with no relationship
        static::getDatabase()->createDocument('artist', new Document([
            '$id' => 'artist4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 4',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('artist', 'artist4');
        $this->assertEquals(true, $deleted);

        $artist4 = static::getDatabase()->getDocument('artist', 'artist4');
        $this->assertEquals(true, $artist4->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('artist', 'artist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'artist',
            id: 'newAlbums',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, set child relationship to null
        static::getDatabase()->deleteDocument('artist', 'artist1');

        // Check relation was set to null
        $album2 = static::getDatabase()->getDocument('album', 'album2');
        $this->assertEquals(null, $album2->getAttribute('artist', ''));

        // Relate again
        static::getDatabase()->updateDocument(
            'album',
            $album2->getId(),
            $album2->setAttribute('artist', 'artist2')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'artist',
            id: 'newAlbums',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('artist', 'artist2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('artist', 'artist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('album', 'album2');
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

        $artist = static::getDatabase()->createDocument('artist', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 100',
            'newAlbums' => $albums
        ]));

        $artist = static::getDatabase()->getDocument('artist', $artist->getId());
        $this->assertCount(50, $artist->getAttribute('newAlbums'));

        $albums = static::getDatabase()->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999)
        ]);

        $this->assertCount(50, $albums);

        $count = static::getDatabase()->count('album', [
            Query::equal('artist', [$artist->getId()]),
        ]);

        $this->assertEquals(50, $count);

        static::getDatabase()->deleteDocument('album', 'album_1');
        $artist = static::getDatabase()->getDocument('artist', $artist->getId());
        $this->assertCount(49, $artist->getAttribute('newAlbums'));

        static::getDatabase()->deleteDocument('artist', $artist->getId());

        $albums = static::getDatabase()->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999)
        ]);

        $this->assertCount(0, $albums);

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'artist',
            'newAlbums'
        );

        // Try to get document again
        $artist = static::getDatabase()->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('newAlbums', '');
        $this->assertEquals(null, $albums);
    }

    public function testOneToManyTwoWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('customer');
        static::getDatabase()->createCollection('account');

        static::getDatabase()->createAttribute('customer', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('account', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('account', 'number', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'customer',
            relatedCollection: 'account',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'accounts'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('customer');
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
        $collection = static::getDatabase()->getCollection('account');
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
        $customer1 = static::getDatabase()->createDocument('customer', new Document([
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
        static::getDatabase()->updateDocument('customer', 'customer1', $customer1->setAttribute('accounts', ['account1','no-account']));

        $customer1Document = static::getDatabase()->getDocument('customer', 'customer1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($customer1Document->getAttribute('accounts')));

        // Create document with relationship with related ID
        $account2 = static::getDatabase()->createDocument('account', new Document([
            '$id' => 'account2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 2',
            'number' => '987654321',
        ]));
        static::getDatabase()->createDocument('customer', new Document([
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
        static::getDatabase()->createDocument('account', new Document([
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
        static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 4',
        ]));
        static::getDatabase()->createDocument('account', new Document([
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
        $customer = static::getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account1', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = static::getDatabase()->getDocument('customer', 'customer2');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account2', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = static::getDatabase()->getDocument('customer', 'customer3');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account3', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = static::getDatabase()->getDocument('customer', 'customer4');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account4', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        // Get related documents
        $account = static::getDatabase()->getDocument('account', 'account1');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer1', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = static::getDatabase()->getDocument('account', 'account2');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer2', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = static::getDatabase()->getDocument('account', 'account3');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer3', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = static::getDatabase()->getDocument('account', 'account4');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer4', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $customers = static::getDatabase()->find('customer');

        $this->assertEquals(4, \count($customers));

        // Select related document attributes
        $customer = static::getDatabase()->findOne('customer', [
            Query::select(['*', 'accounts.name'])
        ]);

        if ($customer->isEmpty()) {
            throw new Exception('Customer not found');
        }

        $this->assertEquals('Account 1', $customer->getAttribute('accounts')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $customer->getAttribute('accounts')[0]);

        $customer = static::getDatabase()->getDocument('customer', 'customer1', [
            Query::select(['*', 'accounts.name'])
        ]);

        $this->assertEquals('Account 1', $customer->getAttribute('accounts')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $customer->getAttribute('accounts')[0]);

        // Update root document attribute without altering relationship
        $customer1 = static::getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('name', 'Customer 1 Updated')
        );

        $this->assertEquals('Customer 1 Updated', $customer1->getAttribute('name'));
        $customer1 = static::getDatabase()->getDocument('customer', 'customer1');
        $this->assertEquals('Customer 1 Updated', $customer1->getAttribute('name'));

        $account2 = static::getDatabase()->getDocument('account', 'account2');

        // Update inverse root document attribute without altering relationship
        $account2 = static::getDatabase()->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute('name', 'Account 2 Updated')
        );

        $this->assertEquals('Account 2 Updated', $account2->getAttribute('name'));
        $account2 = static::getDatabase()->getDocument('account', 'account2');
        $this->assertEquals('Account 2 Updated', $account2->getAttribute('name'));

        // Update nested document attribute
        $accounts = $customer1->getAttribute('accounts', []);
        $accounts[0]->setAttribute('name', 'Account 1 Updated');

        $customer1 = static::getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', $accounts)
        );

        $this->assertEquals('Account 1 Updated', $customer1->getAttribute('accounts')[0]->getAttribute('name'));
        $customer1 = static::getDatabase()->getDocument('customer', 'customer1');
        $this->assertEquals('Account 1 Updated', $customer1->getAttribute('accounts')[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $account2 = static::getDatabase()->updateDocument(
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
        $account2 = static::getDatabase()->getDocument('account', 'account2');
        $this->assertEquals('Customer 2 Updated', $account2->getAttribute('customer')->getAttribute('name'));

        // Create new document with no relationship
        $customer5 = static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 5',
        ]));

        // Update to relate to created document
        $customer5 = static::getDatabase()->updateDocument(
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
        $customer5 = static::getDatabase()->getDocument('customer', 'customer5');
        $this->assertEquals('Account 5', $customer5->getAttribute('accounts')[0]->getAttribute('name'));

        // Create new child document with no relationship
        $account6 = static::getDatabase()->createDocument('account', new Document([
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
        $account6 = static::getDatabase()->updateDocument(
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
        $account6 = static::getDatabase()->getDocument('account', 'account6');
        $this->assertEquals('Customer 6', $account6->getAttribute('customer')->getAttribute('name'));

        // Update document with new related document, will remove existing relations
        static::getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', ['account2'])
        );

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', ['account1', 'account2'])
        );

        // Update inverse document
        static::getDatabase()->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute('customer', 'customer2')
        );

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'customer',
            'accounts',
            'newAccounts',
            'newCustomer'
        );

        // Get document with new relationship key
        $customer = static::getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('newAccounts');
        $this->assertEquals('account1', $accounts[0]['$id']);

        // Get inverse document with new relationship key
        $account = static::getDatabase()->getDocument('account', 'account1');
        $customer = $account->getAttribute('newCustomer');
        $this->assertEquals('customer1', $customer['$id']);

        // Create new document with no relationship
        static::getDatabase()->createDocument('customer', new Document([
            '$id' => 'customer7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('customer', 'customer7');
        $this->assertEquals(true, $deleted);

        $customer7 = static::getDatabase()->getDocument('customer', 'customer7');
        $this->assertEquals(true, $customer7->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('customer', 'customer1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'customer',
            id: 'newAccounts',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, set child relationship to null
        static::getDatabase()->deleteDocument('customer', 'customer1');

        // Check relation was set to null
        $account1 = static::getDatabase()->getDocument('account', 'account1');
        $this->assertEquals(null, $account2->getAttribute('newCustomer', ''));

        // Relate again
        static::getDatabase()->updateDocument(
            'account',
            $account1->getId(),
            $account1->setAttribute('newCustomer', 'customer2')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'customer',
            id: 'newAccounts',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('customer', 'customer2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('customer', 'customer2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('account', 'account2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'customer',
            'newAccounts'
        );

        // Try to get document again
        $customer = static::getDatabase()->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('newAccounts');
        $this->assertEquals(null, $accounts);

        // Try to get inverse document again
        $accounts = static::getDatabase()->getDocument('account', 'account1');
        $customer = $accounts->getAttribute('newCustomer');
        $this->assertEquals(null, $customer);
    }

    public function testManyToOneOneWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('review');
        static::getDatabase()->createCollection('movie');

        static::getDatabase()->createAttribute('review', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('movie', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('movie', 'length', Database::VAR_INTEGER, 0, true, formatOptions: ['min' => 0, 'max' => 999]);
        static::getDatabase()->createAttribute('movie', 'date', Database::VAR_DATETIME, 0, false, filters: ['datetime']);
        static::getDatabase()->createAttribute('review', 'date', Database::VAR_DATETIME, 0, false, filters: ['datetime']);
        static::getDatabase()->createRelationship(
            collection: 'review',
            relatedCollection: 'movie',
            type: Database::RELATION_MANY_TO_ONE,
            twoWayKey: 'reviews'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('review');
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
        $collection = static::getDatabase()->getCollection('movie');
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
        $review1 = static::getDatabase()->createDocument('review', new Document([
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
        static::getDatabase()->updateDocument('review', 'review1', $review1->setAttribute('movie', 'no-movie'));

        $review1Document = static::getDatabase()->getDocument('review', 'review1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $review1Document->getAttribute('movie'));

        static::getDatabase()->updateDocument('review', 'review1', $review1->setAttribute('movie', 'movie1'));

        // Create document with relationship to existing document by ID
        $review10 = static::getDatabase()->createDocument('review', new Document([
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
        static::getDatabase()->createDocument('movie', new Document([
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
        static::getDatabase()->createDocument('review', new Document([
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
        $review = static::getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('movie', []);
        $this->assertEquals('movie1', $movie['$id']);
        $this->assertArrayNotHasKey('reviews', $movie);

        $documents = static::getDatabase()->find('review', [
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

        $review = static::getDatabase()->getDocument('review', 'review2');
        $movie = $review->getAttribute('movie', []);
        $this->assertEquals('movie2', $movie['$id']);
        $this->assertArrayNotHasKey('reviews', $movie);

        // Get related document
        $movie = static::getDatabase()->getDocument('movie', 'movie1');
        $this->assertArrayNotHasKey('reviews', $movie);

        $movie = static::getDatabase()->getDocument('movie', 'movie2');
        $this->assertArrayNotHasKey('reviews', $movie);

        $reviews = static::getDatabase()->find('review');

        $this->assertEquals(3, \count($reviews));

        // Select related document attributes
        $review = static::getDatabase()->findOne('review', [
            Query::select(['*', 'movie.name'])
        ]);

        if ($review->isEmpty()) {
            throw new Exception('Review not found');
        }

        $this->assertEquals('Movie 1', $review->getAttribute('movie')->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $review->getAttribute('movie'));

        $review = static::getDatabase()->getDocument('review', 'review1', [
            Query::select(['*', 'movie.name'])
        ]);

        $this->assertEquals('Movie 1', $review->getAttribute('movie')->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $review->getAttribute('movie'));

        // Update root document attribute without altering relationship
        $review1 = static::getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('name', 'Review 1 Updated')
        );

        $this->assertEquals('Review 1 Updated', $review1->getAttribute('name'));
        $review1 = static::getDatabase()->getDocument('review', 'review1');
        $this->assertEquals('Review 1 Updated', $review1->getAttribute('name'));

        // Update nested document attribute
        $movie = $review1->getAttribute('movie');
        $movie->setAttribute('name', 'Movie 1 Updated');

        $review1 = static::getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('movie', $movie)
        );

        $this->assertEquals('Movie 1 Updated', $review1->getAttribute('movie')->getAttribute('name'));
        $review1 = static::getDatabase()->getDocument('review', 'review1');
        $this->assertEquals('Movie 1 Updated', $review1->getAttribute('movie')->getAttribute('name'));

        // Create new document with no relationship
        $review5 = static::getDatabase()->createDocument('review', new Document([
            '$id' => 'review5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Review 5',
        ]));

        // Update to relate to created document
        $review5 = static::getDatabase()->updateDocument(
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
        $review5 = static::getDatabase()->getDocument('review', 'review5');
        $this->assertEquals('Movie 5', $review5->getAttribute('movie')->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('movie', 'movie2')
        );

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'review',
            'movie',
            'newMovie',
        );

        // Get document with new relationship key
        $review = static::getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('newMovie');
        $this->assertEquals('movie2', $movie['$id']);

        // Reset values
        $review1 = static::getDatabase()->getDocument('review', 'review1');

        static::getDatabase()->updateDocument(
            'review',
            $review1->getId(),
            $review1->setAttribute('newMovie', 'movie1')
        );

        // Create new document with no relationship
        static::getDatabase()->createDocument('movie', new Document([
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
        $deleted = static::getDatabase()->deleteDocument('movie', 'movie3');
        $this->assertEquals(true, $deleted);

        $movie3 = static::getDatabase()->getDocument('movie', 'movie3');
        $this->assertEquals(true, $movie3->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('movie', 'movie1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'review',
            id: 'newMovie',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete child, set parent relationship to null
        static::getDatabase()->deleteDocument('movie', 'movie1');

        // Check relation was set to null
        $review1 = static::getDatabase()->getDocument('review', 'review1');
        $this->assertEquals(null, $review1->getAttribute('newMovie'));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'review',
            id: 'newMovie',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete child, will delete parent
        static::getDatabase()->deleteDocument('movie', 'movie2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('movie', 'movie2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('review', 'review2');
        $this->assertEquals(true, $library->isEmpty());


        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'review',
            'newMovie'
        );

        // Try to get document again
        $review = static::getDatabase()->getDocument('review', 'review1');
        $movie = $review->getAttribute('newMovie');
        $this->assertEquals(null, $movie);
    }

    public function testManyToOneTwoWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('product');
        static::getDatabase()->createCollection('store');

        static::getDatabase()->createAttribute('store', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('store', 'opensAt', Database::VAR_STRING, 5, true);

        static::getDatabase()->createAttribute(
            collection: 'product',
            id: 'name',
            type: Database::VAR_STRING,
            size: 255,
            required: true
        );

        static::getDatabase()->createRelationship(
            collection: 'product',
            relatedCollection: 'store',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            twoWayKey: 'products'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('product');
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
        $collection = static::getDatabase()->getCollection('store');
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
        $product1 = static::getDatabase()->createDocument('product', new Document([
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
        static::getDatabase()->updateDocument('product', 'product1', $product1->setAttribute('store', 'no-store'));

        $product1Document = static::getDatabase()->getDocument('product', 'product1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $product1Document->getAttribute('store'));

        static::getDatabase()->updateDocument('product', 'product1', $product1->setAttribute('store', 'store1'));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('store', new Document([
            '$id' => 'store2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Store 2',
            'opensAt' => '09:30',
        ]));
        static::getDatabase()->createDocument('product', new Document([
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
        static::getDatabase()->createDocument('store', new Document([
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

        static::getDatabase()->createDocument('product', new Document([
            '$id' => 'product4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 4',
        ]));
        static::getDatabase()->createDocument('store', new Document([
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
        $product = static::getDatabase()->getDocument('product', 'product1');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store1', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = static::getDatabase()->getDocument('product', 'product2');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store2', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = static::getDatabase()->getDocument('product', 'product3');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store3', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        $product = static::getDatabase()->getDocument('product', 'product4');
        $store = $product->getAttribute('store', []);
        $this->assertEquals('store4', $store['$id']);
        $this->assertArrayNotHasKey('products', $store);

        // Get related document
        $store = static::getDatabase()->getDocument('store', 'store1');
        $products = $store->getAttribute('products');
        $this->assertEquals('product1', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = static::getDatabase()->getDocument('store', 'store2');
        $products = $store->getAttribute('products');
        $this->assertEquals('product2', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = static::getDatabase()->getDocument('store', 'store3');
        $products = $store->getAttribute('products');
        $this->assertEquals('product3', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $store = static::getDatabase()->getDocument('store', 'store4');
        $products = $store->getAttribute('products');
        $this->assertEquals('product4', $products[0]['$id']);
        $this->assertArrayNotHasKey('store', $products[0]);

        $products = static::getDatabase()->find('product');

        $this->assertEquals(4, \count($products));

        // Select related document attributes
        $product = static::getDatabase()->findOne('product', [
            Query::select(['*', 'store.name'])
        ]);

        if ($product->isEmpty()) {
            throw new Exception('Product not found');
        }

        $this->assertEquals('Store 1', $product->getAttribute('store')->getAttribute('name'));
        $this->assertArrayNotHasKey('opensAt', $product->getAttribute('store'));

        $product = static::getDatabase()->getDocument('product', 'product1', [
            Query::select(['*', 'store.name'])
        ]);

        $this->assertEquals('Store 1', $product->getAttribute('store')->getAttribute('name'));
        $this->assertArrayNotHasKey('opensAt', $product->getAttribute('store'));

        // Update root document attribute without altering relationship
        $product1 = static::getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('name', 'Product 1 Updated')
        );

        $this->assertEquals('Product 1 Updated', $product1->getAttribute('name'));
        $product1 = static::getDatabase()->getDocument('product', 'product1');
        $this->assertEquals('Product 1 Updated', $product1->getAttribute('name'));

        // Update inverse document attribute without altering relationship
        $store1 = static::getDatabase()->getDocument('store', 'store1');
        $store1 = static::getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('name', 'Store 1 Updated')
        );

        $this->assertEquals('Store 1 Updated', $store1->getAttribute('name'));
        $store1 = static::getDatabase()->getDocument('store', 'store1');
        $this->assertEquals('Store 1 Updated', $store1->getAttribute('name'));

        // Update nested document attribute
        $store = $product1->getAttribute('store');
        $store->setAttribute('name', 'Store 1 Updated');

        $product1 = static::getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('store', $store)
        );

        $this->assertEquals('Store 1 Updated', $product1->getAttribute('store')->getAttribute('name'));
        $product1 = static::getDatabase()->getDocument('product', 'product1');
        $this->assertEquals('Store 1 Updated', $product1->getAttribute('store')->getAttribute('name'));

        // Update inverse nested document attribute
        $product = $store1->getAttribute('products')[0];
        $product->setAttribute('name', 'Product 1 Updated');

        $store1 = static::getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('products', [$product])
        );

        $this->assertEquals('Product 1 Updated', $store1->getAttribute('products')[0]->getAttribute('name'));
        $store1 = static::getDatabase()->getDocument('store', 'store1');
        $this->assertEquals('Product 1 Updated', $store1->getAttribute('products')[0]->getAttribute('name'));

        // Create new document with no relationship
        $product5 = static::getDatabase()->createDocument('product', new Document([
            '$id' => 'product5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Product 5',
        ]));

        // Update to relate to created document
        $product5 = static::getDatabase()->updateDocument(
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
        $product5 = static::getDatabase()->getDocument('product', 'product5');
        $this->assertEquals('Store 5', $product5->getAttribute('store')->getAttribute('name'));

        // Create new child document with no relationship
        $store6 = static::getDatabase()->createDocument('store', new Document([
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
        $store6 = static::getDatabase()->updateDocument(
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
        $store6 = static::getDatabase()->getDocument('store', 'store6');
        $this->assertEquals('Product 6', $store6->getAttribute('products')[0]->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'product',
            $product1->getId(),
            $product1->setAttribute('store', 'store2')
        );

        $store1 = static::getDatabase()->getDocument('store', 'store1');

        // Update inverse document
        static::getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('products', ['product1'])
        );

        $store2 = static::getDatabase()->getDocument('store', 'store2');

        // Update inverse document
        static::getDatabase()->updateDocument(
            'store',
            $store2->getId(),
            $store2->setAttribute('products', ['product1', 'product2'])
        );

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'product',
            'store',
            'newStore',
            'newProducts'
        );

        // Get document with new relationship key
        $store = static::getDatabase()->getDocument('store', 'store2');
        $products = $store->getAttribute('newProducts');
        $this->assertEquals('product1', $products[0]['$id']);

        // Get inverse document with new relationship key
        $product = static::getDatabase()->getDocument('product', 'product1');
        $store = $product->getAttribute('newStore');
        $this->assertEquals('store2', $store['$id']);

        // Reset relationships
        $store1 = static::getDatabase()->getDocument('store', 'store1');
        static::getDatabase()->updateDocument(
            'store',
            $store1->getId(),
            $store1->setAttribute('newProducts', ['product1'])
        );

        // Create new document with no relationship
        static::getDatabase()->createDocument('store', new Document([
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
        $deleted = static::getDatabase()->deleteDocument('store', 'store7');
        $this->assertEquals(true, $deleted);

        $store7 = static::getDatabase()->getDocument('store', 'store7');
        $this->assertEquals(true, $store7->isEmpty());

        // Try to delete child while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('store', 'store1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Delete parent while still related to another with on delete: restrict
        $result = static::getDatabase()->deleteDocument('product', 'product5');
        $this->assertEquals(true, $result);

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'product',
            id: 'newStore',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete child, set parent relationship to null
        static::getDatabase()->deleteDocument('store', 'store1');

        // Check relation was set to null
        static::getDatabase()->getDocument('product', 'product1');
        $this->assertEquals(null, $product1->getAttribute('newStore'));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'product',
            id: 'newStore',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete child, will delete parent
        static::getDatabase()->deleteDocument('store', 'store2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('store', 'store2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('product', 'product2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'product',
            'newStore'
        );

        // Try to get document again
        $products = static::getDatabase()->getDocument('product', 'product1');
        $store = $products->getAttribute('newStore');
        $this->assertEquals(null, $store);

        // Try to get inverse document again
        $store = static::getDatabase()->getDocument('store', 'store1');
        $products = $store->getAttribute('newProducts');
        $this->assertEquals(null, $products);
    }

    public function testManyToManyOneWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('playlist');
        static::getDatabase()->createCollection('song');

        static::getDatabase()->createAttribute('playlist', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('song', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('song', 'length', Database::VAR_INTEGER, 0, true);

        static::getDatabase()->createRelationship(
            collection: 'playlist',
            relatedCollection: 'song',
            type: Database::RELATION_MANY_TO_MANY,
            id: 'songs'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('playlist');
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
        $playlist1 = static::getDatabase()->createDocument('playlist', new Document([
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
        static::getDatabase()->createDocument('song', new Document([
            '$id' => 'song2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Song 2',
            'length' => 140,
        ]));
        static::getDatabase()->createDocument('playlist', new Document([
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
        static::getDatabase()->updateDocument('playlist', 'playlist1', $playlist1->setAttribute('songs', ['song1','no-song']));

        $playlist1Document = static::getDatabase()->getDocument('playlist', 'playlist1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($playlist1Document->getAttribute('songs')));

        $documents = static::getDatabase()->find('playlist', [
            Query::select(['name']),
            Query::limit(1)
        ]);

        $this->assertArrayNotHasKey('songs', $documents[0]);

        // Get document with relationship
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song1', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        $playlist = static::getDatabase()->getDocument('playlist', 'playlist2');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song2', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        // Get related document
        $library = static::getDatabase()->getDocument('song', 'song1');
        $this->assertArrayNotHasKey('songs', $library);

        $library = static::getDatabase()->getDocument('song', 'song2');
        $this->assertArrayNotHasKey('songs', $library);

        $playlists = static::getDatabase()->find('playlist');

        $this->assertEquals(2, \count($playlists));

        // Select related document attributes
        $playlist = static::getDatabase()->findOne('playlist', [
            Query::select(['*', 'songs.name'])
        ]);

        if ($playlist->isEmpty()) {
            throw new Exception('Playlist not found');
        }

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1', [
            Query::select(['*', 'songs.name'])
        ]);

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        // Update root document attribute without altering relationship
        $playlist1 = static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('name', 'Playlist 1 Updated')
        );

        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));

        // Update nested document attribute
        $songs = $playlist1->getAttribute('songs', []);
        $songs[0]->setAttribute('name', 'Song 1 Updated');

        $playlist1 = static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', $songs)
        );

        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));

        // Create new document with no relationship
        $playlist5 = static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 5',
        ]));

        // Update to relate to created document
        $playlist5 = static::getDatabase()->updateDocument(
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
        static::getDatabase()->createDocument('playlist', new Document([
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
        $playlist5 = static::getDatabase()->getDocument('playlist', 'playlist5');
        $this->assertEquals('Song 5', $playlist5->getAttribute('songs')[0]->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', ['song2'])
        );

        // Rename relationship key
        static::getDatabase()->updateRelationship(
            'playlist',
            'songs',
            'newSongs'
        );

        // Get document with new relationship key
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals('song2', $songs[0]['$id']);

        // Create new document with no relationship
        static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 3',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('playlist', 'playlist3');
        $this->assertEquals(true, $deleted);

        $playlist3 = static::getDatabase()->getDocument('playlist', 'playlist3');
        $this->assertEquals(true, $playlist3->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('playlist', 'playlist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');

        // Reset relationships
        static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('newSongs', ['song1'])
        );

        // Delete child, will delete junction
        static::getDatabase()->deleteDocument('song', 'song1');

        // Check relation was set to null
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals(0, \count($playlist1->getAttribute('newSongs')));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('playlist', 'playlist2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('playlist', 'playlist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('song', 'song2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'playlist',
            'newSongs'
        );

        // Try to get document again
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals(null, $songs);
    }

    public function testManyToManyTwoWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('students');
        static::getDatabase()->createCollection('classes');

        static::getDatabase()->createAttribute('students', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classes', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classes', 'number', Database::VAR_INTEGER, 0, true);

        static::getDatabase()->createRelationship(
            collection: 'students',
            relatedCollection: 'classes',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('students');
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
        $collection = static::getDatabase()->getCollection('classes');
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
        $student1 = static::getDatabase()->createDocument('students', new Document([
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
        static::getDatabase()->updateDocument('students', 'student1', $student1->setAttribute('classes', ['class1', 'no-class']));

        $student1Document = static::getDatabase()->getDocument('students', 'student1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($student1Document->getAttribute('classes')));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('classes', new Document([
            '$id' => 'class2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),

            ],
            'name' => 'Class 2',
            'number' => 2,
        ]));
        static::getDatabase()->createDocument('students', new Document([
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
        static::getDatabase()->createDocument('classes', new Document([
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
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 4'
        ]));
        static::getDatabase()->createDocument('classes', new Document([
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
        $student = static::getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class1', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student2');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class2', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student3');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class3', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student4');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class4', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        // Get related document
        $class = static::getDatabase()->getDocument('classes', 'class1');
        $student = $class->getAttribute('students');
        $this->assertEquals('student1', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class2');
        $student = $class->getAttribute('students');
        $this->assertEquals('student2', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class3');
        $student = $class->getAttribute('students');
        $this->assertEquals('student3', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class4');
        $student = $class->getAttribute('students');
        $this->assertEquals('student4', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        // Select related document attributes
        $student = static::getDatabase()->findOne('students', [
            Query::select(['*', 'classes.name'])
        ]);

        if ($student->isEmpty()) {
            throw new Exception('Student not found');
        }

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        $student = static::getDatabase()->getDocument('students', 'student1', [
            Query::select(['*', 'classes.name'])
        ]);

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        // Update root document attribute without altering relationship
        $student1 = static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('name', 'Student 1 Updated')
        );

        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));

        // Update inverse root document attribute without altering relationship
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $class2 = static::getDatabase()->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('name', 'Class 2 Updated')
        );

        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));

        // Update nested document attribute
        $classes = $student1->getAttribute('classes', []);
        $classes[0]->setAttribute('name', 'Class 1 Updated');

        $student1 = static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', $classes)
        );

        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $students = $class2->getAttribute('students', []);
        $students[0]->setAttribute('name', 'Student 2 Updated');

        $class2 = static::getDatabase()->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('students', $students)
        );

        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));

        // Create new document with no relationship
        $student5 = static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 5',
        ]));

        // Update to relate to created document
        $student5 = static::getDatabase()->updateDocument(
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
        $student5 = static::getDatabase()->getDocument('students', 'student5');
        $this->assertEquals('Class 5', $student5->getAttribute('classes')[0]->getAttribute('name'));

        // Create child document with no relationship
        $class6 = static::getDatabase()->createDocument('classes', new Document([
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
        $class6 = static::getDatabase()->updateDocument(
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
        $class6 = static::getDatabase()->getDocument('classes', 'class6');
        $this->assertEquals('Student 6', $class6->getAttribute('students')[0]->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', ['class2'])
        );

        $class1 = static::getDatabase()->getDocument('classes', 'class1');

        // Update inverse document
        static::getDatabase()->updateDocument(
            'classes',
            $class1->getId(),
            $class1->setAttribute('students', ['student1'])
        );

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'students',
            'classes',
            'newClasses',
            'newStudents'
        );

        // Get document with new relationship key
        $students = static::getDatabase()->getDocument('students', 'student1');
        $classes = $students->getAttribute('newClasses');
        $this->assertEquals('class2', $classes[0]['$id']);

        // Get inverse document with new relationship key
        $class = static::getDatabase()->getDocument('classes', 'class1');
        $students = $class->getAttribute('newStudents');
        $this->assertEquals('student1', $students[0]['$id']);

        // Create new document with no relationship
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('students', 'student7');
        $this->assertEquals(true, $deleted);

        $student6 = static::getDatabase()->getDocument('students', 'student7');
        $this->assertEquals(true, $student6->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('students', 'student1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $student1 = static::getDatabase()->getDocument('students', 'student1');

        // Reset relationships
        static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('newClasses', ['class1'])
        );

        // Delete child, will delete junction
        static::getDatabase()->deleteDocument('classes', 'class1');

        // Check relation was set to null
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals(0, \count($student1->getAttribute('newClasses')));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('students', 'student2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('students', 'student2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete child, should not delete parent
        static::getDatabase()->deleteDocument('classes', 'class6');

        // Check only child was deleted
        $student6 = static::getDatabase()->getDocument('students', 'student6');
        $this->assertEquals(false, $student6->isEmpty());
        $this->assertEmpty($student6->getAttribute('newClasses'));

        $library = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'students',
            'newClasses'
        );

        // Try to get documents again
        $student = static::getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('newClasses');
        $this->assertEquals(null, $classes);

        // Try to get inverse documents again
        $classes = static::getDatabase()->getDocument('classes', 'class1');
        $students = $classes->getAttribute('newStudents');
        $this->assertEquals(null, $students);
    }

    public function testSelectRelationshipAttributes(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('make');
        static::getDatabase()->createCollection('model');

        static::getDatabase()->createAttribute('make', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('model', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('model', 'year', Database::VAR_INTEGER, 0, true);

        static::getDatabase()->createRelationship(
            collection: 'make',
            relatedCollection: 'model',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'models'
        );

        static::getDatabase()->createDocument('make', new Document([
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
        $make = static::getDatabase()->findOne('make', [
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
        $make = static::getDatabase()->findOne('make', [
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

        $make = static::getDatabase()->findOne('make', [
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

        $make = static::getDatabase()->findOne('make', [
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

        $make = static::getDatabase()->findOne('make', [
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

        $make = static::getDatabase()->findOne('make', [
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

        $make = static::getDatabase()->findOne('make', [
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
        $make = static::getDatabase()->findOne('make', [
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
        $make = static::getDatabase()->findOne('make', [
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
        $make = static::getDatabase()->findOne('make', [
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
        $make = static::getDatabase()->findOne('make', [
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('pattern');
        static::getDatabase()->createCollection('shirt');
        static::getDatabase()->createCollection('team');

        static::getDatabase()->createAttribute('pattern', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('shirt', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('team', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'pattern',
            relatedCollection: 'shirt',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'shirt',
            twoWayKey: 'pattern'
        );
        static::getDatabase()->createRelationship(
            collection: 'shirt',
            relatedCollection: 'team',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'team',
            twoWayKey: 'shirt'
        );

        static::getDatabase()->createDocument('pattern', new Document([
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

        $pattern = static::getDatabase()->getDocument('pattern', 'stripes');
        $this->assertEquals('red', $pattern['shirt']['$id']);
        $this->assertArrayNotHasKey('pattern', $pattern['shirt']);
        $this->assertEquals('reds', $pattern['shirt']['team']['$id']);
        $this->assertArrayNotHasKey('shirt', $pattern['shirt']['team']);

        static::getDatabase()->createDocument('team', new Document([
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

        $team = static::getDatabase()->getDocument('team', 'blues');
        $this->assertEquals('blue', $team['shirt']['$id']);
        $this->assertArrayNotHasKey('team', $team['shirt']);
        $this->assertEquals('plain', $team['shirt']['pattern']['$id']);
        $this->assertArrayNotHasKey('shirt', $team['shirt']['pattern']);
    }

    public function testNestedOneToOne_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('teachers');
        static::getDatabase()->createCollection('classrooms');
        static::getDatabase()->createCollection('children');

        static::getDatabase()->createAttribute('children', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('teachers', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classrooms', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'teachers',
            relatedCollection: 'classrooms',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'classroom',
            twoWayKey: 'teacher'
        );
        static::getDatabase()->createRelationship(
            collection: 'classrooms',
            relatedCollection: 'children',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'classroom'
        );

        static::getDatabase()->createDocument('teachers', new Document([
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

        $teacher1 = static::getDatabase()->getDocument('teachers', 'teacher1');
        $this->assertEquals('classroom1', $teacher1['classroom']['$id']);
        $this->assertArrayNotHasKey('teacher', $teacher1['classroom']);
        $this->assertEquals(2, \count($teacher1['classroom']['children']));
        $this->assertEquals('Child 1', $teacher1['classroom']['children'][0]['name']);
        $this->assertEquals('Child 2', $teacher1['classroom']['children'][1]['name']);

        static::getDatabase()->createDocument('children', new Document([
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

        $child3 = static::getDatabase()->getDocument('children', 'child3');
        $this->assertEquals('classroom2', $child3['classroom']['$id']);
        $this->assertArrayNotHasKey('children', $child3['classroom']);
        $this->assertEquals('teacher2', $child3['classroom']['teacher']['$id']);
        $this->assertArrayNotHasKey('classroom', $child3['classroom']['teacher']);
    }

    public function testNestedOneToOne_ManyToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('users');
        static::getDatabase()->createCollection('profiles');
        static::getDatabase()->createCollection('avatars');

        static::getDatabase()->createAttribute('users', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('profiles', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('avatars', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'users',
            relatedCollection: 'profiles',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'profile',
            twoWayKey: 'user'
        );
        static::getDatabase()->createRelationship(
            collection: 'profiles',
            relatedCollection: 'avatars',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'avatar',
        );

        static::getDatabase()->createDocument('users', new Document([
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

        $user1 = static::getDatabase()->getDocument('users', 'user1');
        $this->assertEquals('profile1', $user1['profile']['$id']);
        $this->assertArrayNotHasKey('user', $user1['profile']);
        $this->assertEquals('avatar1', $user1['profile']['avatar']['$id']);
        $this->assertArrayNotHasKey('profile', $user1['profile']['avatar']);

        static::getDatabase()->createDocument('avatars', new Document([
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

        $avatar2 = static::getDatabase()->getDocument('avatars', 'avatar2');
        $this->assertEquals('profile2', $avatar2['profiles'][0]['$id']);
        $this->assertArrayNotHasKey('avatars', $avatar2['profiles'][0]);
        $this->assertEquals('user2', $avatar2['profiles'][0]['user']['$id']);
        $this->assertArrayNotHasKey('profiles', $avatar2['profiles'][0]['user']);
    }

    public function testNestedOneToOne_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('addresses');
        static::getDatabase()->createCollection('houses');
        static::getDatabase()->createCollection('buildings');

        static::getDatabase()->createAttribute('addresses', 'street', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('houses', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('buildings', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'addresses',
            relatedCollection: 'houses',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'house',
            twoWayKey: 'address'
        );
        static::getDatabase()->createRelationship(
            collection: 'houses',
            relatedCollection: 'buildings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->createDocument('addresses', new Document([
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

        $address1 = static::getDatabase()->getDocument('addresses', 'address1');
        $this->assertEquals('house1', $address1['house']['$id']);
        $this->assertArrayNotHasKey('address', $address1['house']);
        $this->assertEquals('building1', $address1['house']['buildings'][0]['$id']);
        $this->assertEquals('building2', $address1['house']['buildings'][1]['$id']);
        $this->assertArrayNotHasKey('houses', $address1['house']['buildings'][0]);
        $this->assertArrayNotHasKey('houses', $address1['house']['buildings'][1]);

        static::getDatabase()->createDocument('buildings', new Document([
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('countries');
        static::getDatabase()->createCollection('cities');
        static::getDatabase()->createCollection('mayors');

        static::getDatabase()->createAttribute('cities', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('countries', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('mayors', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'countries',
            relatedCollection: 'cities',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'country'
        );
        static::getDatabase()->createRelationship(
            collection: 'cities',
            relatedCollection: 'mayors',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'mayor',
            twoWayKey: 'city'
        );

        static::getDatabase()->createDocument('countries', new Document([
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

        $documents = static::getDatabase()->find('countries', [
            Query::limit(1)
        ]);
        $this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        $documents = static::getDatabase()->find('countries', [
            Query::select(['name']),
            Query::limit(1)
        ]);
        $this->assertArrayHasKey('name', $documents[0]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = static::getDatabase()->find('countries', [
            Query::select(['*']),
            Query::limit(1)
        ]);
        $this->assertArrayHasKey('name', $documents[0]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = static::getDatabase()->find('countries', [
            Query::select(['*', 'cities.*', 'cities.mayor.*']),
            Query::limit(1)
        ]);

        $this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        // Insert docs to cache:
        $country1 = static::getDatabase()->getDocument('countries', 'country1');
        $mayor1 = static::getDatabase()->getDocument('mayors', 'mayor1');
        $this->assertEquals('City 1', $mayor1['city']['name']);
        $this->assertEquals('City 1', $country1['cities'][0]['name']);

        static::getDatabase()->updateDocument('cities', 'city1', new Document([
            '$id' => 'city1',
            '$collection' => 'cities',
            'name' => 'City 1 updated',
            'mayor' => 'mayor1', // we don't support partial updates at the moment
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        $mayor1 = static::getDatabase()->getDocument('mayors', 'mayor1');
        $country1 = static::getDatabase()->getDocument('countries', 'country1');

        $this->assertEquals('City 1 updated', $mayor1['city']['name']);
        $this->assertEquals('City 1 updated', $country1['cities'][0]['name']);
        $this->assertEquals('city1', $country1['cities'][0]['$id']);
        $this->assertEquals('city2', $country1['cities'][1]['$id']);
        $this->assertEquals('mayor1', $country1['cities'][0]['mayor']['$id']);
        $this->assertEquals('mayor2', $country1['cities'][1]['mayor']['$id']);
        $this->assertArrayNotHasKey('city', $country1['cities'][0]['mayor']);
        $this->assertArrayNotHasKey('city', $country1['cities'][1]['mayor']);

        static::getDatabase()->createDocument('mayors', new Document([
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

        $country2 = static::getDatabase()->getDocument('countries', 'country2');
        $this->assertEquals('city3', $country2['cities'][0]['$id']);
        $this->assertEquals('mayor3', $country2['cities'][0]['mayor']['$id']);
        $this->assertArrayNotHasKey('country', $country2['cities'][0]);
        $this->assertArrayNotHasKey('city', $country2['cities'][0]['mayor']);
    }

    public function testNestedOneToMany_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('dormitories');
        static::getDatabase()->createCollection('occupants');
        static::getDatabase()->createCollection('pets');

        static::getDatabase()->createAttribute('dormitories', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('occupants', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('pets', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'dormitories',
            relatedCollection: 'occupants',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'dormitory'
        );
        static::getDatabase()->createRelationship(
            collection: 'occupants',
            relatedCollection: 'pets',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'occupant'
        );

        static::getDatabase()->createDocument('dormitories', new Document([
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

        $dormitory1 = static::getDatabase()->getDocument('dormitories', 'dormitory1');
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

        static::getDatabase()->createDocument('pets', new Document([
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

        $pet5 = static::getDatabase()->getDocument('pets', 'pet5');
        $this->assertEquals('occupant3', $pet5['occupant']['$id']);
        $this->assertEquals('dormitory2', $pet5['occupant']['dormitory']['$id']);
        $this->assertArrayNotHasKey('pets', $pet5['occupant']);
        $this->assertArrayNotHasKey('occupant', $pet5['occupant']['dormitory']);
    }

    public function testNestedOneToMany_ManyToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('home');
        static::getDatabase()->createCollection('renters');
        static::getDatabase()->createCollection('floors');

        static::getDatabase()->createAttribute('home', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('renters', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('floors', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'home',
            relatedCollection: 'renters',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );
        static::getDatabase()->createRelationship(
            collection: 'renters',
            relatedCollection: 'floors',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'floor'
        );

        static::getDatabase()->createDocument('home', new Document([
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

        $home1 = static::getDatabase()->getDocument('home', 'home1');
        $this->assertEquals('renter1', $home1['renters'][0]['$id']);
        $this->assertEquals('floor1', $home1['renters'][0]['floor']['$id']);
        $this->assertArrayNotHasKey('home', $home1['renters'][0]);
        $this->assertArrayNotHasKey('renters', $home1['renters'][0]['floor']);

        static::getDatabase()->createDocument('floors', new Document([
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

        $floor2 = static::getDatabase()->getDocument('floors', 'floor2');
        $this->assertEquals('renter2', $floor2['renters'][0]['$id']);
        $this->assertArrayNotHasKey('floor', $floor2['renters'][0]);
        $this->assertEquals('home2', $floor2['renters'][0]['home']['$id']);
        $this->assertArrayNotHasKey('renter', $floor2['renters'][0]['home']);
    }

    public function testNestedOneToMany_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('owners');
        static::getDatabase()->createCollection('cats');
        static::getDatabase()->createCollection('toys');

        static::getDatabase()->createAttribute('owners', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('cats', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('toys', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'owners',
            relatedCollection: 'cats',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'owner'
        );
        static::getDatabase()->createRelationship(
            collection: 'cats',
            relatedCollection: 'toys',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true
        );

        static::getDatabase()->createDocument('owners', new Document([
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

        $owner1 = static::getDatabase()->getDocument('owners', 'owner1');
        $this->assertEquals('cat1', $owner1['cats'][0]['$id']);
        $this->assertArrayNotHasKey('owner', $owner1['cats'][0]);
        $this->assertEquals('toy1', $owner1['cats'][0]['toys'][0]['$id']);
        $this->assertArrayNotHasKey('cats', $owner1['cats'][0]['toys'][0]);

        static::getDatabase()->createDocument('toys', new Document([
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

        $toy2 = static::getDatabase()->getDocument('toys', 'toy2');
        $this->assertEquals('cat2', $toy2['cats'][0]['$id']);
        $this->assertArrayNotHasKey('toys', $toy2['cats'][0]);
        $this->assertEquals('owner2', $toy2['cats'][0]['owner']['$id']);
        $this->assertArrayNotHasKey('cats', $toy2['cats'][0]['owner']);
    }

    public function testNestedManyToOne_OneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('towns');
        static::getDatabase()->createCollection('homelands');
        static::getDatabase()->createCollection('capitals');

        static::getDatabase()->createAttribute('towns', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('homelands', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('capitals', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'towns',
            relatedCollection: 'homelands',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'homeland'
        );
        static::getDatabase()->createRelationship(
            collection: 'homelands',
            relatedCollection: 'capitals',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'capital',
            twoWayKey: 'homeland'
        );

        static::getDatabase()->createDocument('towns', new Document([
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

        $town1 = static::getDatabase()->getDocument('towns', 'town1');
        $this->assertEquals('homeland1', $town1['homeland']['$id']);
        $this->assertArrayNotHasKey('towns', $town1['homeland']);
        $this->assertEquals('capital1', $town1['homeland']['capital']['$id']);
        $this->assertArrayNotHasKey('homeland', $town1['homeland']['capital']);

        static::getDatabase()->createDocument('capitals', new Document([
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

        $capital2 = static::getDatabase()->getDocument('capitals', 'capital2');
        $this->assertEquals('homeland2', $capital2['homeland']['$id']);
        $this->assertArrayNotHasKey('capital', $capital2['homeland']);
        $this->assertEquals(2, \count($capital2['homeland']['towns']));
        $this->assertEquals('town2', $capital2['homeland']['towns'][0]['$id']);
        $this->assertEquals('town3', $capital2['homeland']['towns'][1]['$id']);
    }

    public function testNestedManyToOne_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('players');
        static::getDatabase()->createCollection('teams');
        static::getDatabase()->createCollection('supporters');

        static::getDatabase()->createAttribute('players', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('teams', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('supporters', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'players',
            relatedCollection: 'teams',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'team'
        );
        static::getDatabase()->createRelationship(
            collection: 'teams',
            relatedCollection: 'supporters',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'supporters',
            twoWayKey: 'team'
        );

        static::getDatabase()->createDocument('players', new Document([
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

        $player1 = static::getDatabase()->getDocument('players', 'player1');
        $this->assertEquals('team1', $player1['team']['$id']);
        $this->assertArrayNotHasKey('players', $player1['team']);
        $this->assertEquals(2, \count($player1['team']['supporters']));
        $this->assertEquals('supporter1', $player1['team']['supporters'][0]['$id']);
        $this->assertEquals('supporter2', $player1['team']['supporters'][1]['$id']);

        static::getDatabase()->createDocument('supporters', new Document([
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

        $supporter3 = static::getDatabase()->getDocument('supporters', 'supporter3');
        $this->assertEquals('team2', $supporter3['team']['$id']);
        $this->assertArrayNotHasKey('supporters', $supporter3['team']);
        $this->assertEquals(2, \count($supporter3['team']['players']));
        $this->assertEquals('player2', $supporter3['team']['players'][0]['$id']);
        $this->assertEquals('player3', $supporter3['team']['players'][1]['$id']);
    }

    public function testNestedManyToOne_ManyToOne(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('cows');
        static::getDatabase()->createCollection('farms');
        static::getDatabase()->createCollection('farmer');

        static::getDatabase()->createAttribute('cows', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('farms', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('farmer', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'cows',
            relatedCollection: 'farms',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'farm'
        );
        static::getDatabase()->createRelationship(
            collection: 'farms',
            relatedCollection: 'farmer',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'farmer'
        );

        static::getDatabase()->createDocument('cows', new Document([
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

        $cow1 = static::getDatabase()->getDocument('cows', 'cow1');
        $this->assertEquals('farm1', $cow1['farm']['$id']);
        $this->assertArrayNotHasKey('cows', $cow1['farm']);
        $this->assertEquals('farmer1', $cow1['farm']['farmer']['$id']);
        $this->assertArrayNotHasKey('farms', $cow1['farm']['farmer']);

        static::getDatabase()->createDocument('farmer', new Document([
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

        $farmer2 = static::getDatabase()->getDocument('farmer', 'farmer2');
        $this->assertEquals('farm2', $farmer2['farms'][0]['$id']);
        $this->assertArrayNotHasKey('farmer', $farmer2['farms'][0]);
        $this->assertEquals(2, \count($farmer2['farms'][0]['cows']));
        $this->assertEquals('cow2', $farmer2['farms'][0]['cows'][0]['$id']);
        $this->assertEquals('cow3', $farmer2['farms'][0]['cows'][1]['$id']);
    }

    public function testNestedManyToOne_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('books');
        static::getDatabase()->createCollection('entrants');
        static::getDatabase()->createCollection('rooms');

        static::getDatabase()->createAttribute('books', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('entrants', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('rooms', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'books',
            relatedCollection: 'entrants',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'entrant'
        );
        static::getDatabase()->createRelationship(
            collection: 'entrants',
            relatedCollection: 'rooms',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->createDocument('books', new Document([
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

        $book1 = static::getDatabase()->getDocument('books', 'book1');
        $this->assertEquals('entrant1', $book1['entrant']['$id']);
        $this->assertArrayNotHasKey('books', $book1['entrant']);
        $this->assertEquals(2, \count($book1['entrant']['rooms']));
        $this->assertEquals('class1', $book1['entrant']['rooms'][0]['$id']);
        $this->assertEquals('class2', $book1['entrant']['rooms'][1]['$id']);
    }

    public function testNestedManyToMany_OneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('stones');
        static::getDatabase()->createCollection('hearths');
        static::getDatabase()->createCollection('plots');

        static::getDatabase()->createAttribute('stones', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('hearths', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('plots', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'stones',
            relatedCollection: 'hearths',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'hearths',
            relatedCollection: 'plots',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'plot',
            twoWayKey: 'hearth'
        );

        static::getDatabase()->createDocument('stones', new Document([
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

        $stone1 = static::getDatabase()->getDocument('stones', 'stone1');
        $this->assertEquals(2, \count($stone1['hearths']));
        $this->assertEquals('hearth1', $stone1['hearths'][0]['$id']);
        $this->assertEquals('hearth2', $stone1['hearths'][1]['$id']);
        $this->assertArrayNotHasKey('stone', $stone1['hearths'][0]);
        $this->assertEquals('plot1', $stone1['hearths'][0]['plot']['$id']);
        $this->assertEquals('plot2', $stone1['hearths'][1]['plot']['$id']);
        $this->assertArrayNotHasKey('hearth', $stone1['hearths'][0]['plot']);

        static::getDatabase()->createDocument('plots', new Document([
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

        $plot3 = static::getDatabase()->getDocument('plots', 'plot3');
        $this->assertEquals('hearth3', $plot3['hearth']['$id']);
        $this->assertArrayNotHasKey('plot', $plot3['hearth']);
        $this->assertEquals('stone2', $plot3['hearth']['stones'][0]['$id']);
        $this->assertArrayNotHasKey('hearths', $plot3['hearth']['stones'][0]);
    }

    public function testNestedManyToMany_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('groups');
        static::getDatabase()->createCollection('tounaments');
        static::getDatabase()->createCollection('prizes');

        static::getDatabase()->createAttribute('groups', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('tounaments', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('prizes', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'groups',
            relatedCollection: 'tounaments',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
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

        $group1 = static::getDatabase()->getDocument('groups', 'group1');
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('platforms');
        static::getDatabase()->createCollection('games');
        static::getDatabase()->createCollection('publishers');

        static::getDatabase()->createAttribute('platforms', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('games', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('publishers', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'platforms',
            relatedCollection: 'games',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'games',
            relatedCollection: 'publishers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'publisher',
            twoWayKey: 'games'
        );

        static::getDatabase()->createDocument('platforms', new Document([
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

        $platform1 = static::getDatabase()->getDocument('platforms', 'platform1');
        $this->assertEquals(2, \count($platform1['games']));
        $this->assertEquals('game1', $platform1['games'][0]['$id']);
        $this->assertEquals('game2', $platform1['games'][1]['$id']);
        $this->assertArrayNotHasKey('platforms', $platform1['games'][0]);
        $this->assertEquals('publisher1', $platform1['games'][0]['publisher']['$id']);
        $this->assertEquals('publisher2', $platform1['games'][1]['publisher']['$id']);
        $this->assertArrayNotHasKey('games', $platform1['games'][0]['publisher']);

        static::getDatabase()->createDocument('publishers', new Document([
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

        $publisher3 = static::getDatabase()->getDocument('publishers', 'publisher3');
        $this->assertEquals(1, \count($publisher3['games']));
        $this->assertEquals('game3', $publisher3['games'][0]['$id']);
        $this->assertArrayNotHasKey('publisher', $publisher3['games'][0]);
        $this->assertEquals('platform2', $publisher3['games'][0]['platforms'][0]['$id']);
        $this->assertArrayNotHasKey('games', $publisher3['games'][0]['platforms'][0]);
    }

    public function testNestedManyToMany_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('sauces');
        static::getDatabase()->createCollection('pizzas');
        static::getDatabase()->createCollection('toppings');

        static::getDatabase()->createAttribute('sauces', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('pizzas', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('toppings', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'sauces',
            relatedCollection: 'pizzas',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'pizzas',
            relatedCollection: 'toppings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'toppings',
            twoWayKey: 'pizzas'
        );

        static::getDatabase()->createDocument('sauces', new Document([
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

        $sauce1 = static::getDatabase()->getDocument('sauces', 'sauce1');
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('lawns', permissions: [Permission::create(Role::any())], documentSecurity: true);
        static::getDatabase()->createCollection('trees', permissions: [Permission::create(Role::any())], documentSecurity: true);
        static::getDatabase()->createCollection('birds', permissions: [Permission::create(Role::any())], documentSecurity: true);

        static::getDatabase()->createAttribute('lawns', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('trees', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('birds', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'lawns',
            relatedCollection: 'trees',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'lawn',
            onDelete: Database::RELATION_MUTATE_CASCADE,
        );
        static::getDatabase()->createRelationship(
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

        static::getDatabase()->createDocument('lawns', new Document([
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

        $lawn1 = static::getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals($permissions, $lawn1->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]['birds'][0]->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]['birds'][1]->getPermissions());

        $tree1 = static::getDatabase()->getDocument('trees', 'tree1');
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());
        $lawn1 = static::getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals('Lawn 1', $lawn1['name']);

        // Try update root document
        try {
            static::getDatabase()->updateDocument(
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
            static::getDatabase()->deleteDocument(
                'lawns',
                $lawn1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        $tree1 = static::getDatabase()->getDocument('trees', 'tree1');

        // Try update nested document
        try {
            static::getDatabase()->updateDocument(
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
            static::getDatabase()->deleteDocument(
                'trees',
                $tree1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        $bird1 = static::getDatabase()->getDocument('birds', 'bird1');

        // Try update multi-level nested document
        try {
            static::getDatabase()->updateDocument(
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
            static::getDatabase()->deleteDocument(
                'birds',
                $bird1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        Authorization::setRole(Role::user('user1')->toString());

        $bird1 = static::getDatabase()->getDocument('birds', 'bird1');

        // Try update multi-level nested document
        $bird1 = static::getDatabase()->updateDocument(
            'birds',
            $bird1->getId(),
            $bird1->setAttribute('name', 'Bird 1 Updated')
        );

        $this->assertEquals('Bird 1 Updated', $bird1['name']);

        Authorization::setRole(Role::user('user2')->toString());

        // Try delete multi-level nested document
        $deleted = static::getDatabase()->deleteDocument(
            'birds',
            $bird1->getId(),
        );

        $this->assertEquals(true, $deleted);
        $tree1 = static::getDatabase()->getDocument('trees', 'tree1');
        $this->assertEquals(1, count($tree1['birds']));

        // Try update nested document
        $tree1 = static::getDatabase()->updateDocument(
            'trees',
            $tree1->getId(),
            $tree1->setAttribute('name', 'Tree 1 Updated')
        );

        $this->assertEquals('Tree 1 Updated', $tree1['name']);

        // Try delete nested document
        $deleted = static::getDatabase()->deleteDocument(
            'trees',
            $tree1->getId(),
        );

        $this->assertEquals(true, $deleted);
        $lawn1 = static::getDatabase()->getDocument('lawns', 'lawn1');
        $this->assertEquals(0, count($lawn1['trees']));

        // Create document with no permissions
        static::getDatabase()->createDocument('lawns', new Document([
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

        $lawn2 = static::getDatabase()->getDocument('lawns', 'lawn2');
        $this->assertEquals(true, $lawn2->isEmpty());

        $tree2 = static::getDatabase()->getDocument('trees', 'tree2');
        $this->assertEquals(true, $tree2->isEmpty());

        $bird3 = static::getDatabase()->getDocument('birds', 'bird3');
        $this->assertEquals(true, $bird3->isEmpty());
    }

    public function testExceedMaxDepthOneToMany(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToMany';
        $level2Collection = 'level2OneToMany';
        $level3Collection = 'level3OneToMany';
        $level4Collection = 'level4OneToMany';

        static::getDatabase()->createCollection($level1Collection);
        static::getDatabase()->createCollection($level2Collection);
        static::getDatabase()->createCollection($level3Collection);
        static::getDatabase()->createCollection($level4Collection);

        static::getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        // Exceed create depth
        $level1 = static::getDatabase()->createDocument($level1Collection, new Document([
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
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals(0, count($level3[$level4Collection]));
        $level4 = static::getDatabase()->getDocument($level4Collection, 'level4');
        $this->assertTrue($level4->isEmpty());

        // Exceed fetch depth
        $level1 = static::getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertEquals(1, count($level1[$level2Collection]));
        $this->assertEquals('level2', $level1[$level2Collection][0]->getId());
        $this->assertEquals(1, count($level1[$level2Collection][0][$level3Collection]));
        $this->assertEquals('level3', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][0][$level3Collection][0]);


        // Exceed update depth
        $level1 = static::getDatabase()->updateDocument(
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
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3new');
        $this->assertEquals(0, count($level3[$level4Collection]));
        $level4 = static::getDatabase()->getDocument($level4Collection, 'level4new');
        $this->assertTrue($level4->isEmpty());
    }

    public function testExceedMaxDepthOneToOne(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToOne';
        $level2Collection = 'level2OneToOne';
        $level3Collection = 'level3OneToOne';
        $level4Collection = 'level4OneToOne';

        static::getDatabase()->createCollection($level1Collection);
        static::getDatabase()->createCollection($level2Collection);
        static::getDatabase()->createCollection($level3Collection);
        static::getDatabase()->createCollection($level4Collection);

        static::getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        // Exceed create depth
        $level1 = static::getDatabase()->createDocument($level1Collection, new Document([
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
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertNull($level3[$level4Collection]);

        // Create level 4 document
        $level3->setAttribute($level4Collection, new Document([
            '$id' => 'level4',
        ]));
        $level3 = static::getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection]->getId());

        // Exceed fetch depth
        $level1 = static::getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);
    }

    public function testExceedMaxDepthOneToOneNull(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToOneNull';
        $level2Collection = 'level2OneToOneNull';
        $level3Collection = 'level3OneToOneNull';
        $level4Collection = 'level4OneToOneNull';

        static::getDatabase()->createCollection($level1Collection);
        static::getDatabase()->createCollection($level2Collection);
        static::getDatabase()->createCollection($level3Collection);
        static::getDatabase()->createCollection($level4Collection);

        static::getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $level1 = static::getDatabase()->createDocument($level1Collection, new Document([
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
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertNull($level3[$level4Collection]);

        // Create level 4 document
        $level3->setAttribute($level4Collection, new Document([
            '$id' => 'level4',
        ]));
        $level3 = static::getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection]->getId());
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals('level4', $level3[$level4Collection]->getId());

        // Exceed fetch depth
        $level1 = static::getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);
    }

    public function testExceedMaxDepthManyToOneParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1ManyToOneParent';
        $level2Collection = 'level2ManyToOneParent';
        $level3Collection = 'level3ManyToOneParent';
        $level4Collection = 'level4ManyToOneParent';

        static::getDatabase()->createCollection($level1Collection);
        static::getDatabase()->createCollection($level2Collection);
        static::getDatabase()->createCollection($level3Collection);
        static::getDatabase()->createCollection($level4Collection);

        static::getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );

        $level1 = static::getDatabase()->createDocument($level1Collection, new Document([
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
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertNull($level3[$level4Collection]);

        // Create level 4 document
        $level3->setAttribute($level4Collection, new Document([
            '$id' => 'level4',
        ]));
        $level3 = static::getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection]->getId());
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals('level4', $level3[$level4Collection]->getId());

        // Exceed fetch depth
        $level1 = static::getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);
    }

    public function testExceedMaxDepthOneToManyChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToManyChild';
        $level2Collection = 'level2OneToManyChild';
        $level3Collection = 'level3OneToManyChild';
        $level4Collection = 'level4OneToManyChild';

        static::getDatabase()->createCollection($level1Collection);
        static::getDatabase()->createCollection($level2Collection);
        static::getDatabase()->createCollection($level3Collection);
        static::getDatabase()->createCollection($level4Collection);

        static::getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $level1 = static::getDatabase()->createDocument($level1Collection, new Document([
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
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals(0, count($level3[$level4Collection]));

        // Create level 4 document
        $level3->setAttribute($level4Collection, [new Document([
            '$id' => 'level4',
        ])]);
        $level3 = static::getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection][0]->getId());

        // Verify level 4 document is set
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertArrayHasKey($level4Collection, $level3);
        $this->assertEquals('level4', $level3[$level4Collection][0]->getId());

        // Exceed fetch depth
        $level4 = static::getDatabase()->getDocument($level4Collection, 'level4');
        $this->assertArrayHasKey($level3Collection, $level4);
        $this->assertEquals('level3', $level4[$level3Collection]->getId());
        $this->assertArrayHasKey($level2Collection, $level4[$level3Collection]);
        $this->assertEquals('level2', $level4[$level3Collection][$level2Collection]->getId());
        $this->assertArrayNotHasKey($level1Collection, $level4[$level3Collection][$level2Collection]);
    }

    public function testCreateRelationshipMissingCollection(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Collection not found');

        static::getDatabase()->createRelationship(
            collection: 'missing',
            relatedCollection: 'missing',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateRelationshipMissingRelatedCollection(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('test');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Related collection not found');

        static::getDatabase()->createRelationship(
            collection: 'test',
            relatedCollection: 'missing',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateDuplicateRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('test1');
        static::getDatabase()->createCollection('test2');

        static::getDatabase()->createRelationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Attribute already exists');

        static::getDatabase()->createRelationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateInvalidRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('test3');
        static::getDatabase()->createCollection('test4');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid relationship type');

        static::getDatabase()->createRelationship(
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        try {
            static::getDatabase()->deleteRelationship('test', 'test2');
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertEquals('Relationship not found', $e->getMessage());
        }
    }

    public function testCreateInvalidIntValueRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('invalid1');
        static::getDatabase()->createCollection('invalid2');

        static::getDatabase()->createRelationship(
            collection: 'invalid1',
            relatedCollection: 'invalid2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        static::getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => 10,
        ]));
    }

    /**
     * @depends testCreateInvalidIntValueRelationship
     */
    public function testCreateInvalidObjectValueRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        static::getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => new \stdClass(),
        ]));
    }

    /**
     * @depends testCreateInvalidIntValueRelationship
     */
    public function testCreateInvalidArrayIntValueRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createRelationship(
            collection: 'invalid1',
            relatedCollection: 'invalid2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'invalid3',
            twoWayKey: 'invalid4',
        );

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        static::getDatabase()->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid3' => [10],
        ]));
    }

    public function testCreateEmptyValueRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('null1');
        static::getDatabase()->createCollection('null2');

        static::getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'null3',
            twoWayKey: 'null4',
        );
        static::getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'null4',
            twoWayKey: 'null5',
        );
        static::getDatabase()->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'null6',
            twoWayKey: 'null7',
        );

        $document = static::getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null2' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null2'));

        $document = static::getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null1' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null1'));

        $document = static::getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null3' => null,
        ]));

        // One to many will be empty array instead of null
        $this->assertEquals([], $document->getAttribute('null3'));

        $document = static::getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null4' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null4'));

        $document = static::getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null4' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null4'));

        $document = static::getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null5' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null5'));

        $document = static::getDatabase()->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null6' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null6'));

        $document = static::getDatabase()->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null7' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null7'));
    }

    public function testDeleteCollectionDeletesRelationships(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('testers');
        static::getDatabase()->createCollection('devices');

        static::getDatabase()->createRelationship(
            collection: 'testers',
            relatedCollection: 'devices',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'tester'
        );

        $testers = static::getDatabase()->getCollection('testers');
        $devices = static::getDatabase()->getCollection('devices');

        $this->assertEquals(1, \count($testers->getAttribute('attributes')));
        $this->assertEquals(1, \count($devices->getAttribute('attributes')));
        $this->assertEquals(1, \count($devices->getAttribute('indexes')));

        static::getDatabase()->deleteCollection('testers');

        $testers = static::getDatabase()->getCollection('testers');
        $devices = static::getDatabase()->getCollection('devices');

        $this->assertEquals(true, $testers->isEmpty());
        $this->assertEquals(0, \count($devices->getAttribute('attributes')));
        $this->assertEquals(0, \count($devices->getAttribute('indexes')));
    }

    public function testDeleteTwoWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('drivers');
        static::getDatabase()->createCollection('licenses');

        static::getDatabase()->createRelationship(
            collection: 'drivers',
            relatedCollection: 'licenses',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'license',
            twoWayKey: 'driver'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(1, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('licenses', 'driver');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'drivers',
            relatedCollection: 'licenses',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'licenses',
            twoWayKey: 'driver'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('licenses', 'driver');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'licenses',
            relatedCollection: 'drivers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'driver',
            twoWayKey: 'licenses'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('drivers', 'licenses');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'licenses',
            relatedCollection: 'drivers',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'drivers',
            twoWayKey: 'licenses'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');
        $junction = static::getDatabase()->getCollection('_' . $licenses->getInternalId() . '_' . $drivers->getInternalId());

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));
        $this->assertEquals(2, \count($junction->getAttribute('attributes')));
        $this->assertEquals(2, \count($junction->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('drivers', 'licenses');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');
        $junction = static::getDatabase()->getCollection('_licenses_drivers');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        $this->assertEquals(true, $junction->isEmpty());
    }

    public function testUpdateRelationshipToExistingKey(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('ovens');
        static::getDatabase()->createCollection('cakes');

        static::getDatabase()->createAttribute('ovens', 'maxTemp', Database::VAR_INTEGER, 0, true);
        static::getDatabase()->createAttribute('ovens', 'owner', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('cakes', 'height', Database::VAR_INTEGER, 0, true);
        static::getDatabase()->createAttribute('cakes', 'colour', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'ovens',
            relatedCollection: 'cakes',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'cakes',
            twoWayKey: 'oven'
        );

        try {
            static::getDatabase()->updateRelationship('ovens', 'cakes', newKey: 'owner');
            $this->fail('Failed to throw exception');
        } catch (DuplicateException $e) {
            $this->assertEquals('Relationship already exists', $e->getMessage());
        }

        try {
            static::getDatabase()->updateRelationship('ovens', 'cakes', newTwoWayKey: 'height');
            $this->fail('Failed to throw exception');
        } catch (DuplicateException $e) {
            $this->assertEquals('Related attribute already exists', $e->getMessage());
        }
    }

    public function testOneToOneRelationshipKeyWithSymbols(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('$symbols_coll.ection1');
        static::getDatabase()->createCollection('$symbols_coll.ection2');

        static::getDatabase()->createRelationship(
            collection: '$symbols_coll.ection1',
            relatedCollection: '$symbols_coll.ection2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $doc1 = static::getDatabase()->createDocument('$symbols_coll.ection2', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = static::getDatabase()->createDocument('$symbols_coll.ection1', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection2' => $doc1->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = static::getDatabase()->getDocument('$symbols_coll.ection2', $doc1->getId());
        $doc2 = static::getDatabase()->getDocument('$symbols_coll.ection1', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection1')->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection2')->getId());
    }

    public function testOneToManyRelationshipKeyWithSymbols(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('$symbols_coll.ection3');
        static::getDatabase()->createCollection('$symbols_coll.ection4');

        static::getDatabase()->createRelationship(
            collection: '$symbols_coll.ection3',
            relatedCollection: '$symbols_coll.ection4',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $doc1 = static::getDatabase()->createDocument('$symbols_coll.ection4', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = static::getDatabase()->createDocument('$symbols_coll.ection3', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection4' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = static::getDatabase()->getDocument('$symbols_coll.ection4', $doc1->getId());
        $doc2 = static::getDatabase()->getDocument('$symbols_coll.ection3', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection3')->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection4')[0]->getId());
    }

    public function testManyToOneRelationshipKeyWithSymbols(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('$symbols_coll.ection5');
        static::getDatabase()->createCollection('$symbols_coll.ection6');

        static::getDatabase()->createRelationship(
            collection: '$symbols_coll.ection5',
            relatedCollection: '$symbols_coll.ection6',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
        );

        $doc1 = static::getDatabase()->createDocument('$symbols_coll.ection6', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = static::getDatabase()->createDocument('$symbols_coll.ection5', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection6' => $doc1->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = static::getDatabase()->getDocument('$symbols_coll.ection6', $doc1->getId());
        $doc2 = static::getDatabase()->getDocument('$symbols_coll.ection5', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection5')[0]->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection6')->getId());
    }

    public function testManyToManyRelationshipKeyWithSymbols(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('$symbols_coll.ection7');
        static::getDatabase()->createCollection('$symbols_coll.ection8');

        static::getDatabase()->createRelationship(
            collection: '$symbols_coll.ection7',
            relatedCollection: '$symbols_coll.ection8',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $doc1 = static::getDatabase()->createDocument('$symbols_coll.ection8', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = static::getDatabase()->createDocument('$symbols_coll.ection7', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection8' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = static::getDatabase()->getDocument('$symbols_coll.ection8', $doc1->getId());
        $doc2 = static::getDatabase()->getDocument('$symbols_coll.ection7', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection7')[0]->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection8')[0]->getId());
    }


}

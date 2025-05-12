<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Tests\E2E\Adapter\Scopes\Relationships\ManyToManyTests;
use Tests\E2E\Adapter\Scopes\Relationships\ManyToOneTests;
use Tests\E2E\Adapter\Scopes\Relationships\OneToManyTests;
use Tests\E2E\Adapter\Scopes\Relationships\OneToOneTests;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

trait RelationshipTests
{
    use OneToOneTests;
    use OneToManyTests;
    use ManyToOneTests;
    use ManyToManyTests;

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
            Query::select('name'),
            Query::select('models.name'),
            Query::select('*'), // Added this to make tests pass, perhaps to add in to nesting queries?
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
            Query::select('name'),
            Query::select('$id')
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
            Query::select('name'),
            Query::select('$internalId')
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
            Query::select('name'),
            Query::select('$collection'),
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
            Query::select('name'),
            Query::select('$createdAt'),
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
            Query::select('name'),
            Query::select('$updatedAt'),
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
            Query::select('name'),
            Query::select('$permissions'),
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
            Query::select('*'),
            Query::select('models.year')
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
            Query::select('*'),
            Query::select('models.*')
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
            Query::select('models.*')
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
            Query::select('name'),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertArrayNotHasKey('models', $make);
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

    public function testUpdateDocumentsRelationships(): void
    {
        if (!$this->getDatabase()->getAdapter()->getSupportForBatchOperations() || !$this->getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $this->getDatabase()->createCollection('testUpdateDocumentsRelationships1', attributes: [
            new Document([
                '$id' => ID::custom('string'),
                'type' => Database::VAR_STRING,
                'size' => 767,
                'required' => true,
            ])
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $this->getDatabase()->createCollection('testUpdateDocumentsRelationships2', attributes: [
            new Document([
                '$id' => ID::custom('string'),
                'type' => Database::VAR_STRING,
                'size' => 767,
                'required' => true,
            ])
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $this->getDatabase()->createRelationship(
            collection: 'testUpdateDocumentsRelationships1',
            relatedCollection: 'testUpdateDocumentsRelationships2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->getDatabase()->createDocument('testUpdateDocumentsRelationships1', new Document([
            '$id' => 'doc1',
            'string' => 'text📝',
        ]));

        $this->getDatabase()->createDocument('testUpdateDocumentsRelationships2', new Document([
            '$id' => 'doc1',
            'string' => 'text📝',
            'testUpdateDocumentsRelationships1' => 'doc1'
        ]));

        $sisterDocument = $this->getDatabase()->getDocument('testUpdateDocumentsRelationships2', 'doc1');
        $this->assertNotNull($sisterDocument);

        $this->getDatabase()->updateDocuments('testUpdateDocumentsRelationships1', new Document([
            'string' => 'text📝 updated',
        ]));

        $document = $this->getDatabase()->findOne('testUpdateDocumentsRelationships1');

        $this->assertNotFalse($document);
        $this->assertEquals('text📝 updated', $document->getAttribute('string'));

        $sisterDocument = $this->getDatabase()->getDocument('testUpdateDocumentsRelationships2', 'doc1');
        $this->assertNotNull($sisterDocument);

        $relationalDocument = $sisterDocument->getAttribute('testUpdateDocumentsRelationships1');
        $this->assertEquals('text📝 updated', $relationalDocument->getAttribute('string'));

        // Check relationship value updating between each other.
        $this->getDatabase()->deleteRelationship('testUpdateDocumentsRelationships1', 'testUpdateDocumentsRelationships2');

        $this->getDatabase()->createRelationship(
            collection: 'testUpdateDocumentsRelationships1',
            relatedCollection: 'testUpdateDocumentsRelationships2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        for ($i = 2; $i < 11; $i++) {
            $this->getDatabase()->createDocument('testUpdateDocumentsRelationships1', new Document([
                '$id' => 'doc' . $i,
                'string' => 'text📝',
            ]));

            $this->getDatabase()->createDocument('testUpdateDocumentsRelationships2', new Document([
                '$id' => 'doc' . $i,
                'string' => 'text📝',
                'testUpdateDocumentsRelationships1' => 'doc' . $i
            ]));
        }

        $this->getDatabase()->updateDocuments('testUpdateDocumentsRelationships2', new Document([
            'testUpdateDocumentsRelationships1' => null
        ]));

        $this->getDatabase()->updateDocuments('testUpdateDocumentsRelationships2', new Document([
            'testUpdateDocumentsRelationships1' => 'doc1'
        ]));

        $documents = $this->getDatabase()->find('testUpdateDocumentsRelationships2');

        foreach ($documents as $document) {
            $this->assertEquals('doc1', $document->getAttribute('testUpdateDocumentsRelationships1')->getId());
        }
    }

    public function testUpdateDocumentWithRelationships(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('userProfiles', [
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
        static::getDatabase()->createCollection('links', [
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
        static::getDatabase()->createCollection('videos', [
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
        static::getDatabase()->createCollection('products', [
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
        static::getDatabase()->createCollection('settings', [
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
        static::getDatabase()->createCollection('appearance', [
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
        static::getDatabase()->createCollection('group', [
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
        static::getDatabase()->createCollection('community', [
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

        static::getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'links',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'links'
        );

        static::getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'videos',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'videos'
        );

        static::getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'products',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'products',
            twoWayKey: 'userProfile',
        );

        static::getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'settings',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'settings'
        );

        static::getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'appearance',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'appearance'
        );

        static::getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'group',
            type: Database::RELATION_MANY_TO_ONE,
            id: 'group'
        );

        static::getDatabase()->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'community',
            type: Database::RELATION_MANY_TO_ONE,
            id: 'community'
        );

        $profile = static::getDatabase()->createDocument('userProfiles', new Document([
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

        $updatedProfile = static::getDatabase()->updateDocument('userProfiles', '1', $profile);

        $this->assertEquals('New Link Value', $updatedProfile->getAttribute('links')[0]->getAttribute('title'));
        $this->assertEquals('New Meta Title', $updatedProfile->getAttribute('settings')->getAttribute('metaTitle'));
        $this->assertEquals('New Group Name', $updatedProfile->getAttribute('group')->getAttribute('name'));

        // This is the point of test, related documents should be present if they are not updated
        $this->assertEquals('Video 1', $updatedProfile->getAttribute('videos')[0]->getAttribute('title'));
        $this->assertEquals('Product 1', $updatedProfile->getAttribute('products')[0]->getAttribute('title'));
        $this->assertEquals('Meta Title', $updatedProfile->getAttribute('appearance')->getAttribute('metaTitle'));
        $this->assertEquals('Community 1', $updatedProfile->getAttribute('community')->getAttribute('name'));

        // updating document using two way key in one to many relationship
        $product = static::getDatabase()->getDocument('products', 'product1');
        $product->setAttribute('userProfile', [
            '$id' => '1',
            'username' => 'updated user value',
        ]);
        $updatedProduct = static::getDatabase()->updateDocument('products', 'product1', $product);
        $this->assertEquals('updated user value', $updatedProduct->getAttribute('userProfile')->getAttribute('username'));
        $this->assertEquals('Product 1', $updatedProduct->getAttribute('title'));
        $this->assertEquals('product1', $updatedProduct->getId());
        $this->assertEquals('1', $updatedProduct->getAttribute('userProfile')->getId());

        static::getDatabase()->deleteCollection('userProfiles');
        static::getDatabase()->deleteCollection('links');
        static::getDatabase()->deleteCollection('settings');
        static::getDatabase()->deleteCollection('group');
        static::getDatabase()->deleteCollection('community');
        static::getDatabase()->deleteCollection('videos');
        static::getDatabase()->deleteCollection('products');
        static::getDatabase()->deleteCollection('appearance');
    }
}

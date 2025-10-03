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

    public function testZoo(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('zoo');
        $database->createAttribute('zoo', 'name', Database::VAR_STRING, 256, true);

        $database->createCollection('veterinarians');
        $database->createAttribute('veterinarians', 'fullname', Database::VAR_STRING, 256, true);

        $database->createCollection('presidents');
        $database->createAttribute('presidents', 'first_name', Database::VAR_STRING, 256, true);
        $database->createAttribute('presidents', 'last_name', Database::VAR_STRING, 256, true);
        $database->createRelationship(
            collection: 'presidents',
            relatedCollection: 'veterinarians',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'votes',
            twoWayKey: 'presidents'
        );

        $database->createCollection('__animals');
        $database->createAttribute('__animals', 'name', Database::VAR_STRING, 256, true);
        $database->createAttribute('__animals', 'age', Database::VAR_INTEGER, 0, false);
        $database->createAttribute('__animals', 'price', Database::VAR_FLOAT, 0, false);
        $database->createAttribute('__animals', 'date_of_birth', Database::VAR_DATETIME, 0, true, filters:['datetime']);
        $database->createAttribute('__animals', 'longtext', Database::VAR_STRING, 100000000, false);
        $database->createAttribute('__animals', 'is_active', Database::VAR_BOOLEAN, 0, false, default: true);
        $database->createAttribute('__animals', 'integers', Database::VAR_INTEGER, 0, false, array: true);
        $database->createAttribute('__animals', 'email', Database::VAR_STRING, 255, false);
        $database->createAttribute('__animals', 'ip', Database::VAR_STRING, 255, false);
        $database->createAttribute('__animals', 'url', Database::VAR_STRING, 255, false);
        $database->createAttribute('__animals', 'enum', Database::VAR_STRING, 255, false);

        $database->createRelationship(
            collection: 'presidents',
            relatedCollection: '__animals',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'animal',
            twoWayKey: 'president'
        );

        $database->createRelationship(
            collection: 'veterinarians',
            relatedCollection: '__animals',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'animals',
            twoWayKey: 'veterinarian'
        );

        $database->createRelationship(
            collection: '__animals',
            relatedCollection: 'zoo',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'zoo',
            twoWayKey: 'animals'
        );

        $zoo = $database->createDocument('zoo', new Document([
            '$id' => 'zoo1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Bronx Zoo'
        ]));

        $this->assertEquals('zoo1', $zoo->getId());
        $this->assertArrayHasKey('animals', $zoo);

        $iguana = $database->createDocument('__animals', new Document([
            '$id' => 'iguana',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Iguana',
            'age' => 11,
            'price' => 50.5,
            'date_of_birth' => '1975-06-12',
            'longtext' => 'I am a pretty long text',
            'is_active' => true,
            'integers' => [1, 2, 3],
            'email' => 'iguana@appwrite.io',
            'enum' => 'maybe',
            'ip' => '127.0.0.1',
            'url' => 'https://appwrite.io/',
            'zoo' => $zoo->getId(),
        ]));

        $tiger = $database->createDocument('__animals', new Document([
            '$id' => 'tiger',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Tiger',
            'age' => 5,
            'price' => 1000,
            'date_of_birth' => '2020-06-12',
            'longtext' => 'I am a hungry tiger',
            'is_active' => false,
            'integers' => [9, 2, 3],
            'email' => 'tiger@appwrite.io',
            'enum' => 'yes',
            'ip' => '255.0.0.1',
            'url' => 'https://appwrite.io/',
            'zoo' => $zoo->getId(),
        ]));

        $lama = $database->createDocument('__animals', new Document([
            '$id' => 'lama',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Lama',
            'age' => 15,
            'price' => 1000,
            'date_of_birth' => '1975-06-12',
            'is_active' => true,
            'integers' => null,
            'email' => null,
            'enum' => null,
            'ip' => '255.0.0.1',
            'url' => 'https://appwrite.io/',
            'zoo' => null,
        ]));

        $veterinarian1 = $database->createDocument('veterinarians', new Document([
            '$id' => 'dr.pol',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'fullname' => 'The Incredible Dr. Pol',
            'animals' => ['iguana'],
        ]));

        $veterinarian2 = $database->createDocument('veterinarians', new Document([
            '$id' => 'dr.seuss',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'fullname' => 'Dr. Seuss',
            'animals' => ['tiger'],
        ]));

        $trump = $database->createDocument('presidents', new Document([
            '$id' => 'trump',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'first_name' => 'Donald',
            'last_name' => 'Trump',
            'votes' => [
                $veterinarian1->getId(),
                $veterinarian2->getId(),
            ],
        ]));

        $bush = $database->createDocument('presidents', new Document([
            '$id' => 'bush',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'first_name' => 'George',
            'last_name' => 'Bush',
            'animal' => 'iguana',
        ]));

        $biden = $database->createDocument('presidents', new Document([
            '$id' => 'biden',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'first_name' => 'Joe',
            'last_name' => 'Biden',
            'animal' => 'tiger',
        ]));

        /**
         * Check Zoo data
         */
        $zoo = $database->getDocument('zoo', 'zoo1');

        $this->assertEquals('zoo1', $zoo->getId());
        $this->assertEquals('Bronx Zoo', $zoo->getAttribute('name'));
        $this->assertArrayHasKey('animals', $zoo);
        $this->assertEquals(2, count($zoo->getAttribute('animals')));
        $this->assertArrayHasKey('president', $zoo->getAttribute('animals')[0]);
        $this->assertArrayHasKey('veterinarian', $zoo->getAttribute('animals')[0]);

        $zoo = $database->findOne('zoo');

        $this->assertEquals('zoo1', $zoo->getId());
        $this->assertEquals('Bronx Zoo', $zoo->getAttribute('name'));
        $this->assertArrayHasKey('animals', $zoo);
        $this->assertEquals(2, count($zoo->getAttribute('animals')));
        $this->assertArrayHasKey('president', $zoo->getAttribute('animals')[0]);
        $this->assertArrayHasKey('veterinarian', $zoo->getAttribute('animals')[0]);

        /**
         * Check Veterinarians data
         */
        $veterinarian = $database->getDocument('veterinarians', 'dr.pol');

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayHasKey('presidents', $veterinarian);
        $this->assertEquals(1, count($veterinarian->getAttribute('presidents')));
        $this->assertArrayHasKey('animal', $veterinarian->getAttribute('presidents')[0]);
        $this->assertArrayHasKey('animals', $veterinarian);
        $this->assertEquals(1, count($veterinarian->getAttribute('animals')));
        $this->assertArrayHasKey('zoo', $veterinarian->getAttribute('animals')[0]);
        $this->assertArrayHasKey('president', $veterinarian->getAttribute('animals')[0]);

        $veterinarian = $database->findOne('veterinarians', [
            Query::equal('$id', ['dr.pol'])
        ]);

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayHasKey('presidents', $veterinarian);
        $this->assertEquals(1, count($veterinarian->getAttribute('presidents')));
        $this->assertArrayHasKey('animal', $veterinarian->getAttribute('presidents')[0]);
        $this->assertArrayHasKey('animals', $veterinarian);
        $this->assertEquals(1, count($veterinarian->getAttribute('animals')));
        $this->assertArrayHasKey('zoo', $veterinarian->getAttribute('animals')[0]);
        $this->assertArrayHasKey('president', $veterinarian->getAttribute('animals')[0]);

        /**
         * Check Animals data
         */
        $animal = $database->getDocument('__animals', 'iguana');

        $this->assertEquals('iguana', $animal->getId());
        $this->assertArrayHasKey('zoo', $animal);
        $this->assertEquals('Bronx Zoo', $animal['zoo']->getAttribute('name'));
        $this->assertArrayHasKey('veterinarian', $animal);
        $this->assertEquals('dr.pol', $animal['veterinarian']->getId());
        $this->assertArrayHasKey('presidents', $animal['veterinarian']);
        $this->assertArrayHasKey('president', $animal);
        $this->assertEquals('bush', $animal['president']->getId());

        $animal = $database->findOne('__animals', [
            Query::equal('$id', ['tiger'])
        ]);

        $this->assertEquals('tiger', $animal->getId());
        $this->assertArrayHasKey('zoo', $animal);
        $this->assertEquals('Bronx Zoo', $animal['zoo']->getAttribute('name'));
        $this->assertArrayHasKey('veterinarian', $animal);
        $this->assertEquals('dr.seuss', $animal['veterinarian']->getId());
        $this->assertArrayHasKey('presidents', $animal['veterinarian']);
        $this->assertArrayHasKey('president', $animal);
        $this->assertEquals('biden', $animal['president']->getId());

        /**
         * Check President data
         */
        $president = $database->getDocument('presidents', 'trump');

        $this->assertEquals('trump', $president->getId());
        $this->assertArrayHasKey('animal', $president);
        $this->assertArrayHasKey('votes', $president);
        $this->assertEquals(2, count($president['votes']));

        /**
         * Check President data
         */
        $president = $database->findOne('presidents', [
            Query::equal('$id', ['bush'])
        ]);

        $this->assertEquals('bush', $president->getId());
        $this->assertArrayHasKey('animal', $president);
        $this->assertArrayHasKey('votes', $president);
        $this->assertEquals(0, count($president['votes']));

        $president = $database->findOne('presidents', [
            Query::select([
                '*',
                'votes.*',
            ]),
            Query::equal('$id', ['trump'])
        ]);

        $this->assertEquals('trump', $president->getId());
        $this->assertArrayHasKey('votes', $president);
        $this->assertEquals(2, count($president['votes']));
        $this->assertArrayNotHasKey('animals', $president['votes'][0]); // Not exist

        $president = $database->findOne('presidents', [
            Query::select([
                '*',
                'votes.*',
                'votes.animals.*',
            ]),
            Query::equal('$id', ['trump'])
        ]);

        $this->assertEquals('trump', $president->getId());
        $this->assertArrayHasKey('votes', $president);
        $this->assertEquals(2, count($president['votes']));
        $this->assertArrayHasKey('animals', $president['votes'][0]); // Exist

        /**
         * Check Selects queries
         */
        $veterinarian = $database->findOne('veterinarians', [
            Query::select(['*']), // No resolving
            Query::equal('$id', ['dr.pol']),
        ]);

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayNotHasKey('presidents', $veterinarian);
        $this->assertArrayNotHasKey('animals', $veterinarian);

        $veterinarian = $database->findOne(
            'veterinarians',
            [
                Query::select([
                    'animals.*',
                ])
            ]
        );

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayHasKey('animals', $veterinarian);
        $this->assertArrayNotHasKey('presidents', $veterinarian);

        $animal = $veterinarian['animals'][0];

        $this->assertArrayHasKey('president', $animal);
        $this->assertEquals('bush', $animal->getAttribute('president')); // Check president is a value
        $this->assertArrayHasKey('zoo', $animal);
        $this->assertEquals('zoo1', $animal->getAttribute('zoo')); // Check zoo is a value

        $veterinarian = $database->findOne(
            'veterinarians',
            [
                Query::select([
                    'animals.*',
                    'animals.zoo.*',
                    'animals.president.*',
                ])
            ]
        );

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayHasKey('animals', $veterinarian);
        $this->assertArrayNotHasKey('presidents', $veterinarian);

        $animal = $veterinarian['animals'][0];

        $this->assertArrayHasKey('president', $animal);
        $this->assertEquals('Bush', $animal->getAttribute('president')->getAttribute('last_name')); // Check president is an object
        $this->assertArrayHasKey('zoo', $animal);
        $this->assertEquals('Bronx Zoo', $animal->getAttribute('zoo')->getAttribute('name')); // Check zoo is an object
    }

    public function testSimpleRelationshipPopulation(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Simple test case: user -> post (one-to-many)
        $database->createCollection('users_simple');
        $database->createCollection('posts_simple');

        $database->createAttribute('users_simple', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('posts_simple', 'title', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'users_simple',
            relatedCollection: 'posts_simple',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'posts',
            twoWayKey: 'author'
        );

        // Create some data
        $user = $database->createDocument('users_simple', new Document([
            '$id' => 'user1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'John Doe',
        ]));

        $post1 = $database->createDocument('posts_simple', new Document([
            '$id' => 'post1',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'First Post',
            'author' => 'user1',
        ]));

        $post2 = $database->createDocument('posts_simple', new Document([
            '$id' => 'post2',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Second Post',
            'author' => 'user1',
        ]));

        // Test: fetch user with posts populated
        $fetchedUser = $database->getDocument('users_simple', 'user1');
        $posts = $fetchedUser->getAttribute('posts', []);

        // Basic assertions
        $this->assertIsArray($posts, 'Posts should be an array');
        $this->assertCount(2, $posts, 'Should have 2 posts');

        if (!empty($posts)) {
            $this->assertInstanceOf(Document::class, $posts[0], 'First post should be a Document object');
            $this->assertEquals('First Post', $posts[0]->getAttribute('title'), 'First post title should be populated');
        }

        // Test: fetch posts with author populated
        $fetchedPosts = $database->find('posts_simple');

        $this->assertCount(2, $fetchedPosts, 'Should fetch 2 posts');

        if (!empty($fetchedPosts)) {
            $author = $fetchedPosts[0]->getAttribute('author');
            $this->assertInstanceOf(Document::class, $author, 'Author should be a Document object');
            $this->assertEquals('John Doe', $author->getAttribute('name'), 'Author name should be populated');
        }
    }

    public function testDeleteRelatedCollection(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('c1');
        $database->createCollection('c2');

        // ONE_TO_ONE
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c1');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c2');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c1');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // ONE_TO_MANY
        $database->createCollection('c2');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c1');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c2');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c1');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // RELATION_MANY_TO_ONE
        $database->createCollection('c2');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c1');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c2');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        $database->createCollection('c1');
        $database->createRelationship(
            collection: 'c1',
            relatedCollection: 'c2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true
        );

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));
    }

    public function testVirtualRelationsAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('v1');
        $database->createCollection('v2');

        /**
         * RELATION_ONE_TO_ONE
         * TwoWay is false no attribute is created on v2
         */
        $database->createRelationship(
            collection: 'v1',
            relatedCollection: 'v2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: false
        );

        try {
            $database->createDocument('v2', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
                'v1' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->createDocument('v2', new Document([
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
            $database->find('v2', [
                Query::equal('v1', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        /**
         * Success for later test update
         */
        $doc = $database->createDocument('v1', new Document([
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
            $database->updateDocument('v1', 'man', new Document([
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

        $database->deleteRelationship('v1', 'v2');

        /**
         * RELATION_ONE_TO_MANY
         * No attribute is created in V1 collection
         */
        $database->createRelationship(
            collection: 'v1',
            relatedCollection: 'v2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );

        try {
            $database->createDocument('v1', new Document([
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
            $database->createDocument('v1', new Document([
                '$permissions' => [],
                'v2' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->createDocument('v2', new Document([
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
        $doc = $database->createDocument('v2', new Document([
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
            $database->updateDocument('v1', 'v1_uid', new Document([
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
            $database->updateDocument('v1', 'v1_uid', new Document([
                '$permissions' => [],
                'v2' => 'v2_uid'
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->updateDocument('v2', 'v2_uid', new Document([
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
            $database->find('v2', [
                //@phpstan-ignore-next-line
                Query::equal('v1', [['doc1']]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        try {
            $database->find('v1', [
                Query::equal('v2', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        $database->deleteRelationship('v1', 'v2');

        /**
         * RELATION_MANY_TO_ONE
         * No attribute is created in V2 collection
         */
        $database->createRelationship(
            collection: 'v1',
            relatedCollection: 'v2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true
        );

        try {
            $database->createDocument('v1', new Document([
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
            $database->createDocument('v2', new Document([
                '$permissions' => [],
                'v1' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->createDocument('v2', new Document([
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
            $database->find('v2', [
                Query::equal('v1', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        /**
         * Success for later test update
         */
        $doc = $database->createDocument('v1', new Document([
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
            $database->updateDocument('v1', 'doc1', new Document([
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
            $database->updateDocument('v2', 'doc2', new Document([
                '$permissions' => [],
                'v1' => null
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        $database->deleteRelationship('v1', 'v2');

        /**
         * RELATION_MANY_TO_MANY
         * No attribute on V1/v2 collections only on junction table
         */
        $database->createRelationship(
            collection: 'v1',
            relatedCollection: 'v2',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'students',
            twoWayKey: 'classes'
        );

        try {
            $database->createDocument('v1', new Document([
                '$permissions' => [],
                'students' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->createDocument('v2', new Document([
                '$permissions' => [],
                'classes' => 'invalid_value',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->createDocument('v2', new Document([
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
            $database->find('v1', [
                Query::equal('students', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        try {
            $database->find('v2', [
                Query::equal('classes', ['virtual_attribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        /**
         * Success for later test update
         */

        $doc = $database->createDocument('v1', new Document([
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
            $database->updateDocument('v1', 'class1', new Document([
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
            $database->updateDocument('v1', 'class1', new Document([
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
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection("structure_1", [], [], [Permission::create(Role::any())]);
        $database->createCollection("structure_2", [], [], [Permission::create(Role::any())]);

        $database->createRelationship(
            collection: "structure_1",
            relatedCollection: "structure_2",
            type: Database::RELATION_ONE_TO_ONE,
        );

        try {
            $database->createDocument('structure_1', new Document([
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
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
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
            $database->createCollection("level{$i}", [$attribute], [], $permissions);
        }

        for ($i = 1; $i < 5; $i++) {
            $collectionId = $i;
            $relatedCollectionId = $i + 1;
            $database->createRelationship(
                collection: "level{$collectionId}",
                relatedCollection: "level{$relatedCollectionId}",
                type: Database::RELATION_ONE_TO_ONE,
                id: "level{$relatedCollectionId}"
            );
        }

        // Create document with relationship with nested data
        $level1 = $database->createDocument('level1', new Document([
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
        $database->updateDocument('level1', $level1->getId(), new Document($level1->getArrayCopy()));
        $updatedLevel1 = $database->getDocument('level1', $level1->getId());
        $this->assertEquals($level1, $updatedLevel1);

        try {
            $database->updateDocument('level1', $level1->getId(), $level1->setAttribute('name', 'haha'));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }
        $level1->setAttribute('name', 'Level 1');
        $database->updateCollection('level3', [
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

        $level1 = $database->updateDocument('level1', $level1->getId(), $level1);
        $this->assertEquals('updated value', $level1['level2']['level3']['name']);

        for ($i = 1; $i < 6; $i++) {
            $database->deleteCollection("level{$i}");
        }
    }



    public function testUpdateAttributeRenameRelationshipTwoWay(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('rn_rs_test_a');
        $database->createCollection('rn_rs_test_b');

        $database->createAttribute('rn_rs_test_b', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            'rn_rs_test_a',
            'rn_rs_test_b',
            Database::RELATION_ONE_TO_ONE,
            true
        );

        $docA = $database->createDocument('rn_rs_test_a', new Document([
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

        $docB = $database->getDocument('rn_rs_test_b', 'b1');
        $this->assertArrayHasKey('rn_rs_test_a', $docB->getAttributes());
        $this->assertEquals('B1', $docB->getAttribute('name'));

        // Rename attribute
        $database->updateRelationship(
            collection: 'rn_rs_test_a',
            id: 'rn_rs_test_b',
            newKey: 'rn_rs_test_b_renamed'
        );

        // Rename again
        $database->updateRelationship(
            collection: 'rn_rs_test_a',
            id: 'rn_rs_test_b_renamed',
            newKey: 'rn_rs_test_b_renamed_2'
        );

        // Check our data is OK
        $docA = $database->getDocument('rn_rs_test_a', $docA->getId());
        $this->assertArrayHasKey('rn_rs_test_b_renamed_2', $docA->getAttributes());
        $this->assertEquals($docB->getId(), $docA->getAttribute('rn_rs_test_b_renamed_2')['$id']);
    }

    public function testNoInvalidKeysWithRelationships(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $database->createCollection('species');
        $database->createCollection('creatures');
        $database->createCollection('characteristics');

        $database->createAttribute('species', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('creatures', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('characteristics', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'species',
            relatedCollection: 'creatures',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'creature',
            twoWayKey:'species'
        );
        $database->createRelationship(
            collection: 'creatures',
            relatedCollection: 'characteristics',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'characteristic',
            twoWayKey:'creature'
        );

        $species = $database->createDocument('species', new Document([
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
        $database->updateDocument('species', $species->getId(), new Document([
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

        $updatedSpecies = $database->getDocument('species', $species->getId());

        $this->assertEquals($species, $updatedSpecies);
    }

    public function testSelectRelationshipAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('make');
        $database->createCollection('model');

        $database->createAttribute('make', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('make', 'origin', Database::VAR_STRING, 255, true);
        $database->createAttribute('model', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('model', 'year', Database::VAR_INTEGER, 0, true);

        $database->createRelationship(
            collection: 'make',
            relatedCollection: 'model',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'models',
            twoWayKey: 'make',
        );

        $database->createDocument('make', new Document([
            '$id' => 'ford',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Ford',
            'origin' => 'USA',
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
        $make = $database->findOne('make', [
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
        $this->assertArrayHasKey('$id', $make);
        $this->assertArrayHasKey('$sequence', $make);
        $this->assertArrayHasKey('$permissions', $make);
        $this->assertArrayHasKey('$collection', $make);
        $this->assertArrayHasKey('$createdAt', $make);
        $this->assertArrayHasKey('$updatedAt', $make);

        // Select internal attributes
        $make = $database->findOne('make', [
            Query::select(['name', '$id']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayHasKey('name', $make);
        $this->assertArrayHasKey('$id', $make);
        $this->assertArrayHasKey('$sequence', $make);
        $this->assertArrayHasKey('$collection', $make);
        $this->assertArrayHasKey('$createdAt', $make);
        $this->assertArrayHasKey('$updatedAt', $make);
        $this->assertArrayHasKey('$permissions', $make);

        $make = $database->findOne('make', [
            Query::select(['name', '$sequence']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayHasKey('name', $make);
        $this->assertArrayHasKey('$id', $make);
        $this->assertArrayHasKey('$sequence', $make);
        $this->assertArrayHasKey('$collection', $make);
        $this->assertArrayHasKey('$createdAt', $make);
        $this->assertArrayHasKey('$updatedAt', $make);
        $this->assertArrayHasKey('$permissions', $make);

        $make = $database->findOne('make', [
            Query::select(['name', '$collection']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayHasKey('name', $make);
        $this->assertArrayHasKey('$id', $make);
        $this->assertArrayHasKey('$sequence', $make);
        $this->assertArrayHasKey('$collection', $make);
        $this->assertArrayHasKey('$createdAt', $make);
        $this->assertArrayHasKey('$updatedAt', $make);
        $this->assertArrayHasKey('$permissions', $make);

        $make = $database->findOne('make', [
            Query::select(['name', '$createdAt']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayHasKey('name', $make);
        $this->assertArrayHasKey('$id', $make);
        $this->assertArrayHasKey('$sequence', $make);
        $this->assertArrayHasKey('$collection', $make);
        $this->assertArrayHasKey('$createdAt', $make);
        $this->assertArrayHasKey('$updatedAt', $make);
        $this->assertArrayHasKey('$permissions', $make);

        $make = $database->findOne('make', [
            Query::select(['name', '$updatedAt']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayHasKey('name', $make);
        $this->assertArrayHasKey('$id', $make);
        $this->assertArrayHasKey('$sequence', $make);
        $this->assertArrayHasKey('$collection', $make);
        $this->assertArrayHasKey('$createdAt', $make);
        $this->assertArrayHasKey('$updatedAt', $make);
        $this->assertArrayHasKey('$permissions', $make);

        $make = $database->findOne('make', [
            Query::select(['name', '$permissions']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertArrayHasKey('name', $make);
        $this->assertArrayHasKey('$id', $make);
        $this->assertArrayHasKey('$sequence', $make);
        $this->assertArrayHasKey('$collection', $make);
        $this->assertArrayHasKey('$createdAt', $make);
        $this->assertArrayHasKey('$updatedAt', $make);
        $this->assertArrayHasKey('$permissions', $make);

        // Select all parent attributes, some child attributes
        $make = $database->findOne('make', [
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
        $make = $database->findOne('make', [
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
        $make = $database->findOne('make', [
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
        $make = $database->findOne('make', [
            Query::select(['name']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }
        $this->assertEquals('Ford', $make['name']);
        $this->assertArrayNotHasKey('models', $make);

        // Select some parent attributes, all child attributes
        $make = $database->findOne('make', [
            Query::select(['name', 'models.*']),
        ]);

        $this->assertEquals('Ford', $make['name']);
        $this->assertEquals(2, \count($make['models']));

        /*
         * FROM CHILD TO PARENT
         */

        // Select some parent attributes, some child attributes
        $model = $database->findOne('model', [
            Query::select(['name', 'make.name']),
        ]);

        $this->assertEquals('Fiesta', $model['name']);
        $this->assertEquals('Ford', $model['make']['name']);
        $this->assertArrayNotHasKey('origin', $model['make']);
        $this->assertArrayNotHasKey('year', $model);
        $this->assertArrayHasKey('name', $model);

        // Select all parent attributes, some child attributes
        $model = $database->findOne('model', [
            Query::select(['*', 'make.name']),
        ]);

        $this->assertEquals('Fiesta', $model['name']);
        $this->assertEquals('Ford', $model['make']['name']);
        $this->assertArrayHasKey('year', $model);

        // Select all parent attributes, all child attributes
        $model = $database->findOne('model', [
            Query::select(['*', 'make.*']),
        ]);

        $this->assertEquals('Fiesta', $model['name']);
        $this->assertEquals('Ford', $model['make']['name']);
        $this->assertArrayHasKey('year', $model);
        $this->assertArrayHasKey('name', $model['make']);

        // Select all parent attributes, no child attributes
        $model = $database->findOne('model', [
            Query::select(['*']),
        ]);

        $this->assertEquals('Fiesta', $model['name']);
        $this->assertArrayHasKey('make', $model);
        $this->assertArrayHasKey('year', $model);

        // Select some parent attributes, all child attributes
        $model = $database->findOne('model', [
            Query::select(['name', 'make.*']),
        ]);

        $this->assertEquals('Fiesta', $model['name']);
        $this->assertEquals('Ford', $model['make']['name']);
        $this->assertEquals('USA', $model['make']['origin']);
    }

    public function testInheritRelationshipPermissions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('lawns', permissions: [Permission::create(Role::any())], documentSecurity: true);
        $database->createCollection('trees', permissions: [Permission::create(Role::any())], documentSecurity: true);
        $database->createCollection('birds', permissions: [Permission::create(Role::any())], documentSecurity: true);

        $database->createAttribute('lawns', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('trees', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('birds', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'lawns',
            relatedCollection: 'trees',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'lawn',
            onDelete: Database::RELATION_MUTATE_CASCADE,
        );
        $database->createRelationship(
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

        $database->createDocument('lawns', new Document([
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

        $lawn1 = $database->getDocument('lawns', 'lawn1');
        $this->assertEquals($permissions, $lawn1->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]['birds'][0]->getPermissions());
        $this->assertEquals($permissions, $lawn1['trees'][0]['birds'][1]->getPermissions());

        $tree1 = $database->getDocument('trees', 'tree1');
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
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());
        $lawn1 = $database->getDocument('lawns', 'lawn1');
        $this->assertEquals('Lawn 1', $lawn1['name']);

        // Try update root document
        try {
            $database->updateDocument(
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
            $database->deleteDocument(
                'lawns',
                $lawn1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        $tree1 = $database->getDocument('trees', 'tree1');

        // Try update nested document
        try {
            $database->updateDocument(
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
            $database->deleteDocument(
                'trees',
                $tree1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        $bird1 = $database->getDocument('birds', 'bird1');

        // Try update multi-level nested document
        try {
            $database->updateDocument(
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
            $database->deleteDocument(
                'birds',
                $bird1->getId(),
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing "delete" permission for role "user:user2". Only "["any"]" scopes are allowed and "["user:user2"]" was given.', $e->getMessage());
        }

        Authorization::setRole(Role::user('user1')->toString());

        $bird1 = $database->getDocument('birds', 'bird1');

        // Try update multi-level nested document
        $bird1 = $database->updateDocument(
            'birds',
            $bird1->getId(),
            $bird1->setAttribute('name', 'Bird 1 Updated')
        );

        $this->assertEquals('Bird 1 Updated', $bird1['name']);

        Authorization::setRole(Role::user('user2')->toString());

        // Try delete multi-level nested document
        $deleted = $database->deleteDocument(
            'birds',
            $bird1->getId(),
        );

        $this->assertEquals(true, $deleted);
        $tree1 = $database->getDocument('trees', 'tree1');
        $this->assertEquals(1, count($tree1['birds']));

        // Try update nested document
        $tree1 = $database->updateDocument(
            'trees',
            $tree1->getId(),
            $tree1->setAttribute('name', 'Tree 1 Updated')
        );

        $this->assertEquals('Tree 1 Updated', $tree1['name']);

        // Try delete nested document
        $deleted = $database->deleteDocument(
            'trees',
            $tree1->getId(),
        );

        $this->assertEquals(true, $deleted);
        $lawn1 = $database->getDocument('lawns', 'lawn1');
        $this->assertEquals(0, count($lawn1['trees']));

        // Create document with no permissions
        $database->createDocument('lawns', new Document([
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

        $lawn2 = $database->getDocument('lawns', 'lawn2');
        $this->assertEquals(true, $lawn2->isEmpty());

        $tree2 = $database->getDocument('trees', 'tree2');
        $this->assertEquals(true, $tree2->isEmpty());

        $bird3 = $database->getDocument('birds', 'bird3');
        $this->assertEquals(true, $bird3->isEmpty());
    }

    public function testCreateRelationshipMissingCollection(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Collection not found');

        $database->createRelationship(
            collection: 'missing',
            relatedCollection: 'missing',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateRelationshipMissingRelatedCollection(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('test');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Related collection not found');

        $database->createRelationship(
            collection: 'test',
            relatedCollection: 'missing',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateDuplicateRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('test1');
        $database->createCollection('test2');

        $database->createRelationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Attribute already exists');

        $database->createRelationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
    }

    public function testCreateInvalidRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('test3');
        $database->createCollection('test4');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid relationship type');

        $database->createRelationship(
            collection: 'test3',
            relatedCollection: 'test4',
            type: 'invalid',
            twoWay: true,
        );
    }


    public function testDeleteMissingRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        try {
            $database->deleteRelationship('test', 'test2');
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertEquals('Relationship not found', $e->getMessage());
        }
    }

    public function testCreateInvalidIntValueRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('invalid1');
        $database->createCollection('invalid2');

        $database->createRelationship(
            collection: 'invalid1',
            relatedCollection: 'invalid2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $database->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => 10,
        ]));
    }

    /**
     * @depends testCreateInvalidIntValueRelationship
     */
    public function testCreateInvalidObjectValueRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $database->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => new \stdClass(),
        ]));
    }

    /**
     * @depends testCreateInvalidIntValueRelationship
     */
    public function testCreateInvalidArrayIntValueRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createRelationship(
            collection: 'invalid1',
            relatedCollection: 'invalid2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'invalid3',
            twoWayKey: 'invalid4',
        );

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $database->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid3' => [10],
        ]));
    }

    public function testCreateEmptyValueRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('null1');
        $database->createCollection('null2');

        $database->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        $database->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'null3',
            twoWayKey: 'null4',
        );
        $database->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'null4',
            twoWayKey: 'null5',
        );
        $database->createRelationship(
            collection: 'null1',
            relatedCollection: 'null2',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'null6',
            twoWayKey: 'null7',
        );

        $document = $database->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null2' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null2'));

        $document = $database->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null1' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null1'));

        $document = $database->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null3' => null,
        ]));

        // One to many will be empty array instead of null
        $this->assertEquals([], $document->getAttribute('null3'));

        $document = $database->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null4' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null4'));

        $document = $database->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null4' => null,
        ]));

        $this->assertEquals(null, $document->getAttribute('null4'));

        $document = $database->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null5' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null5'));

        $document = $database->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null6' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null6'));

        $document = $database->createDocument('null2', new Document([
            '$id' => ID::unique(),
            'null7' => null,
        ]));

        $this->assertEquals([], $document->getAttribute('null7'));
    }

    public function testUpdateRelationshipToExistingKey(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('ovens');
        $database->createCollection('cakes');

        $database->createAttribute('ovens', 'maxTemp', Database::VAR_INTEGER, 0, true);
        $database->createAttribute('ovens', 'owner', Database::VAR_STRING, 255, true);
        $database->createAttribute('cakes', 'height', Database::VAR_INTEGER, 0, true);
        $database->createAttribute('cakes', 'colour', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'ovens',
            relatedCollection: 'cakes',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'cakes',
            twoWayKey: 'oven'
        );

        try {
            $database->updateRelationship('ovens', 'cakes', newKey: 'owner');
            $this->fail('Failed to throw exception');
        } catch (DuplicateException $e) {
            $this->assertEquals('Relationship already exists', $e->getMessage());
        }

        try {
            $database->updateRelationship('ovens', 'cakes', newTwoWayKey: 'height');
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
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $database->createCollection('userProfiles', [
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
        $database->createCollection('links', [
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
        $database->createCollection('videos', [
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
        $database->createCollection('products', [
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
        $database->createCollection('settings', [
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
        $database->createCollection('appearance', [
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
        $database->createCollection('group', [
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
        $database->createCollection('community', [
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

        $database->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'links',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'links'
        );

        $database->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'videos',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'videos'
        );

        $database->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'products',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'products',
            twoWayKey: 'userProfile',
        );

        $database->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'settings',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'settings'
        );

        $database->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'appearance',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'appearance'
        );

        $database->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'group',
            type: Database::RELATION_MANY_TO_ONE,
            id: 'group'
        );

        $database->createRelationship(
            collection: 'userProfiles',
            relatedCollection: 'community',
            type: Database::RELATION_MANY_TO_ONE,
            id: 'community'
        );

        $profile = $database->createDocument('userProfiles', new Document([
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

        $updatedProfile = $database->updateDocument('userProfiles', '1', $profile);

        $this->assertEquals('New Link Value', $updatedProfile->getAttribute('links')[0]->getAttribute('title'));
        $this->assertEquals('New Meta Title', $updatedProfile->getAttribute('settings')->getAttribute('metaTitle'));
        $this->assertEquals('New Group Name', $updatedProfile->getAttribute('group')->getAttribute('name'));

        // This is the point of test, related documents should be present if they are not updated
        $this->assertEquals('Video 1', $updatedProfile->getAttribute('videos')[0]->getAttribute('title'));
        $this->assertEquals('Product 1', $updatedProfile->getAttribute('products')[0]->getAttribute('title'));
        $this->assertEquals('Meta Title', $updatedProfile->getAttribute('appearance')->getAttribute('metaTitle'));
        $this->assertEquals('Community 1', $updatedProfile->getAttribute('community')->getAttribute('name'));

        // updating document using two way key in one to many relationship
        $product = $database->getDocument('products', 'product1');
        $product->setAttribute('userProfile', [
            '$id' => '1',
            'username' => 'updated user value',
        ]);
        $updatedProduct = $database->updateDocument('products', 'product1', $product);
        $this->assertEquals('updated user value', $updatedProduct->getAttribute('userProfile')->getAttribute('username'));
        $this->assertEquals('Product 1', $updatedProduct->getAttribute('title'));
        $this->assertEquals('product1', $updatedProduct->getId());
        $this->assertEquals('1', $updatedProduct->getAttribute('userProfile')->getId());

        $database->deleteCollection('userProfiles');
        $database->deleteCollection('links');
        $database->deleteCollection('settings');
        $database->deleteCollection('group');
        $database->deleteCollection('community');
        $database->deleteCollection('videos');
        $database->deleteCollection('products');
        $database->deleteCollection('appearance');
    }

    /**
     * Test that nested relationships are populated for all documents in a multi-document query
     * Covers bug: https://github.com/appwrite/appwrite/issues/10552
     */
    public function testMultiDocumentNestedRelationships(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Create collections: car -> customer -> inspection
        $database->createCollection('car');
        $database->createAttribute('car', 'plate_number', Database::VAR_STRING, 255, true);

        $database->createCollection('customer');
        $database->createAttribute('customer', 'name', Database::VAR_STRING, 255, true);

        $database->createCollection('inspection');
        $database->createAttribute('inspection', 'type', Database::VAR_STRING, 255, true);

        // Create relationships
        // car -> customer (many to one, one-way to avoid circular references)
        $database->createRelationship(
            collection: 'car',
            relatedCollection: 'customer',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: false,
            id: 'customer',
        );

        // customer -> inspection (one to many, one-way)
        $database->createRelationship(
            collection: 'customer',
            relatedCollection: 'inspection',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: false,
            id: 'inspections',
        );

        // Create test data - customers with inspections first
        $database->createDocument('inspection', new Document([
            '$id' => 'inspection1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'type' => 'annual',
        ]));
        $database->createDocument('inspection', new Document([
            '$id' => 'inspection2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'type' => 'safety',
        ]));
        $database->createDocument('inspection', new Document([
            '$id' => 'inspection3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'type' => 'emissions',
        ]));
        $database->createDocument('inspection', new Document([
            '$id' => 'inspection4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'type' => 'annual',
        ]));
        $database->createDocument('inspection', new Document([
            '$id' => 'inspection5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'type' => 'safety',
        ]));

        $database->createDocument('customer', new Document([
            '$id' => 'customer1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Customer 1',
            'inspections' => ['inspection1', 'inspection2'],
        ]));

        $database->createDocument('customer', new Document([
            '$id' => 'customer2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Customer 2',
            'inspections' => ['inspection3', 'inspection4'],
        ]));

        $database->createDocument('customer', new Document([
            '$id' => 'customer3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Customer 3',
            'inspections' => ['inspection5'],
        ]));

        $car1 = $database->createDocument('car', new Document([
            '$id' => 'car1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'plate_number' => 'ABC123',
            'customer' => 'customer1',
        ]));

        $car2 = $database->createDocument('car', new Document([
            '$id' => 'car2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'plate_number' => 'DEF456',
            'customer' => 'customer2',
        ]));

        $car3 = $database->createDocument('car', new Document([
            '$id' => 'car3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'plate_number' => 'GHI789',
            'customer' => 'customer3',
        ]));

        // Query all cars with nested relationship selections
        $cars = $database->find('car', [
            Query::select([
                '*',
                'customer.*',
                'customer.inspections.type',
            ]),
        ]);

        $this->assertCount(3, $cars);

        $this->assertEquals('ABC123', $cars[0]['plate_number']);
        $this->assertEquals('Customer 1', $cars[0]['customer']['name']);
        $this->assertCount(2, $cars[0]['customer']['inspections']);
        $this->assertEquals('annual', $cars[0]['customer']['inspections'][0]['type']);
        $this->assertEquals('safety', $cars[0]['customer']['inspections'][1]['type']);

        $this->assertEquals('DEF456', $cars[1]['plate_number']);
        $this->assertEquals('Customer 2', $cars[1]['customer']['name']);
        $this->assertCount(2, $cars[1]['customer']['inspections']);
        $this->assertEquals('emissions', $cars[1]['customer']['inspections'][0]['type']);
        $this->assertEquals('annual', $cars[1]['customer']['inspections'][1]['type']);

        $this->assertEquals('GHI789', $cars[2]['plate_number']);
        $this->assertEquals('Customer 3', $cars[2]['customer']['name']);
        $this->assertCount(1, $cars[2]['customer']['inspections']);
        $this->assertEquals('safety', $cars[2]['customer']['inspections'][0]['type']);

        // Test with createDocuments as well
        $database->deleteDocument('car', 'car1');
        $database->deleteDocument('car', 'car2');
        $database->deleteDocument('car', 'car3');

        $database->createDocuments('car', [
            new Document([
                '$id' => 'car1',
                '$permissions' => [Permission::read(Role::any())],
                'plate_number' => 'ABC123',
                'customer' => 'customer1',
            ]),
            new Document([
                '$id' => 'car2',
                '$permissions' => [Permission::read(Role::any())],
                'plate_number' => 'DEF456',
                'customer' => 'customer2',
            ]),
            new Document([
                '$id' => 'car3',
                '$permissions' => [Permission::read(Role::any())],
                'plate_number' => 'GHI789',
                'customer' => 'customer3',
            ]),
        ]);

        $cars = $database->find('car', [
            Query::select([
                '*',
                'customer.*',
                'customer.inspections.type',
            ]),
        ]);

        // Verify all cars still have nested relationships after batch create
        $this->assertCount(3, $cars);
        $this->assertCount(2, $cars[0]['customer']['inspections']);
        $this->assertCount(2, $cars[1]['customer']['inspections']);
        $this->assertCount(1, $cars[2]['customer']['inspections']);

        // Clean up
        $database->deleteCollection('inspection');
        $database->deleteCollection('car');
        $database->deleteCollection('customer');
    }

    /**
     * Test that nested document creation properly populates relationships at all depths.
     * This test verifies the fix for the depth handling bug where populateDocumentsRelationships()
     * would early return for non-zero depth, causing nested documents to not have their relationships populated.
     */
    public function testNestedDocumentCreationWithDepthHandling(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Create three collections with chained relationships: Order -> Product -> Store
        $database->createCollection('order_depth_test');
        $database->createCollection('product_depth_test');
        $database->createCollection('store_depth_test');

        $database->createAttribute('order_depth_test', 'orderNumber', Database::VAR_STRING, 255, true);
        $database->createAttribute('product_depth_test', 'productName', Database::VAR_STRING, 255, true);
        $database->createAttribute('store_depth_test', 'storeName', Database::VAR_STRING, 255, true);

        // Order -> Product (many-to-one)
        $database->createRelationship(
            collection: 'order_depth_test',
            relatedCollection: 'product_depth_test',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'product',
            twoWayKey: 'orders'
        );

        // Product -> Store (many-to-one)
        $database->createRelationship(
            collection: 'product_depth_test',
            relatedCollection: 'store_depth_test',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'store',
            twoWayKey: 'products'
        );

        // First, create a store that will be referenced by the nested product
        $store = $database->createDocument('store_depth_test', new Document([
            '$id' => 'store1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'storeName' => 'Main Store',
        ]));

        $this->assertEquals('store1', $store->getId());
        $this->assertEquals('Main Store', $store->getAttribute('storeName'));

        // Create an order with a nested product that references the existing store
        // The nested product is created at depth 1
        // With the bug, the product's relationships (including 'store') would not be populated
        // With the fix, the product's 'store' relationship should be properly populated
        $order = $database->createDocument('order_depth_test', new Document([
            '$id' => 'order1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'orderNumber' => 'ORD-001',
            'product' => [
                '$id' => 'product1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'productName' => 'Widget',
                'store' => 'store1', // Reference to existing store
            ],
        ]));

        // Verify the order was created
        $this->assertEquals('order1', $order->getId());
        $this->assertEquals('ORD-001', $order->getAttribute('orderNumber'));

        // Verify the nested product relationship is populated (depth 1)
        $this->assertArrayHasKey('product', $order);
        $product = $order->getAttribute('product');
        $this->assertInstanceOf(Document::class, $product);
        $this->assertEquals('product1', $product->getId());
        $this->assertEquals('Widget', $product->getAttribute('productName'));

        // CRITICAL: Verify the product's store relationship is populated (depth 2)
        // This is the key assertion that would fail with the bug
        $this->assertArrayHasKey('store', $product);
        $productStore = $product->getAttribute('store');
        $this->assertInstanceOf(Document::class, $productStore);
        $this->assertEquals('store1', $productStore->getId());
        $this->assertEquals('Main Store', $productStore->getAttribute('storeName'));

        // Also test with update - create another order and update it with nested product
        $order2 = $database->createDocument('order_depth_test', new Document([
            '$id' => 'order2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'orderNumber' => 'ORD-002',
        ]));

        // Update order2 to add a nested product
        $order2Updated = $database->updateDocument('order_depth_test', 'order2', $order2->setAttribute('product', [
            '$id' => 'product2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'productName' => 'Gadget',
            'store' => 'store1',
        ]));

        // Verify the updated order has the nested product with populated store
        $this->assertEquals('order2', $order2Updated->getId());
        $product2 = $order2Updated->getAttribute('product');
        $this->assertInstanceOf(Document::class, $product2);
        $this->assertEquals('product2', $product2->getId());

        // Verify the product's store is populated after update
        $this->assertArrayHasKey('store', $product2);
        $product2Store = $product2->getAttribute('store');
        $this->assertInstanceOf(Document::class, $product2Store);
        $this->assertEquals('store1', $product2Store->getId());

        // Clean up
        $database->deleteCollection('order_depth_test');
        $database->deleteCollection('product_depth_test');
        $database->deleteCollection('store_depth_test');
    }

    /**
     * Test filtering by relationship fields using dot-path notation
     */
    public function testRelationshipFiltering(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Create Author -> Posts relationship
        $database->createCollection('authors_filter');
        $database->createCollection('posts_filter');

        $database->createAttribute('authors_filter', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('authors_filter', 'age', Database::VAR_INTEGER, 0, true);
        $database->createAttribute('posts_filter', 'title', Database::VAR_STRING, 255, true);
        $database->createAttribute('posts_filter', 'published', Database::VAR_BOOLEAN, 0, true);

        $database->createRelationship(
            collection: 'authors_filter',
            relatedCollection: 'posts_filter',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'posts',
            twoWayKey: 'author'
        );

        // Create test data
        $author1 = $database->createDocument('authors_filter', new Document([
            '$id' => 'author1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Alice',
            'age' => 30,
        ]));

        $author2 = $database->createDocument('authors_filter', new Document([
            '$id' => 'author2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Bob',
            'age' => 25,
        ]));

        // Create posts
        $database->createDocument('posts_filter', new Document([
            '$id' => 'post1',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Alice Post 1',
            'published' => true,
            'author' => 'author1',
        ]));

        $database->createDocument('posts_filter', new Document([
            '$id' => 'post2',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Alice Post 2',
            'published' => true,
            'author' => 'author1',
        ]));

        $database->createDocument('posts_filter', new Document([
            '$id' => 'post3',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Bob Post',
            'published' => true,
            'author' => 'author2',
        ]));

        // Test: Filter posts by author name
        $posts = $database->find('posts_filter', [
            Query::equal('author.name', ['Alice']),
        ]);
        $this->assertCount(2, $posts);
        $this->assertEquals('post1', $posts[0]->getId());
        $this->assertEquals('post2', $posts[1]->getId());

        // Test: Filter posts by author age
        $posts = $database->find('posts_filter', [
            Query::lessThan('author.age', 30),
        ]);
        $this->assertCount(1, $posts);
        $this->assertEquals('post3', $posts[0]->getId());

        // Test: Filter authors by their posts' published status
        $authors = $database->find('authors_filter', [
            Query::equal('posts.published', [true]),
        ]);
        $this->assertCount(2, $authors); // Both authors have published posts

        // Clean up ONE_TO_MANY test
        $database->deleteCollection('authors_filter');
        $database->deleteCollection('posts_filter');

        // ==================== Test ONE_TO_ONE relationships ====================
        $database->createCollection('users_oto');
        $database->createCollection('profiles_oto');

        $database->createAttribute('users_oto', 'username', Database::VAR_STRING, 255, true);
        $database->createAttribute('profiles_oto', 'bio', Database::VAR_STRING, 255, true);

        // ONE_TO_ONE with twoWay=true
        $database->createRelationship(
            collection: 'users_oto',
            relatedCollection: 'profiles_oto',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'profile',
            twoWayKey: 'user'
        );

        $user1 = $database->createDocument('users_oto', new Document([
            '$id' => 'user1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'username' => 'alice',
        ]));

        $profile1 = $database->createDocument('profiles_oto', new Document([
            '$id' => 'profile1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'bio' => 'Software Engineer',
            'user' => 'user1',
        ]));

        // Test: Filter profiles by user username
        $profiles = $database->find('profiles_oto', [
            Query::equal('user.username', ['alice']),
        ]);
        $this->assertCount(1, $profiles);
        $this->assertEquals('profile1', $profiles[0]->getId());

        // Test: Filter users by profile bio
        $users = $database->find('users_oto', [
            Query::equal('profile.bio', ['Software Engineer']),
        ]);
        $this->assertCount(1, $users);
        $this->assertEquals('user1', $users[0]->getId());

        // Clean up ONE_TO_ONE test
        $database->deleteCollection('users_oto');
        $database->deleteCollection('profiles_oto');

        // ==================== Test MANY_TO_ONE relationships ====================
        $database->createCollection('comments_mto');
        $database->createCollection('users_mto');

        $database->createAttribute('comments_mto', 'content', Database::VAR_STRING, 255, true);
        $database->createAttribute('users_mto', 'name', Database::VAR_STRING, 255, true);

        // MANY_TO_ONE with twoWay=true
        $database->createRelationship(
            collection: 'comments_mto',
            relatedCollection: 'users_mto',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'commenter',
            twoWayKey: 'comments'
        );

        $userA = $database->createDocument('users_mto', new Document([
            '$id' => 'userA',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Alice',
        ]));

        $comment1 = $database->createDocument('comments_mto', new Document([
            '$id' => 'comment1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'content' => 'Great post!',
            'commenter' => 'userA',
        ]));

        $comment2 = $database->createDocument('comments_mto', new Document([
            '$id' => 'comment2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'content' => 'Nice work!',
            'commenter' => 'userA',
        ]));

        // Test: Filter comments by commenter name
        $comments = $database->find('comments_mto', [
            Query::equal('commenter.name', ['Alice']),
        ]);
        $this->assertCount(2, $comments);

        // Test: Filter users by their comments' content
        $users = $database->find('users_mto', [
            Query::equal('comments.content', ['Great post!']),
        ]);
        $this->assertCount(1, $users);
        $this->assertEquals('userA', $users[0]->getId());

        // Clean up MANY_TO_ONE test
        $database->deleteCollection('comments_mto');
        $database->deleteCollection('users_mto');

        // ==================== Test MANY_TO_MANY relationships ====================
        $database->createCollection('students_mtm');
        $database->createCollection('courses_mtm');

        $database->createAttribute('students_mtm', 'studentName', Database::VAR_STRING, 255, true);
        $database->createAttribute('courses_mtm', 'courseName', Database::VAR_STRING, 255, true);

        // MANY_TO_MANY
        $database->createRelationship(
            collection: 'students_mtm',
            relatedCollection: 'courses_mtm',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'enrolledCourses',
            twoWayKey: 'students'
        );

        $student1 = $database->createDocument('students_mtm', new Document([
            '$id' => 'student1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'studentName' => 'John',
        ]));

        $course1 = $database->createDocument('courses_mtm', new Document([
            '$id' => 'course1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'courseName' => 'Physics',
            'students' => ['student1'],
        ]));

        // Test: Filter students by enrolled course name
        $students = $database->find('students_mtm', [
            Query::equal('enrolledCourses.courseName', ['Physics']),
        ]);
        $this->assertCount(1, $students);
        $this->assertEquals('student1', $students[0]->getId());

        // Test: Filter courses by student name
        $courses = $database->find('courses_mtm', [
            Query::equal('students.studentName', ['John']),
        ]);
        $this->assertCount(1, $courses);
        $this->assertEquals('course1', $courses[0]->getId());

        // Clean up MANY_TO_MANY test
        $database->deleteCollection('students_mtm');
        $database->deleteCollection('courses_mtm');
    }

    /**
     * Comprehensive test for all query types on relationships
     */
    public function testRelationshipQueryTypes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Setup test collections
        $database->createCollection('products_qt');
        $database->createCollection('vendors_qt');

        $database->createAttribute('products_qt', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('products_qt', 'price', Database::VAR_FLOAT, 0, true);
        $database->createAttribute('vendors_qt', 'company', Database::VAR_STRING, 255, true);
        $database->createAttribute('vendors_qt', 'rating', Database::VAR_FLOAT, 0, true);
        $database->createAttribute('vendors_qt', 'email', Database::VAR_STRING, 255, true);
        $database->createAttribute('vendors_qt', 'verified', Database::VAR_BOOLEAN, 0, true);

        $database->createRelationship(
            collection: 'products_qt',
            relatedCollection: 'vendors_qt',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'vendor',
            twoWayKey: 'products'
        );

        // Create test vendors
        $database->createDocument('vendors_qt', new Document([
            '$id' => 'vendor1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'company' => 'Acme Corp',
            'rating' => 4.5,
            'email' => 'sales@acme.com',
            'verified' => true,
        ]));

        $database->createDocument('vendors_qt', new Document([
            '$id' => 'vendor2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'company' => 'TechSupply Inc',
            'rating' => 3.8,
            'email' => 'info@techsupply.com',
            'verified' => true,
        ]));

        $database->createDocument('vendors_qt', new Document([
            '$id' => 'vendor3',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'company' => 'Budget Vendors',
            'rating' => 2.5,
            'email' => 'contact@budget.com',
            'verified' => false,
        ]));

        // Create test products
        $database->createDocument('products_qt', new Document([
            '$id' => 'product1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Widget A',
            'price' => 19.99,
            'vendor' => 'vendor1',
        ]));

        $database->createDocument('products_qt', new Document([
            '$id' => 'product2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Widget B',
            'price' => 29.99,
            'vendor' => 'vendor2',
        ]));

        $database->createDocument('products_qt', new Document([
            '$id' => 'product3',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Widget C',
            'price' => 9.99,
            'vendor' => 'vendor3',
        ]));

        // Test: Query::equal()
        $products = $database->find('products_qt', [
            Query::equal('vendor.company', ['Acme Corp'])
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Test: Query::notEqual()
        $products = $database->find('products_qt', [
            Query::notEqual('vendor.company', ['Budget Vendors'])
        ]);
        $this->assertCount(2, $products);

        // Test: Query::lessThan()
        $products = $database->find('products_qt', [
            Query::lessThan('vendor.rating', 4.0)
        ]);
        $this->assertCount(2, $products); // vendor2 (3.8) and vendor3 (2.5)

        // Test: Query::lessThanEqual()
        $products = $database->find('products_qt', [
            Query::lessThanEqual('vendor.rating', 3.8)
        ]);
        $this->assertCount(2, $products);

        // Test: Query::greaterThan()
        $products = $database->find('products_qt', [
            Query::greaterThan('vendor.rating', 4.0)
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Test: Query::greaterThanEqual()
        $products = $database->find('products_qt', [
            Query::greaterThanEqual('vendor.rating', 3.8)
        ]);
        $this->assertCount(2, $products); // vendor1 (4.5) and vendor2 (3.8)

        // Test: Query::startsWith()
        $products = $database->find('products_qt', [
            Query::startsWith('vendor.email', 'sales@')
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Test: Query::endsWith()
        $products = $database->find('products_qt', [
            Query::endsWith('vendor.email', '.com')
        ]);
        $this->assertCount(3, $products);

        // Test: Query::contains()
        $products = $database->find('products_qt', [
            Query::contains('vendor.company', ['Corp'])
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Test: Boolean query
        $products = $database->find('products_qt', [
            Query::equal('vendor.verified', [true])
        ]);
        $this->assertCount(2, $products); // vendor1 and vendor2 are verified

        $products = $database->find('products_qt', [
            Query::equal('vendor.verified', [false])
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product3', $products[0]->getId());

        // Test: Multiple conditions on same relationship (query grouping optimization)
        $products = $database->find('products_qt', [
            Query::greaterThan('vendor.rating', 3.0),
            Query::equal('vendor.verified', [true]),
            Query::startsWith('vendor.company', 'Acme')
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Clean up
        $database->deleteCollection('products_qt');
        $database->deleteCollection('vendors_qt');
    }

    /**
     * Test edge cases and error scenarios for relationship queries
     */
    public function testRelationshipQueryEdgeCases(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Setup test collections
        $database->createCollection('orders_edge');
        $database->createCollection('customers_edge');

        $database->createAttribute('orders_edge', 'orderNumber', Database::VAR_STRING, 255, true);
        $database->createAttribute('orders_edge', 'total', Database::VAR_FLOAT, 0, true);
        $database->createAttribute('customers_edge', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('customers_edge', 'age', Database::VAR_INTEGER, 0, true);

        $database->createRelationship(
            collection: 'orders_edge',
            relatedCollection: 'customers_edge',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'customer',
            twoWayKey: 'orders'
        );

        // Create customer
        $database->createDocument('customers_edge', new Document([
            '$id' => 'customer1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'John Doe',
            'age' => 30,
        ]));

        // Create order
        $database->createDocument('orders_edge', new Document([
            '$id' => 'order1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'orderNumber' => 'ORD001',
            'total' => 100.00,
            'customer' => 'customer1',
        ]));

        // Edge Case 1: Query with no matching results
        $orders = $database->find('orders_edge', [
            Query::equal('customer.name', ['Jane Doe'])
        ]);
        $this->assertCount(0, $orders);

        // Edge Case 2: Query with impossible condition (combines to empty set)
        $orders = $database->find('orders_edge', [
            Query::equal('customer.name', ['John Doe']),
            Query::equal('customer.age', [25]) // John is 30, not 25
        ]);
        $this->assertCount(0, $orders);

        // Edge Case 3: Query on non-existent relationship field
        try {
            $orders = $database->find('orders_edge', [
                Query::equal('nonexistent.field', ['value'])
            ]);
            // Should return empty or throw - either is acceptable
            $this->assertCount(0, $orders);
        } catch (\Exception $e) {
            // Expected - non-existent relationship
            $this->assertTrue(true);
        }

        // Edge Case 4: Empty array values (should throw exception)
        try {
            $orders = $database->find('orders_edge', [
                Query::equal('customer.name', [])
            ]);
            $this->fail('Expected exception for empty array values');
        } catch (\Exception $e) {
            // Expected - empty array values are invalid
            $this->assertStringContainsString('at least one value', $e->getMessage());
        }

        // Edge Case 5: Null or missing relationship
        $database->createDocument('orders_edge', new Document([
            '$id' => 'order2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'orderNumber' => 'ORD002',
            'total' => 50.00,
            // No customer relationship
        ]));

        $orders = $database->find('orders_edge', [
            Query::equal('customer.name', ['John Doe'])
        ]);
        $this->assertCount(1, $orders); // Only order1 has a customer

        // Edge Case 6: Combining relationship query with regular query
        $orders = $database->find('orders_edge', [
            Query::equal('customer.name', ['John Doe']),
            Query::greaterThan('total', 75.00)
        ]);
        $this->assertCount(1, $orders);
        $this->assertEquals('order1', $orders[0]->getId());

        // Edge Case 7: Query with limit and offset
        $orders = $database->find('orders_edge', [
            Query::equal('customer.name', ['John Doe']),
            Query::limit(1),
            Query::offset(0)
        ]);
        $this->assertCount(1, $orders);

        // Clean up
        $database->deleteCollection('orders_edge');
        $database->deleteCollection('customers_edge');
    }

    /**
     * Test relationship queries from parent side with virtual fields
     */
    public function testRelationshipQueryParentSide(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Setup ONE_TO_MANY relationship
        $database->createCollection('teams_parent');
        $database->createCollection('members_parent');

        $database->createAttribute('teams_parent', 'teamName', Database::VAR_STRING, 255, true);
        $database->createAttribute('teams_parent', 'active', Database::VAR_BOOLEAN, 0, true);
        $database->createAttribute('members_parent', 'memberName', Database::VAR_STRING, 255, true);
        $database->createAttribute('members_parent', 'role', Database::VAR_STRING, 255, true);
        $database->createAttribute('members_parent', 'senior', Database::VAR_BOOLEAN, 0, true);

        $database->createRelationship(
            collection: 'teams_parent',
            relatedCollection: 'members_parent',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'members',
            twoWayKey: 'team'
        );

        // Create teams
        $database->createDocument('teams_parent', new Document([
            '$id' => 'team1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'teamName' => 'Engineering',
            'active' => true,
        ]));

        $database->createDocument('teams_parent', new Document([
            '$id' => 'team2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'teamName' => 'Sales',
            'active' => true,
        ]));

        // Create members
        $database->createDocument('members_parent', new Document([
            '$id' => 'member1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'memberName' => 'Alice',
            'role' => 'Engineer',
            'senior' => true,
            'team' => 'team1',
        ]));

        $database->createDocument('members_parent', new Document([
            '$id' => 'member2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'memberName' => 'Bob',
            'role' => 'Manager',
            'senior' => false,
            'team' => 'team2',
        ]));

        $database->createDocument('members_parent', new Document([
            '$id' => 'member3',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'memberName' => 'Charlie',
            'role' => 'Engineer',
            'senior' => true,
            'team' => 'team1',
        ]));

        // Test: Find teams that have senior engineers
        $teams = $database->find('teams_parent', [
            Query::equal('members.role', ['Engineer']),
            Query::equal('members.senior', [true])
        ]);
        $this->assertCount(1, $teams);
        $this->assertEquals('team1', $teams[0]->getId());

        // Test: Find teams with managers
        $teams = $database->find('teams_parent', [
            Query::equal('members.role', ['Manager'])
        ]);
        $this->assertCount(1, $teams);
        $this->assertEquals('team2', $teams[0]->getId());

        // Test: Find teams with members named 'Alice'
        $teams = $database->find('teams_parent', [
            Query::startsWith('members.memberName', 'A')
        ]);
        $this->assertCount(1, $teams);
        $this->assertEquals('team1', $teams[0]->getId());

        // Test: No teams with junior managers
        $teams = $database->find('teams_parent', [
            Query::equal('members.role', ['Manager']),
            Query::equal('members.senior', [true])
        ]);
        $this->assertCount(0, $teams);

        // Clean up
        $database->deleteCollection('teams_parent');
        $database->deleteCollection('members_parent');
    }

    /**
     * Test MANY_TO_MANY relationships with complex queries
     */
    public function testRelationshipManyToManyComplex(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Setup MANY_TO_MANY
        $database->createCollection('developers_mtm');
        $database->createCollection('projects_mtm');

        $database->createAttribute('developers_mtm', 'devName', Database::VAR_STRING, 255, true);
        $database->createAttribute('developers_mtm', 'experience', Database::VAR_INTEGER, 0, true);
        $database->createAttribute('projects_mtm', 'projectName', Database::VAR_STRING, 255, true);
        $database->createAttribute('projects_mtm', 'budget', Database::VAR_FLOAT, 0, true);
        $database->createAttribute('projects_mtm', 'priority', Database::VAR_STRING, 50, true);

        $database->createRelationship(
            collection: 'developers_mtm',
            relatedCollection: 'projects_mtm',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'assignedProjects',
            twoWayKey: 'assignedDevelopers'
        );

        // Create developers
        $dev1 = $database->createDocument('developers_mtm', new Document([
            '$id' => 'dev1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'devName' => 'Senior Dev',
            'experience' => 10,
        ]));

        $dev2 = $database->createDocument('developers_mtm', new Document([
            '$id' => 'dev2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'devName' => 'Junior Dev',
            'experience' => 2,
        ]));

        // Create projects
        $project1 = $database->createDocument('projects_mtm', new Document([
            '$id' => 'proj1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'projectName' => 'High Priority Project',
            'budget' => 100000.00,
            'priority' => 'high',
            'assignedDevelopers' => ['dev1', 'dev2'],
        ]));

        $project2 = $database->createDocument('projects_mtm', new Document([
            '$id' => 'proj2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'projectName' => 'Low Priority Project',
            'budget' => 25000.00,
            'priority' => 'low',
            'assignedDevelopers' => ['dev2'],
        ]));

        // Test: Find developers on high priority projects
        $developers = $database->find('developers_mtm', [
            Query::equal('assignedProjects.priority', ['high'])
        ]);
        $this->assertCount(2, $developers); // Both assigned to proj1

        // Test: Find developers on high budget projects
        $developers = $database->find('developers_mtm', [
            Query::greaterThan('assignedProjects.budget', 50000.00)
        ]);
        $this->assertCount(2, $developers);

        // Test: Find projects with experienced developers
        $projects = $database->find('projects_mtm', [
            Query::greaterThanEqual('assignedDevelopers.experience', 10)
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Test: Find projects with junior developers
        $projects = $database->find('projects_mtm', [
            Query::lessThan('assignedDevelopers.experience', 5)
        ]);
        $this->assertCount(2, $projects); // Both projects have dev2

        // Test: Combined queries
        $projects = $database->find('projects_mtm', [
            Query::equal('assignedDevelopers.devName', ['Junior Dev']),
            Query::equal('priority', ['low'])
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj2', $projects[0]->getId());

        // Clean up
        $database->deleteCollection('developers_mtm');
        $database->deleteCollection('projects_mtm');
    }

    public function testNestedRelationshipQueriesMultipleDepths(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Create 3-level nested structure:
        // Companies -> Employees -> Projects -> Tasks
        // Also: Employees -> Department (MANY_TO_ONE)

        // Level 0: Companies
        $database->createCollection('companies_nested');
        $database->createAttribute('companies_nested', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('companies_nested', 'industry', Database::VAR_STRING, 255, true);

        // Level 1: Employees
        $database->createCollection('employees_nested');
        $database->createAttribute('employees_nested', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('employees_nested', 'role', Database::VAR_STRING, 255, true);

        // Level 1b: Departments (for MANY_TO_ONE)
        $database->createCollection('departments_nested');
        $database->createAttribute('departments_nested', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('departments_nested', 'budget', Database::VAR_INTEGER, 0, true);

        // Level 2: Projects
        $database->createCollection('projects_nested');
        $database->createAttribute('projects_nested', 'title', Database::VAR_STRING, 255, true);
        $database->createAttribute('projects_nested', 'status', Database::VAR_STRING, 255, true);

        // Level 3: Tasks
        $database->createCollection('tasks_nested');
        $database->createAttribute('tasks_nested', 'description', Database::VAR_STRING, 255, true);
        $database->createAttribute('tasks_nested', 'priority', Database::VAR_STRING, 255, true);
        $database->createAttribute('tasks_nested', 'completed', Database::VAR_BOOLEAN, 0, true);

        // Create relationships
        // Companies -> Employees (ONE_TO_MANY)
        $database->createRelationship(
            collection: 'companies_nested',
            relatedCollection: 'employees_nested',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'employees',
            twoWayKey: 'company'
        );

        // Employees -> Department (MANY_TO_ONE)
        $database->createRelationship(
            collection: 'employees_nested',
            relatedCollection: 'departments_nested',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'department',
            twoWayKey: 'employees'
        );

        // Employees -> Projects (ONE_TO_MANY)
        $database->createRelationship(
            collection: 'employees_nested',
            relatedCollection: 'projects_nested',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'projects',
            twoWayKey: 'employee'
        );

        // Projects -> Tasks (ONE_TO_MANY)
        $database->createRelationship(
            collection: 'projects_nested',
            relatedCollection: 'tasks_nested',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'tasks',
            twoWayKey: 'project'
        );

        // Create test data
        $dept1 = $database->createDocument('departments_nested', new Document([
            '$id' => 'dept1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Engineering',
            'budget' => 100000,
        ]));

        $dept2 = $database->createDocument('departments_nested', new Document([
            '$id' => 'dept2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Marketing',
            'budget' => 50000,
        ]));

        $company1 = $database->createDocument('companies_nested', new Document([
            '$id' => 'company1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'TechCorp',
            'industry' => 'Technology',
        ]));

        $company2 = $database->createDocument('companies_nested', new Document([
            '$id' => 'company2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'MarketCo',
            'industry' => 'Marketing',
        ]));

        $employee1 = $database->createDocument('employees_nested', new Document([
            '$id' => 'emp1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Alice Johnson',
            'role' => 'Developer',
            'company' => 'company1',
            'department' => 'dept1',
        ]));

        $employee2 = $database->createDocument('employees_nested', new Document([
            '$id' => 'emp2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Bob Smith',
            'role' => 'Marketer',
            'company' => 'company2',
            'department' => 'dept2',
        ]));

        $project1 = $database->createDocument('projects_nested', new Document([
            '$id' => 'proj1',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Website Redesign',
            'status' => 'active',
            'employee' => 'emp1',
        ]));

        $project2 = $database->createDocument('projects_nested', new Document([
            '$id' => 'proj2',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Campaign Launch',
            'status' => 'planning',
            'employee' => 'emp2',
        ]));

        $task1 = $database->createDocument('tasks_nested', new Document([
            '$id' => 'task1',
            '$permissions' => [Permission::read(Role::any())],
            'description' => 'Design homepage',
            'priority' => 'high',
            'completed' => false,
            'project' => 'proj1',
        ]));

        $task2 = $database->createDocument('tasks_nested', new Document([
            '$id' => 'task2',
            '$permissions' => [Permission::read(Role::any())],
            'description' => 'Write copy',
            'priority' => 'medium',
            'completed' => true,
            'project' => 'proj2',
        ]));

        $task3 = $database->createDocument('tasks_nested', new Document([
            '$id' => 'task3',
            '$permissions' => [Permission::read(Role::any())],
            'description' => 'Implement backend',
            'priority' => 'high',
            'completed' => false,
            'project' => 'proj1',
        ]));

        // ==================== DEPTH 1 TESTS ====================
        // Test: Query employees by company name (1 level deep)
        $employees = $database->find('employees_nested', [
            Query::equal('company.name', ['TechCorp']),
        ]);
        $this->assertCount(1, $employees);
        $this->assertEquals('emp1', $employees[0]->getId());

        // Test: Query employees by department name (1 level deep MANY_TO_ONE)
        $employees = $database->find('employees_nested', [
            Query::equal('department.name', ['Engineering']),
        ]);
        $this->assertCount(1, $employees);
        $this->assertEquals('emp1', $employees[0]->getId());

        // Test: Query projects by employee name (1 level deep)
        $projects = $database->find('projects_nested', [
            Query::equal('employee.name', ['Alice Johnson']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // ==================== DEPTH 2 TESTS ====================
        // Test: Query projects by employee's company name (2 levels deep)
        $projects = $database->find('projects_nested', [
            Query::equal('employee.company.name', ['TechCorp']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Test: Query projects by employee's department name (2 levels deep, MANY_TO_ONE)
        $projects = $database->find('projects_nested', [
            Query::equal('employee.department.name', ['Engineering']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Test: Query tasks by project employee name (2 levels deep)
        $tasks = $database->find('tasks_nested', [
            Query::equal('project.employee.name', ['Alice Johnson']),
        ]);
        $this->assertCount(2, $tasks);

        // ==================== DEPTH 3 TESTS ====================
        // Test: Query tasks by project->employee->company name (3 levels deep)
        $tasks = $database->find('tasks_nested', [
            Query::equal('project.employee.company.name', ['TechCorp']),
        ]);
        $this->assertCount(2, $tasks);
        $this->assertEquals('task1', $tasks[0]->getId());
        $this->assertEquals('task3', $tasks[1]->getId());

        // Test: Query tasks by project->employee->department budget (3 levels deep with MANY_TO_ONE)
        $tasks = $database->find('tasks_nested', [
            Query::greaterThan('project.employee.department.budget', 75000),
        ]);
        $this->assertCount(2, $tasks); // Both tasks are in projects by employees in Engineering dept

        // Test: Query tasks by project->employee->company industry (3 levels deep)
        $tasks = $database->find('tasks_nested', [
            Query::equal('project.employee.company.industry', ['Marketing']),
        ]);
        $this->assertCount(1, $tasks);
        $this->assertEquals('task2', $tasks[0]->getId());

        // ==================== COMBINED DEPTH TESTS ====================
        // Test: Combine depth 1 and depth 3 queries
        $tasks = $database->find('tasks_nested', [
            Query::equal('priority', ['high']),
            Query::equal('project.employee.company.name', ['TechCorp']),
        ]);
        $this->assertCount(2, $tasks);

        // Test: Multiple depth 2 queries combined
        $projects = $database->find('projects_nested', [
            Query::equal('employee.company.industry', ['Technology']),
            Query::equal('employee.department.name', ['Engineering']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Clean up
        $database->deleteCollection('tasks_nested');
        $database->deleteCollection('projects_nested');
        $database->deleteCollection('employees_nested');
        $database->deleteCollection('departments_nested');
        $database->deleteCollection('companies_nested');
    }
}

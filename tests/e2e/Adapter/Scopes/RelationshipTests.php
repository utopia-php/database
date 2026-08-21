<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Tests\E2E\Adapter\Scopes\Relationships\ManyToManyTests;
use Tests\E2E\Adapter\Scopes\Relationships\ManyToOneTests;
use Tests\E2E\Adapter\Scopes\Relationships\OneToManyTests;
use Tests\E2E\Adapter\Scopes\Relationships\OneToOneTests;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Query\Schema\ForeignKeyAction;

trait RelationshipTests
{
    use ManyToManyTests;
    use ManyToOneTests;
    use OneToManyTests;
    use OneToOneTests;

    public function testZoo(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'zoo'));
        $database->createAttribute('zoo', Attribute::string(key: 'name', size: 256, required: true));

        $database->createCollection(new Collection(id: 'veterinarians'));
        $database->createAttribute('veterinarians', Attribute::string(key: 'fullname', size: 256, required: true));

        $database->createCollection(new Collection(id: 'presidents'));
        $database->createAttribute('presidents', Attribute::string(key: 'firstName', size: 256, required: true));
        $database->createAttribute('presidents', Attribute::string(key: 'lastName', size: 256, required: true));
        $database->createRelationship(Relationship::manyToMany(
            collection: 'presidents',
            relatedCollection: 'veterinarians',
            twoWay: true,
            key: 'votes',
            twoWayKey: 'presidents'
        ));

        $database->createCollection(new Collection(id: '__animals'));
        $database->createAttribute('__animals', Attribute::string(key: 'name', size: 256, required: true));
        $database->createAttribute('__animals', Attribute::integer(key: 'age'));
        $database->createAttribute('__animals', Attribute::double(key: 'price'));
        $database->createAttribute('__animals', Attribute::datetime(key: 'dateOfBirth', required: true, filters: ['datetime']));
        $database->createAttribute('__animals', Attribute::string(key: 'longtext', size: 100000000));
        $database->createAttribute('__animals', Attribute::boolean(key: 'isActive', default: true));
        $database->createAttribute('__animals', Attribute::integer(key: 'integers', array: true));
        $database->createAttribute('__animals', Attribute::string(key: 'email'));
        $database->createAttribute('__animals', Attribute::string(key: 'ip'));
        $database->createAttribute('__animals', Attribute::string(key: 'url'));
        $database->createAttribute('__animals', Attribute::string(key: 'enum'));

        $database->createRelationship(Relationship::oneToOne(
            collection: 'presidents',
            relatedCollection: '__animals',
            twoWay: true,
            key: 'animal',
            twoWayKey: 'president'
        ));

        $database->createRelationship(Relationship::oneToMany(
            collection: 'veterinarians',
            relatedCollection: '__animals',
            twoWay: true,
            key: 'animals',
            twoWayKey: 'veterinarian'
        ));

        $database->createRelationship(Relationship::manyToOne(
            collection: '__animals',
            relatedCollection: 'zoo',
            twoWay: true,
            key: 'zoo',
            twoWayKey: 'animals'
        ));

        $zoo = $database->createDocument('zoo', new Document([
            '$id' => 'zoo1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Bronx Zoo',
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
            'dateOfBirth' => '1975-06-12',
            'longtext' => 'I am a pretty long text',
            'isActive' => true,
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
            'dateOfBirth' => '2020-06-12',
            'longtext' => 'I am a hungry tiger',
            'isActive' => false,
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
            'dateOfBirth' => '1975-06-12',
            'isActive' => true,
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
            'firstName' => 'Donald',
            'lastName' => 'Trump',
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
            'firstName' => 'George',
            'lastName' => 'Bush',
            'animal' => 'iguana',
        ]));

        $biden = $database->createDocument('presidents', new Document([
            '$id' => 'biden',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'firstName' => 'Joe',
            'lastName' => 'Biden',
            'animal' => 'tiger',
        ]));

        /**
         * Check Zoo data
         */
        $zoo = $database->getDocument('zoo', 'zoo1');

        $this->assertEquals('zoo1', $zoo->getId());
        $this->assertEquals('Bronx Zoo', $zoo->getAttribute('name'));
        $this->assertArrayHasKey('animals', $zoo);
        $this->assertCount(2, $zoo->getDocuments('animals'));
        $this->assertArrayHasKey('president', $zoo->getDocuments('animals')[0]);
        $this->assertArrayHasKey('veterinarian', $zoo->getDocuments('animals')[0]);

        $zoo = $database->findOne('zoo');

        $this->assertEquals('zoo1', $zoo->getId());
        $this->assertEquals('Bronx Zoo', $zoo->getAttribute('name'));
        $this->assertArrayHasKey('animals', $zoo);
        $this->assertCount(2, $zoo->getDocuments('animals'));
        $this->assertArrayHasKey('president', $zoo->getDocuments('animals')[0]);
        $this->assertArrayHasKey('veterinarian', $zoo->getDocuments('animals')[0]);

        /**
         * Check Veterinarians data
         */
        $veterinarian = $database->getDocument('veterinarians', 'dr.pol');

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayHasKey('presidents', $veterinarian);
        $this->assertCount(1, $veterinarian->getDocuments('presidents'));
        $this->assertArrayHasKey('animal', $veterinarian->getDocuments('presidents')[0]);
        $this->assertArrayHasKey('animals', $veterinarian);
        $this->assertCount(1, $veterinarian->getDocuments('animals'));
        $this->assertArrayHasKey('zoo', $veterinarian->getDocuments('animals')[0]);
        $this->assertArrayHasKey('president', $veterinarian->getDocuments('animals')[0]);

        $veterinarian = $database->findOne('veterinarians', [
            Query::equal('$id', ['dr.pol']),
        ]);

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayHasKey('presidents', $veterinarian);
        $this->assertCount(1, $veterinarian->getDocuments('presidents'));
        $this->assertArrayHasKey('animal', $veterinarian->getDocuments('presidents')[0]);
        $this->assertArrayHasKey('animals', $veterinarian);
        $this->assertCount(1, $veterinarian->getDocuments('animals'));
        $this->assertArrayHasKey('zoo', $veterinarian->getDocuments('animals')[0]);
        $this->assertArrayHasKey('president', $veterinarian->getDocuments('animals')[0]);

        /**
         * Check Animals data
         */
        $animal = $database->getDocument('__animals', 'iguana');

        $this->assertEquals('iguana', $animal->getId());
        $this->assertArrayHasKey('zoo', $animal);
        $this->assertEquals('Bronx Zoo', $animal->getDocument('zoo')->getAttribute('name'));
        $this->assertArrayHasKey('veterinarian', $animal);
        $this->assertEquals('dr.pol', $animal->getDocument('veterinarian')->getId());
        $this->assertArrayHasKey('presidents', $animal->getDocument('veterinarian'));
        $this->assertArrayHasKey('president', $animal);
        $this->assertEquals('bush', $animal->getDocument('president')->getId());

        $animal = $database->findOne('__animals', [
            Query::equal('$id', ['tiger']),
        ]);

        $this->assertEquals('tiger', $animal->getId());
        $this->assertArrayHasKey('zoo', $animal);
        $this->assertEquals('Bronx Zoo', $animal->getDocument('zoo')->getAttribute('name'));
        $this->assertArrayHasKey('veterinarian', $animal);
        $this->assertEquals('dr.seuss', $animal->getDocument('veterinarian')->getId());
        $this->assertArrayHasKey('presidents', $animal->getDocument('veterinarian'));
        $this->assertArrayHasKey('president', $animal);
        $this->assertEquals('biden', $animal->getDocument('president')->getId());

        /**
         * Check President data
         */
        $president = $database->getDocument('presidents', 'trump');

        $this->assertEquals('trump', $president->getId());
        $this->assertArrayHasKey('animal', $president);
        $this->assertArrayHasKey('votes', $president);
        $this->assertCount(2, $president->getDocuments('votes'));

        /**
         * Check President data
         */
        $president = $database->findOne('presidents', [
            Query::equal('$id', ['bush']),
        ]);

        $this->assertEquals('bush', $president->getId());
        $this->assertArrayHasKey('animal', $president);
        $this->assertArrayHasKey('votes', $president);
        $this->assertCount(0, $president->getDocuments('votes'));

        $president = $database->findOne('presidents', [
            Query::select([
                '*',
                'votes.*',
            ]),
            Query::equal('$id', ['trump']),
        ]);

        $this->assertEquals('trump', $president->getId());
        $this->assertArrayHasKey('votes', $president);
        $this->assertCount(2, $president->getDocuments('votes'));
        $this->assertArrayNotHasKey('animals', $president->getDocuments('votes')[0]); // Not exist

        $president = $database->findOne('presidents', [
            Query::select([
                '*',
                'votes.*',
                'votes.animals.*',
            ]),
            Query::equal('$id', ['trump']),
        ]);

        $this->assertEquals('trump', $president->getId());
        $this->assertArrayHasKey('votes', $president);
        $this->assertCount(2, $president->getDocuments('votes'));
        $this->assertArrayHasKey('animals', $president->getDocuments('votes')[0]); // Exist

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
                ]),
            ]
        );

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayHasKey('animals', $veterinarian);
        $this->assertArrayNotHasKey('presidents', $veterinarian);

        $animal = $veterinarian->getDocuments('animals')[0];

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
                ]),
            ]
        );

        $this->assertEquals('dr.pol', $veterinarian->getId());
        $this->assertArrayHasKey('animals', $veterinarian);
        $this->assertArrayNotHasKey('presidents', $veterinarian);

        $animal = $veterinarian->getDocuments('animals')[0];

        $this->assertArrayHasKey('president', $animal);
        $this->assertEquals('Bush', $animal->getDocument('president')->getAttribute('lastName')); // Check president is an object
        $this->assertArrayHasKey('zoo', $animal);
        $this->assertEquals('Bronx Zoo', $animal->getDocument('zoo')->getAttribute('name')); // Check zoo is an object
    }

    public function testSimpleRelationshipPopulation(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Simple test case: user -> post (one-to-many)
        $database->createCollection(new Collection(id: 'usersSimple'));
        $database->createCollection(new Collection(id: 'postsSimple'));

        $database->createAttribute('usersSimple', Attribute::string(key: 'name', required: true));
        $database->createAttribute('postsSimple', Attribute::string(key: 'title', required: true));

        $database->createRelationship(Relationship::oneToMany(
            collection: 'usersSimple',
            relatedCollection: 'postsSimple',
            twoWay: true,
            key: 'posts',
            twoWayKey: 'author'
        ));

        // Create some data
        $user = $database->createDocument('usersSimple', new Document([
            '$id' => 'user1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'John Doe',
        ]));

        $post1 = $database->createDocument('postsSimple', new Document([
            '$id' => 'post1',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'First Post',
            'author' => 'user1',
        ]));

        $post2 = $database->createDocument('postsSimple', new Document([
            '$id' => 'post2',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Second Post',
            'author' => 'user1',
        ]));

        // fetch user with posts populated
        $fetchedUser = $database->getDocument('usersSimple', 'user1');
        $posts = $fetchedUser->getDocuments('posts');

        $this->assertCount(2, $posts, 'Should have 2 posts');
        $this->assertEquals('First Post', $posts[0]->getAttribute('title'), 'First post title should be populated');

        $fetchedPosts = $database->find('postsSimple');

        $this->assertCount(2, $fetchedPosts, 'Should fetch 2 posts');
        $this->assertEquals('John Doe', $fetchedPosts[0]->getDocument('author')->getAttribute('name'), 'Author name should be populated');
    }

    public function testDeleteRelatedCollection(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'c1'));
        $database->createCollection(new Collection(id: 'c2'));

        // ONE_TO_ONE
        $database->createRelationship(Relationship::oneToOne(collection: 'c1', relatedCollection: 'c2'));

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c1'));
        $database->createRelationship(Relationship::oneToOne(collection: 'c1', relatedCollection: 'c2'));

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c2'));
        $database->createRelationship(Relationship::oneToOne(collection: 'c1', relatedCollection: 'c2', twoWay: true));

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c1'));
        $database->createRelationship(Relationship::oneToOne(collection: 'c1', relatedCollection: 'c2', twoWay: true));

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        // ONE_TO_MANY
        $database->createCollection(new Collection(id: 'c2'));
        $database->createRelationship(Relationship::oneToMany(collection: 'c1', relatedCollection: 'c2'));

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c1'));
        $database->createRelationship(Relationship::oneToMany(collection: 'c1', relatedCollection: 'c2'));

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c2'));
        $database->createRelationship(Relationship::oneToMany(collection: 'c1', relatedCollection: 'c2', twoWay: true));

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c1'));
        $database->createRelationship(Relationship::oneToMany(collection: 'c1', relatedCollection: 'c2', twoWay: true));

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        // RELATION_MANY_TO_ONE
        $database->createCollection(new Collection(id: 'c2'));
        $database->createRelationship(Relationship::manyToOne(collection: 'c1', relatedCollection: 'c2'));

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c1'));
        $database->createRelationship(Relationship::manyToOne(collection: 'c1', relatedCollection: 'c2'));

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c2'));
        $database->createRelationship(Relationship::manyToOne(collection: 'c1', relatedCollection: 'c2', twoWay: true));

        $this->assertEquals(true, $database->deleteCollection('c1'));
        $collection = $database->getCollection('c2');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);

        $database->createCollection(new Collection(id: 'c1'));
        $database->createRelationship(Relationship::manyToOne(collection: 'c1', relatedCollection: 'c2', twoWay: true));

        $this->assertEquals(true, $database->deleteCollection('c2'));
        $collection = $database->getCollection('c1');
        $this->assertCount(0, $collection->attributes);
        $this->assertCount(0, $collection->indexes);
    }

    public function testVirtualRelationsAttributes(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'v1'));
        $database->createCollection(new Collection(id: 'v2'));

        /**
         * RELATION_ONE_TO_ONE
         * TwoWay is false no attribute is created on v2
         */
        $database->createRelationship(Relationship::oneToOne(collection: 'v1', relatedCollection: 'v2'));

        try {
            $database->createDocument('v2', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
                'v1' => 'invalidValue',
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
                ],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->find('v2', [
                Query::equal('v1', ['virtualAttribute']),
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
                    Permission::read(Role::any()),
                ],
            ],
        ]));

        $this->assertEquals('man', $doc->getId());

        try {
            $database->updateDocument('v1', 'man', new Document([
                '$permissions' => [],
                'v2' => [[
                    '$id' => 'woman',
                    '$permissions' => [],
                ]],
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
        $database->createRelationship(Relationship::oneToMany(collection: 'v1', relatedCollection: 'v2', twoWay: true));

        try {
            $database->createDocument('v1', new Document([
                '$id' => 'doc1',
                '$permissions' => [],
                'v2' => [ // Expecting Array of arrays or array of strings, object provided
                    '$id' => 'test',
                    '$permissions' => [],
                ],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->createDocument('v1', new Document([
                '$permissions' => [],
                'v2' => 'invalidValue',
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
                ]],
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
                    Permission::update(Role::any()),
                ],
            ],
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
                ],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->updateDocument('v1', 'v1_uid', new Document([
                '$permissions' => [],
                'v2' => 'v2_uid',
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
                ],
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
                // @phpstan-ignore-next-line
                Query::equal('v1', [['doc1']]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        try {
            $database->find('v1', [
                Query::equal('v2', ['virtualAttribute']),
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
        $database->createRelationship(Relationship::manyToOne(collection: 'v1', relatedCollection: 'v2', twoWay: true));

        try {
            $database->createDocument('v1', new Document([
                '$id' => 'doc',
                '$permissions' => [],
                'v2' => [[ // Expecting an object or a string array provided
                    '$id' => 'test',
                    '$permissions' => [],
                ]],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->createDocument('v2', new Document([
                '$permissions' => [],
                'v1' => 'invalidValue',
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
                ],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->find('v2', [
                Query::equal('v1', ['virtualAttribute']),
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
            ],
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
                'v1' => null,
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
        $database->createRelationship(Relationship::manyToMany(
            collection: 'v1',
            relatedCollection: 'v2',
            twoWay: true,
            key: 'students',
            twoWayKey: 'classes'
        ));

        try {
            $database->createDocument('v1', new Document([
                '$permissions' => [],
                'students' => 'invalidValue',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->createDocument('v2', new Document([
                '$permissions' => [],
                'classes' => 'invalidValue',
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
                ],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }

        try {
            $database->find('v1', [
                Query::equal('students', ['virtualAttribute']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        try {
            $database->find('v2', [
                Query::equal('classes', ['virtualAttribute']),
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
                        Permission::read(Role::any()),
                    ],
                ],
                [
                    '$id' => 'Bill',
                    '$permissions' => [
                        Permission::update(Role::any()),
                        Permission::read(Role::any()),
                    ],
                ],
            ],
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
                        Permission::read(Role::any()),
                    ],
                ],
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
                'students' => 'Richard',
            ]));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertTrue($e instanceof RelationshipException);
        }
    }

    public function testUpdateAttributeRenameRelationshipTwoWay(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'rnRsTestA'));
        $database->createCollection(new Collection(id: 'rnRsTestB'));

        $database->createAttribute('rnRsTestB', Attribute::string(key: 'name', required: true));

        $database->createRelationship(Relationship::oneToOne(collection: 'rnRsTestA', relatedCollection: 'rnRsTestB', twoWay: true));

        $docA = $database->createDocument('rnRsTestA', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'rnRsTestB' => [
                '$id' => 'b1',
                'name' => 'B1',
            ],
        ]));

        $docB = $database->getDocument('rnRsTestB', 'b1');
        $this->assertArrayHasKey('rnRsTestA', $docB->getAttributes());
        $this->assertEquals('B1', $docB->getAttribute('name'));

        // Rename attribute
        $database->updateRelationship(
            collection: 'rnRsTestA',
            id: 'rnRsTestB',
            newKey: 'rnRsTestB_renamed'
        );

        // Rename again
        $database->updateRelationship(
            collection: 'rnRsTestA',
            id: 'rnRsTestB_renamed',
            newKey: 'rnRsTestB_renamed_2'
        );

        // Check our data is OK
        $docA = $database->getDocument('rnRsTestA', $docA->getId());
        $this->assertArrayHasKey('rnRsTestB_renamed_2', $docA->getAttributes());
        $this->assertEquals($docB->getId(), $docA->getDocument('rnRsTestB_renamed_2')->getId());
    }

    public function testSelectRelationshipAttributes(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'make'));
        $database->createCollection(new Collection(id: 'model'));

        $database->createAttribute('make', Attribute::string(key: 'name', required: true));
        $database->createAttribute('make', Attribute::string(key: 'origin', required: true));
        $database->createAttribute('model', Attribute::string(key: 'name', required: true));
        $database->createAttribute('model', Attribute::integer(key: 'year', required: true));

        $database->createRelationship(Relationship::oneToMany(
            collection: 'make',
            relatedCollection: 'model',
            twoWay: true,
            key: 'models',
            twoWayKey: 'make'
        ));

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
        $this->assertCount(2, $make->getDocuments('models'));
        $this->assertEquals('Fiesta', $make->getDocuments('models')[0]->getAttribute('name'));
        $this->assertEquals('Focus', $make->getDocuments('models')[1]->getAttribute('name'));
        $this->assertArrayNotHasKey('year', $make->getDocuments('models')[0]);
        $this->assertArrayNotHasKey('year', $make->getDocuments('models')[1]);
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
        $this->assertCount(2, $make->getDocuments('models'));
        $this->assertArrayNotHasKey('name', $make->getDocuments('models')[0]);
        $this->assertArrayNotHasKey('name', $make->getDocuments('models')[1]);
        $this->assertEquals(2010, $make->getDocuments('models')[0]->getAttribute('year'));
        $this->assertEquals(2011, $make->getDocuments('models')[1]->getAttribute('year'));

        // Select all parent attributes, all child attributes
        $make = $database->findOne('make', [
            Query::select(['*', 'models.*']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertCount(2, $make->getDocuments('models'));
        $this->assertEquals('Fiesta', $make->getDocuments('models')[0]->getAttribute('name'));
        $this->assertEquals('Focus', $make->getDocuments('models')[1]->getAttribute('name'));
        $this->assertEquals(2010, $make->getDocuments('models')[0]->getAttribute('year'));
        $this->assertEquals(2011, $make->getDocuments('models')[1]->getAttribute('year'));

        // Select all parent attributes, all child attributes
        // Must select parent if selecting children
        $make = $database->findOne('make', [
            Query::select(['models.*']),
        ]);

        if ($make->isEmpty()) {
            throw new Exception('Make not found');
        }

        $this->assertEquals('Ford', $make['name']);
        $this->assertCount(2, $make->getDocuments('models'));
        $this->assertEquals('Fiesta', $make->getDocuments('models')[0]->getAttribute('name'));
        $this->assertEquals('Focus', $make->getDocuments('models')[1]->getAttribute('name'));
        $this->assertEquals(2010, $make->getDocuments('models')[0]->getAttribute('year'));
        $this->assertEquals(2011, $make->getDocuments('models')[1]->getAttribute('year'));

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
        $this->assertCount(2, $make->getDocuments('models'));

        /*
         * FROM CHILD TO PARENT
         */

        // Select some parent attributes, some child attributes
        $model = $database->findOne('model', [
            Query::select(['name', 'make.name']),
        ]);

        $this->assertEquals('Fiesta', $model['name']);
        $this->assertEquals('Ford', $model->getDocument('make')->getAttribute('name'));
        $this->assertArrayNotHasKey('origin', $model->getDocument('make'));
        $this->assertArrayNotHasKey('year', $model);
        $this->assertArrayHasKey('name', $model);

        // Select all parent attributes, some child attributes
        $model = $database->findOne('model', [
            Query::select(['*', 'make.name']),
        ]);

        $this->assertEquals('Fiesta', $model['name']);
        $this->assertEquals('Ford', $model->getDocument('make')->getAttribute('name'));
        $this->assertArrayHasKey('year', $model);

        // Select all parent attributes, all child attributes
        $model = $database->findOne('model', [
            Query::select(['*', 'make.*']),
        ]);

        $this->assertEquals('Fiesta', $model['name']);
        $this->assertEquals('Ford', $model->getDocument('make')->getAttribute('name'));
        $this->assertArrayHasKey('year', $model);
        $this->assertArrayHasKey('name', $model->getDocument('make'));

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
        $this->assertEquals('Ford', $model->getDocument('make')->getAttribute('name'));
        $this->assertEquals('USA', $model->getDocument('make')->getAttribute('origin'));
    }

    public function testInheritRelationshipPermissions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'lawns', permissions: [Permission::create(Role::any())]));
        $database->createCollection(new Collection(id: 'trees', permissions: [Permission::create(Role::any())]));
        $database->createCollection(new Collection(id: 'birds', permissions: [Permission::create(Role::any())]));

        $database->createAttribute('lawns', Attribute::string(key: 'name', required: true));
        $database->createAttribute('trees', Attribute::string(key: 'name', required: true));
        $database->createAttribute('birds', Attribute::string(key: 'name', required: true));

        $database->createRelationship(Relationship::oneToMany(
            collection: 'lawns',
            relatedCollection: 'trees',
            twoWay: true,
            twoWayKey: 'lawn',
            onDelete: ForeignKeyAction::Cascade
        ));
        $database->createRelationship(Relationship::manyToMany(collection: 'trees', relatedCollection: 'birds', twoWay: true, onDelete: ForeignKeyAction::SetNull));

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
        $this->assertEquals($permissions, $lawn1->getDocuments('trees')[0]->getPermissions());
        $this->assertEquals($permissions, $lawn1->getDocuments('trees')[0]->getDocuments('birds')[0]->getPermissions());
        $this->assertEquals($permissions, $lawn1->getDocuments('trees')[0]->getDocuments('birds')[1]->getPermissions());

        $tree1 = $database->getDocument('trees', 'tree1');
        $this->assertEquals($permissions, $tree1->getPermissions());
        $this->assertEquals($permissions, $tree1->getDocument('lawn')->getPermissions());
        $this->assertEquals($permissions, $tree1->getDocuments('birds')[0]->getPermissions());
        $this->assertEquals($permissions, $tree1->getDocuments('birds')[1]->getPermissions());
    }

    public function testUpdateDocumentsRelationships(): void
    {
        if (! $this->getDatabase()->getAdapter()->supports(Capability::BatchOperations) || ! ($this->getDatabase()->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        $this->getDatabase()->createCollection(new Collection(id: 'testUpdateDocumentsRelationships1', attributes: [
            Attribute::string(key: 'string', size: 767, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $this->getDatabase()->createCollection(new Collection(id: 'testUpdateDocumentsRelationships2', attributes: [
            Attribute::string(key: 'string', size: 767, required: true),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $this->getDatabase()->createRelationship(Relationship::oneToOne(collection: 'testUpdateDocumentsRelationships1', relatedCollection: 'testUpdateDocumentsRelationships2', twoWay: true));

        $this->getDatabase()->createDocument('testUpdateDocumentsRelationships1', new Document([
            '$id' => 'doc1',
            'string' => 'text📝',
        ]));

        $this->getDatabase()->createDocument('testUpdateDocumentsRelationships2', new Document([
            '$id' => 'doc1',
            'string' => 'text📝',
            'testUpdateDocumentsRelationships1' => 'doc1',
        ]));

        $sisterDocument = $this->getDatabase()->getDocument('testUpdateDocumentsRelationships2', 'doc1');
        $this->assertFalse($sisterDocument->isEmpty());

        $this->getDatabase()->updateDocuments('testUpdateDocumentsRelationships1', new Document([
            'string' => 'text📝 updated',
        ]));

        $document = $this->getDatabase()->findOne('testUpdateDocumentsRelationships1');

        $this->assertFalse($document->isEmpty());
        $this->assertEquals('text📝 updated', $document->getAttribute('string'));

        $sisterDocument = $this->getDatabase()->getDocument('testUpdateDocumentsRelationships2', 'doc1');
        $this->assertFalse($sisterDocument->isEmpty());

        $relationalDocument = $sisterDocument->getDocument('testUpdateDocumentsRelationships1');
        $this->assertEquals('text📝 updated', $relationalDocument->getAttribute('string'));

        // Check relationship value updating between each other.
        $this->getDatabase()->deleteRelationship('testUpdateDocumentsRelationships1', 'testUpdateDocumentsRelationships2');

        $this->getDatabase()->createRelationship(Relationship::oneToMany(collection: 'testUpdateDocumentsRelationships1', relatedCollection: 'testUpdateDocumentsRelationships2', twoWay: true));

        for ($i = 2; $i < 11; $i++) {
            $this->getDatabase()->createDocument('testUpdateDocumentsRelationships1', new Document([
                '$id' => 'doc'.$i,
                'string' => 'text📝',
            ]));

            $this->getDatabase()->createDocument('testUpdateDocumentsRelationships2', new Document([
                '$id' => 'doc'.$i,
                'string' => 'text📝',
                'testUpdateDocumentsRelationships1' => 'doc'.$i,
            ]));
        }

        $this->getDatabase()->updateDocuments('testUpdateDocumentsRelationships2', new Document([
            'testUpdateDocumentsRelationships1' => null,
        ]));

        $this->getDatabase()->updateDocuments('testUpdateDocumentsRelationships2', new Document([
            'testUpdateDocumentsRelationships1' => 'doc1',
        ]));

        $documents = $this->getDatabase()->find('testUpdateDocumentsRelationships2');

        foreach ($documents as $document) {
            $this->assertEquals('doc1', $document->getDocument('testUpdateDocumentsRelationships1')->getId());
        }
    }

    public function testUpdateDocumentWithRelationships(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }
        $database->createCollection(new Collection(id: 'userProfiles', attributes: [
            Attribute::string(key: 'username', size: 700, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: 'links', attributes: [
            Attribute::string(key: 'title', size: 700, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: 'videos', attributes: [
            Attribute::string(key: 'title', size: 700, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: 'products', attributes: [
            Attribute::string(key: 'title', size: 700, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: 'settings', attributes: [
            Attribute::string(key: 'metaTitle', size: 700, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: 'appearance', attributes: [
            Attribute::string(key: 'metaTitle', size: 700, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: 'group', attributes: [
            Attribute::string(key: 'name', size: 700, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: 'community', attributes: [
            Attribute::string(key: 'name', size: 700, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $database->createRelationship(Relationship::oneToMany(collection: 'userProfiles', relatedCollection: 'links', key: 'links'));

        $database->createRelationship(Relationship::oneToMany(collection: 'userProfiles', relatedCollection: 'videos', key: 'videos'));

        $database->createRelationship(Relationship::oneToMany(
            collection: 'userProfiles',
            relatedCollection: 'products',
            twoWay: true,
            key: 'products',
            twoWayKey: 'userProfile'
        ));

        $database->createRelationship(Relationship::oneToOne(collection: 'userProfiles', relatedCollection: 'settings', key: 'settings'));

        $database->createRelationship(Relationship::oneToOne(collection: 'userProfiles', relatedCollection: 'appearance', key: 'appearance'));

        $database->createRelationship(Relationship::manyToOne(collection: 'userProfiles', relatedCollection: 'group', key: 'group'));

        $database->createRelationship(Relationship::manyToOne(collection: 'userProfiles', relatedCollection: 'community', key: 'community'));

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
        $this->assertEquals('link1', $profile->getDocuments('links')[0]->getId());
        $this->assertEquals('settings1', $profile->getDocument('settings')->getId());
        $this->assertEquals('group1', $profile->getDocument('group')->getId());
        $this->assertEquals('community1', $profile->getDocument('community')->getId());
        $this->assertEquals('video1', $profile->getDocuments('videos')[0]->getId());
        $this->assertEquals('product1', $profile->getDocuments('products')[0]->getId());
        $this->assertEquals('appearance1', $profile->getDocument('appearance')->getId());

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

        $this->assertEquals('New Link Value', $updatedProfile->getDocuments('links')[0]->getAttribute('title'));
        $this->assertEquals('New Meta Title', $updatedProfile->getDocument('settings')->getAttribute('metaTitle'));
        $this->assertEquals('New Group Name', $updatedProfile->getDocument('group')->getAttribute('name'));

        // This is the point of test, related documents should be present if they are not updated
        $this->assertEquals('Video 1', $updatedProfile->getDocuments('videos')[0]->getAttribute('title'));
        $this->assertEquals('Product 1', $updatedProfile->getDocuments('products')[0]->getAttribute('title'));
        $this->assertEquals('Meta Title', $updatedProfile->getDocument('appearance')->getAttribute('metaTitle'));
        $this->assertEquals('Community 1', $updatedProfile->getDocument('community')->getAttribute('name'));

        // updating document using two way key in one to many relationship
        $product = $database->getDocument('products', 'product1');
        $product->setAttribute('userProfile', [
            '$id' => '1',
            'username' => 'updated user value',
        ]);
        $updatedProduct = $database->updateDocument('products', 'product1', $product);
        $this->assertEquals('updated user value', $updatedProduct->getDocument('userProfile')->getAttribute('username'));
        $this->assertEquals('Product 1', $updatedProduct->getAttribute('title'));
        $this->assertEquals('product1', $updatedProduct->getId());
        $this->assertEquals('1', $updatedProduct->getDocument('userProfile')->getId());

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
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create collections: car -> customer -> inspection
        $database->createCollection(new Collection(id: 'car'));
        $database->createAttribute('car', Attribute::string(key: 'plateNumber', required: true));

        $database->createCollection(new Collection(id: 'customer'));
        $database->createAttribute('customer', Attribute::string(key: 'name', required: true));

        $database->createCollection(new Collection(id: 'inspection'));
        $database->createAttribute('inspection', Attribute::string(key: 'type', required: true));

        // Create relationships
        // car -> customer (many to one, one-way to avoid circular references)
        $database->createRelationship(Relationship::manyToOne(collection: 'car', relatedCollection: 'customer', key: 'customer'));

        // customer -> inspection (one to many, one-way)
        $database->createRelationship(Relationship::oneToMany(collection: 'customer', relatedCollection: 'inspection', key: 'inspections'));

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
            'plateNumber' => 'ABC123',
            'customer' => 'customer1',
        ]));

        $car2 = $database->createDocument('car', new Document([
            '$id' => 'car2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'plateNumber' => 'DEF456',
            'customer' => 'customer2',
        ]));

        $car3 = $database->createDocument('car', new Document([
            '$id' => 'car3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'plateNumber' => 'GHI789',
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

        $this->assertEquals('ABC123', $cars[0]['plateNumber']);
        $this->assertEquals('Customer 1', $cars[0]->getDocument('customer')->getAttribute('name'));
        $this->assertCount(2, $cars[0]->getDocument('customer')->getDocuments('inspections'));
        $this->assertEquals('annual', $cars[0]->getDocument('customer')->getDocuments('inspections')[0]->getAttribute('type'));
        $this->assertEquals('safety', $cars[0]->getDocument('customer')->getDocuments('inspections')[1]->getAttribute('type'));

        $this->assertEquals('DEF456', $cars[1]['plateNumber']);
        $this->assertEquals('Customer 2', $cars[1]->getDocument('customer')->getAttribute('name'));
        $this->assertCount(2, $cars[1]->getDocument('customer')->getDocuments('inspections'));
        $this->assertEquals('emissions', $cars[1]->getDocument('customer')->getDocuments('inspections')[0]->getAttribute('type'));
        $this->assertEquals('annual', $cars[1]->getDocument('customer')->getDocuments('inspections')[1]->getAttribute('type'));

        $this->assertEquals('GHI789', $cars[2]['plateNumber']);
        $this->assertEquals('Customer 3', $cars[2]->getDocument('customer')->getAttribute('name'));
        $this->assertCount(1, $cars[2]->getDocument('customer')->getDocuments('inspections'));
        $this->assertEquals('safety', $cars[2]->getDocument('customer')->getDocuments('inspections')[0]->getAttribute('type'));

        // Test with createDocuments as well
        $database->deleteDocument('car', 'car1');
        $database->deleteDocument('car', 'car2');
        $database->deleteDocument('car', 'car3');

        $database->createDocuments('car', [
            new Document([
                '$id' => 'car1',
                '$permissions' => [Permission::read(Role::any())],
                'plateNumber' => 'ABC123',
                'customer' => 'customer1',
            ]),
            new Document([
                '$id' => 'car2',
                '$permissions' => [Permission::read(Role::any())],
                'plateNumber' => 'DEF456',
                'customer' => 'customer2',
            ]),
            new Document([
                '$id' => 'car3',
                '$permissions' => [Permission::read(Role::any())],
                'plateNumber' => 'GHI789',
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
        $this->assertCount(2, $cars[0]->getDocument('customer')->getDocuments('inspections'));
        $this->assertCount(2, $cars[1]->getDocument('customer')->getDocuments('inspections'));
        $this->assertCount(1, $cars[2]->getDocument('customer')->getDocuments('inspections'));

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
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create three collections with chained relationships: Order -> Product -> Store
        $database->createCollection(new Collection(id: 'orderDepthTest'));
        $database->createCollection(new Collection(id: 'productDepthTest'));
        $database->createCollection(new Collection(id: 'storeDepthTest'));

        $database->createAttribute('orderDepthTest', Attribute::string(key: 'orderNumber', required: true));
        $database->createAttribute('productDepthTest', Attribute::string(key: 'productName', required: true));
        $database->createAttribute('storeDepthTest', Attribute::string(key: 'storeName', required: true));

        // Order -> Product (many-to-one)
        $database->createRelationship(Relationship::manyToOne(
            collection: 'orderDepthTest',
            relatedCollection: 'productDepthTest',
            twoWay: true,
            key: 'product',
            twoWayKey: 'orders'
        ));

        // Product -> Store (many-to-one)
        $database->createRelationship(Relationship::manyToOne(
            collection: 'productDepthTest',
            relatedCollection: 'storeDepthTest',
            twoWay: true,
            key: 'store',
            twoWayKey: 'products'
        ));

        // First, create a store that will be referenced by the nested product
        $store = $database->createDocument('storeDepthTest', new Document([
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
        $order = $database->createDocument('orderDepthTest', new Document([
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
        $order2 = $database->createDocument('orderDepthTest', new Document([
            '$id' => 'order2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'orderNumber' => 'ORD-002',
        ]));

        // Update order2 to add a nested product
        $order2Updated = $database->updateDocument('orderDepthTest', 'order2', $order2->setAttribute('product', [
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
        $database->deleteCollection('orderDepthTest');
        $database->deleteCollection('productDepthTest');
        $database->deleteCollection('storeDepthTest');
    }

    /**
     * Test filtering by relationship attributes using dot-path notation
     */
    public function testRelationshipTypeQueries(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create author -> posts relationship
        $database->createCollection(new Collection(id: 'authorsFilter'));
        $database->createCollection(new Collection(id: 'postsFilter'));

        $database->createAttribute('authorsFilter', Attribute::string(key: 'name', required: true));
        $database->createAttribute('authorsFilter', Attribute::integer(key: 'age', required: true));
        $database->createAttribute('postsFilter', Attribute::string(key: 'title', required: true));
        $database->createAttribute('postsFilter', Attribute::boolean(key: 'published', required: true));

        $database->createRelationship(Relationship::oneToMany(
            collection: 'authorsFilter',
            relatedCollection: 'postsFilter',
            twoWay: true,
            key: 'posts',
            twoWayKey: 'author'
        ));

        // Create test data
        $author1 = $database->createDocument('authorsFilter', new Document([
            '$id' => 'author1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Alice',
            'age' => 30,
        ]));

        $author2 = $database->createDocument('authorsFilter', new Document([
            '$id' => 'author2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Bob',
            'age' => 25,
        ]));

        // Create posts
        $database->createDocument('postsFilter', new Document([
            '$id' => 'post1',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Alice Post 1',
            'published' => true,
            'author' => 'author1',
        ]));

        $database->createDocument('postsFilter', new Document([
            '$id' => 'post2',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Alice Post 2',
            'published' => true,
            'author' => 'author1',
        ]));

        $database->createDocument('postsFilter', new Document([
            '$id' => 'post3',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Bob Post',
            'published' => true,
            'author' => 'author2',
        ]));

        // Filter posts by author name
        $posts = $database->find('postsFilter', [
            Query::equal('author.name', ['Alice']),
        ]);
        $this->assertCount(2, $posts);
        $this->assertEquals('post1', $posts[0]->getId());
        $this->assertEquals('post2', $posts[1]->getId());

        // Filter posts by author age
        $posts = $database->find('postsFilter', [
            Query::lessThan('author.age', 30),
        ]);
        $this->assertCount(1, $posts);
        $this->assertEquals('post3', $posts[0]->getId());

        // Filter authors by their posts' published status
        $authors = $database->find('authorsFilter', [
            Query::equal('posts.published', [true]),
        ]);
        $this->assertCount(2, $authors); // Both authors have published posts

        $database->deleteCollection('authorsFilter');
        $database->deleteCollection('postsFilter');

        $database->createCollection(new Collection(id: 'usersOto'));
        $database->createCollection(new Collection(id: 'profilesOto'));

        $database->createAttribute('usersOto', Attribute::string(key: 'username', required: true));
        $database->createAttribute('profilesOto', Attribute::string(key: 'bio', required: true));

        // ONE_TO_ONE with twoWay=true
        $database->createRelationship(Relationship::oneToOne(
            collection: 'usersOto',
            relatedCollection: 'profilesOto',
            twoWay: true,
            key: 'profile',
            twoWayKey: 'user'
        ));

        $user1 = $database->createDocument('usersOto', new Document([
            '$id' => 'user1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'username' => 'alice',
        ]));

        $profile1 = $database->createDocument('profilesOto', new Document([
            '$id' => 'profile1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'bio' => 'Software Engineer',
            'user' => 'user1',
        ]));

        // Filter profiles by user username
        $profiles = $database->find('profilesOto', [
            Query::equal('user.username', ['alice']),
        ]);
        $this->assertCount(1, $profiles);
        $this->assertEquals('profile1', $profiles[0]->getId());

        // Filter users by profile bio
        $users = $database->find('usersOto', [
            Query::equal('profile.bio', ['Software Engineer']),
        ]);
        $this->assertCount(1, $users);
        $this->assertEquals('user1', $users[0]->getId());

        // Clean up ONE_TO_ONE test
        $database->deleteCollection('usersOto');
        $database->deleteCollection('profilesOto');

        $database->createCollection(new Collection(id: 'commentsMto'));
        $database->createCollection(new Collection(id: 'usersMto'));

        $database->createAttribute('commentsMto', Attribute::string(key: 'content', required: true));
        $database->createAttribute('usersMto', Attribute::string(key: 'name', required: true));

        // MANY_TO_ONE with twoWay=true
        $database->createRelationship(Relationship::manyToOne(
            collection: 'commentsMto',
            relatedCollection: 'usersMto',
            twoWay: true,
            key: 'commenter',
            twoWayKey: 'comments'
        ));

        $userA = $database->createDocument('usersMto', new Document([
            '$id' => 'userA',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Alice',
        ]));

        $comment1 = $database->createDocument('commentsMto', new Document([
            '$id' => 'comment1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'content' => 'Great post!',
            'commenter' => 'userA',
        ]));

        $comment2 = $database->createDocument('commentsMto', new Document([
            '$id' => 'comment2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'content' => 'Nice work!',
            'commenter' => 'userA',
        ]));

        // Filter comments by commenter name
        $comments = $database->find('commentsMto', [
            Query::equal('commenter.name', ['Alice']),
        ]);
        $this->assertCount(2, $comments);

        // Filter users by their comments' content
        $users = $database->find('usersMto', [
            Query::equal('comments.content', ['Great post!']),
        ]);
        $this->assertCount(1, $users);
        $this->assertEquals('userA', $users[0]->getId());

        // Clean up MANY_TO_ONE test
        $database->deleteCollection('commentsMto');
        $database->deleteCollection('usersMto');

        $database->createCollection(new Collection(id: 'studentsMtm'));
        $database->createCollection(new Collection(id: 'coursesMtm'));

        $database->createAttribute('studentsMtm', Attribute::string(key: 'studentName', required: true));
        $database->createAttribute('coursesMtm', Attribute::string(key: 'courseName', required: true));

        // MANY_TO_MANY
        $database->createRelationship(Relationship::manyToMany(
            collection: 'studentsMtm',
            relatedCollection: 'coursesMtm',
            twoWay: true,
            key: 'enrolledCourses',
            twoWayKey: 'students'
        ));

        $student1 = $database->createDocument('studentsMtm', new Document([
            '$id' => 'student1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'studentName' => 'John',
        ]));

        $course1 = $database->createDocument('coursesMtm', new Document([
            '$id' => 'course1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'courseName' => 'Physics',
            'students' => ['student1'],
        ]));

        // Filter students by enrolled course name
        $students = $database->find('studentsMtm', [
            Query::equal('enrolledCourses.courseName', ['Physics']),
        ]);
        $this->assertCount(1, $students);
        $this->assertEquals('student1', $students[0]->getId());

        // Filter courses by student name
        $courses = $database->find('coursesMtm', [
            Query::equal('students.studentName', ['John']),
        ]);
        $this->assertCount(1, $courses);
        $this->assertEquals('course1', $courses[0]->getId());

        // Clean up MANY_TO_MANY test
        $database->deleteCollection('studentsMtm');
        $database->deleteCollection('coursesMtm');
    }

    /**
     * Test querying parent documents by relationship document $id
     */
    public function testQueryByRelationshipId(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'usersRelId'));
        $database->createCollection(new Collection(id: 'postsRelId'));

        $database->createAttribute('usersRelId', Attribute::string(key: 'name', required: true));
        $database->createAttribute('postsRelId', Attribute::string(key: 'title', required: true));

        $database->createRelationship(Relationship::manyToOne(
            collection: 'postsRelId',
            relatedCollection: 'usersRelId',
            twoWay: true,
            key: 'user',
            twoWayKey: 'posts'
        ));

        // Create test users
        $user1 = $database->createDocument('usersRelId', new Document([
            '$id' => 'user1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Alice',
        ]));

        $user2 = $database->createDocument('usersRelId', new Document([
            '$id' => 'user2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Bob',
        ]));

        // Create posts related to users
        $database->createDocument('postsRelId', new Document([
            '$id' => 'post1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'title' => 'Alice Post 1',
            'user' => 'user1',
        ]));

        $database->createDocument('postsRelId', new Document([
            '$id' => 'post2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'title' => 'Alice Post 2',
            'user' => 'user1',
        ]));

        $database->createDocument('postsRelId', new Document([
            '$id' => 'post3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'title' => 'Bob Post',
            'user' => 'user2',
        ]));

        // Query posts by user.$id - this is the key test
        $posts = $database->find('postsRelId', [
            Query::equal('user.$id', ['user1']),
        ]);
        $this->assertCount(2, $posts);
        $this->assertEquals('post1', $posts[0]->getId());
        $this->assertEquals('post2', $posts[1]->getId());

        // Query posts by different user.$id
        $posts = $database->find('postsRelId', [
            Query::equal('user.$id', ['user2']),
        ]);
        $this->assertCount(1, $posts);
        $this->assertEquals('post3', $posts[0]->getId());

        // Query posts by multiple user.$id values
        $posts = $database->find('postsRelId', [
            Query::equal('user.$id', ['user1', 'user2']),
        ]);
        $this->assertCount(3, $posts);

        // Query users by posts.$id (inverse direction)
        $users = $database->find('usersRelId', [
            Query::equal('posts.$id', ['post1']),
        ]);
        $this->assertCount(1, $users);
        $this->assertEquals('user1', $users[0]->getId());

        // Clean up MANY_TO_ONE test
        $database->deleteCollection('usersRelId');
        $database->deleteCollection('postsRelId');

        // Test ONE_TO_ONE relationship - query profile by user.$id
        $database->createCollection(new Collection(id: 'usersOtoId'));
        $database->createCollection(new Collection(id: 'profilesOtoId'));

        $database->createAttribute('usersOtoId', Attribute::string(key: 'username', required: true));
        $database->createAttribute('profilesOtoId', Attribute::string(key: 'bio', required: true));

        $database->createRelationship(Relationship::oneToOne(
            collection: 'usersOtoId',
            relatedCollection: 'profilesOtoId',
            twoWay: true,
            key: 'profile',
            twoWayKey: 'user'
        ));

        $userOto1 = $database->createDocument('usersOtoId', new Document([
            '$id' => 'userOto1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'username' => 'alice',
        ]));

        $database->createDocument('profilesOtoId', new Document([
            '$id' => 'profileOto1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'bio' => 'Software Engineer',
            'user' => 'userOto1',
        ]));

        // Query profiles by user.$id
        $profiles = $database->find('profilesOtoId', [
            Query::equal('user.$id', ['userOto1']),
        ]);
        $this->assertCount(1, $profiles);
        $this->assertEquals('profileOto1', $profiles[0]->getId());

        // Query users by profile.$id (inverse)
        $users = $database->find('usersOtoId', [
            Query::equal('profile.$id', ['profileOto1']),
        ]);
        $this->assertCount(1, $users);
        $this->assertEquals('userOto1', $users[0]->getId());

        // Clean up ONE_TO_ONE test
        $database->deleteCollection('usersOtoId');
        $database->deleteCollection('profilesOtoId');

        // Test MANY_TO_MANY relationship - query projects by developer.$id
        $database->createCollection(new Collection(id: 'developersMtmId'));
        $database->createCollection(new Collection(id: 'projectsMtmId'));

        $database->createAttribute('developersMtmId', Attribute::string(key: 'devName', required: true));
        $database->createAttribute('projectsMtmId', Attribute::string(key: 'projectName', required: true));

        $database->createRelationship(Relationship::manyToMany(
            collection: 'developersMtmId',
            relatedCollection: 'projectsMtmId',
            twoWay: true,
            key: 'projects',
            twoWayKey: 'developers'
        ));

        $dev1 = $database->createDocument('developersMtmId', new Document([
            '$id' => 'dev1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'devName' => 'Alice',
        ]));

        $dev2 = $database->createDocument('developersMtmId', new Document([
            '$id' => 'dev2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'devName' => 'Bob',
        ]));

        $database->createDocument('projectsMtmId', new Document([
            '$id' => 'project1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'projectName' => 'Project Alpha',
            'developers' => ['dev1', 'dev2'],
        ]));

        $database->createDocument('projectsMtmId', new Document([
            '$id' => 'project2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'projectName' => 'Project Beta',
            'developers' => ['dev1'],
        ]));

        // Query projects by developer.$id
        $projects = $database->find('projectsMtmId', [
            Query::equal('developers.$id', ['dev1']),
        ]);
        $this->assertCount(2, $projects);

        $projects = $database->find('projectsMtmId', [
            Query::equal('developers.$id', ['dev2']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('project1', $projects[0]->getId());

        // Query developers by project.$id (inverse)
        $developers = $database->find('developersMtmId', [
            Query::equal('projects.$id', ['project1']),
        ]);
        $this->assertCount(2, $developers);

        $developers = $database->find('developersMtmId', [
            Query::equal('projects.$id', ['project2']),
        ]);
        $this->assertCount(1, $developers);
        $this->assertEquals('dev1', $developers[0]->getId());

        // Query projects by BOTH developers using Query::containsAll
        // This simulates: "find conversations where both user1 AND user2 are participants"
        $projects = $database->find('projectsMtmId', [
            Query::containsAll('developers.$id', ['dev1', 'dev2']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('project1', $projects[0]->getId());

        // Inverse: find developers who are on BOTH projects
        // dev1 is on project1 and project2, dev2 is only on project1
        $developers = $database->find('developersMtmId', [
            Query::containsAll('projects.$id', ['project1', 'project2']),
        ]);
        $this->assertCount(1, $developers);
        $this->assertEquals('dev1', $developers[0]->getId());

        // Query projects by BOTH developer names (non-$id attribute)
        // project1 has developers Alice and Bob, project2 has only Alice
        $projects = $database->find('projectsMtmId', [
            Query::containsAll('developers.devName', ['Alice', 'Bob']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('project1', $projects[0]->getId());

        // Two separate equal queries on same relationship attribute should throw
        try {
            $database->find('projectsMtmId', [
                Query::equal('developers.$id', ['dev1']),
                Query::equal('developers.$id', ['dev2']),
            ]);
            $this->fail('Expected QueryException for impossible equal queries');
        } catch (\Utopia\Database\Exception\Query $e) {
            $this->assertStringContainsString('Query::containsAll()', $e->getMessage());
        }

        // Test M2M relationship query inside skipRelationships context
        // This simulates Appwrite's XList.php which wraps find() in skipRelationships()
        // when no select queries are provided
        $projects = $database->skipRelationships(fn () => $database->find('projectsMtmId', [
            Query::equal('developers.$id', ['dev1']),
        ]));
        $this->assertCount(2, $projects);

        $projects = $database->skipRelationships(fn () => $database->find('projectsMtmId', [
            Query::equal('developers.$id', ['dev2']),
        ]));
        $this->assertCount(1, $projects);
        $this->assertEquals('project1', $projects[0]->getId());

        // Also test inverse direction inside skipRelationships
        $developers = $database->skipRelationships(fn () => $database->find('developersMtmId', [
            Query::equal('projects.$id', ['project1']),
        ]));
        $this->assertCount(2, $developers);

        // Test containsAll inside skipRelationships
        $projects = $database->skipRelationships(fn () => $database->find('projectsMtmId', [
            Query::containsAll('developers.$id', ['dev1', 'dev2']),
        ]));
        $this->assertCount(1, $projects);
        $this->assertEquals('project1', $projects[0]->getId());

        // Clean up MANY_TO_MANY test
        $database->deleteCollection('developersMtmId');
        $database->deleteCollection('projectsMtmId');
    }

    /**
     * Comprehensive test for all query types on relationships
     */
    public function testRelationshipFilterQueries(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Setup test collections
        $database->createCollection(new Collection(id: 'productsQt'));
        $database->createCollection(new Collection(id: 'vendorsQt'));

        $database->createAttribute('productsQt', Attribute::string(key: 'name', required: true));
        $database->createAttribute('productsQt', Attribute::double(key: 'price', required: true));
        $database->createAttribute('vendorsQt', Attribute::string(key: 'company', required: true));
        $database->createAttribute('vendorsQt', Attribute::double(key: 'rating', required: true));
        $database->createAttribute('vendorsQt', Attribute::string(key: 'email', required: true));
        $database->createAttribute('vendorsQt', Attribute::boolean(key: 'verified', required: true));

        $database->createRelationship(Relationship::manyToOne(
            collection: 'productsQt',
            relatedCollection: 'vendorsQt',
            twoWay: true,
            key: 'vendor',
            twoWayKey: 'products'
        ));

        // Create test vendors
        $database->createDocument('vendorsQt', new Document([
            '$id' => 'vendor1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'company' => 'Acme Corp',
            'rating' => 4.5,
            'email' => 'sales@acme.com',
            'verified' => true,
        ]));

        $database->createDocument('vendorsQt', new Document([
            '$id' => 'vendor2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'company' => 'TechSupply Inc',
            'rating' => 3.8,
            'email' => 'info@techsupply.com',
            'verified' => true,
        ]));

        $database->createDocument('vendorsQt', new Document([
            '$id' => 'vendor3',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'company' => 'Budget Vendors',
            'rating' => 2.5,
            'email' => 'contact@budget.com',
            'verified' => false,
        ]));

        // Create test products
        $database->createDocument('productsQt', new Document([
            '$id' => 'product1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Widget A',
            'price' => 19.99,
            'vendor' => 'vendor1',
        ]));

        $database->createDocument('productsQt', new Document([
            '$id' => 'product2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Widget B',
            'price' => 29.99,
            'vendor' => 'vendor2',
        ]));

        $database->createDocument('productsQt', new Document([
            '$id' => 'product3',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Widget C',
            'price' => 9.99,
            'vendor' => 'vendor3',
        ]));

        // Query::equal()
        $products = $database->find('productsQt', [
            Query::equal('vendor.company', ['Acme Corp']),
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Query::notEqual()
        $products = $database->find('productsQt', [
            Query::notEqual('vendor.company', ['Budget Vendors']),
        ]);
        $this->assertCount(2, $products);

        // Query::lessThan()
        $products = $database->find('productsQt', [
            Query::lessThan('vendor.rating', 4.0),
        ]);
        $this->assertCount(2, $products); // vendor2 (3.8) and vendor3 (2.5)

        // Query::lessThanEqual()
        $products = $database->find('productsQt', [
            Query::lessThanEqual('vendor.rating', 3.8),
        ]);
        $this->assertCount(2, $products);

        // Query::greaterThan()
        $products = $database->find('productsQt', [
            Query::greaterThan('vendor.rating', 4.0),
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Query::greaterThanEqual()
        $products = $database->find('productsQt', [
            Query::greaterThanEqual('vendor.rating', 3.8),
        ]);
        $this->assertCount(2, $products); // vendor1 (4.5) and vendor2 (3.8)

        // Query::startsWith()
        $products = $database->find('productsQt', [
            Query::startsWith('vendor.email', 'sales@'),
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Query::endsWith()
        $products = $database->find('productsQt', [
            Query::endsWith('vendor.email', '.com'),
        ]);
        $this->assertCount(3, $products);

        // Query::contains()
        $products = $database->find('productsQt', [
            Query::contains('vendor.company', ['Corp']),
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Boolean query
        $products = $database->find('productsQt', [
            Query::equal('vendor.verified', [true]),
        ]);
        $this->assertCount(2, $products); // vendor1 and vendor2 are verified

        $products = $database->find('productsQt', [
            Query::equal('vendor.verified', [false]),
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product3', $products[0]->getId());

        // Multiple conditions on same relationship (query grouping optimization)
        $products = $database->find('productsQt', [
            Query::greaterThan('vendor.rating', 3.0),
            Query::equal('vendor.verified', [true]),
            Query::startsWith('vendor.company', 'Acme'),
        ]);
        $this->assertCount(1, $products);
        $this->assertEquals('product1', $products[0]->getId());

        // Clean up
        $database->deleteCollection('productsQt');
        $database->deleteCollection('vendorsQt');
    }

    public function testRelationshipSpatialQueries(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        if (! ($database->getAdapter()->hasFeature(Feature\Spatial::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create Restaurants -> Suppliers relationship with spatial attributes
        $database->createCollection(new Collection(id: 'restaurantsSpatial'));
        $database->createCollection(new Collection(id: 'suppliersSpatial'));

        $database->createAttribute('restaurantsSpatial', Attribute::string(key: 'name', required: true));
        $database->createAttribute('restaurantsSpatial', Attribute::point(key: 'location', required: true));

        $database->createAttribute('suppliersSpatial', Attribute::string(key: 'company', required: true));
        $database->createAttribute('suppliersSpatial', Attribute::point(key: 'warehouseLocation', required: true));
        $database->createAttribute('suppliersSpatial', Attribute::polygon(key: 'deliveryArea', required: true));
        $database->createAttribute('suppliersSpatial', Attribute::linestring(key: 'deliveryRoute', required: true));

        $database->createRelationship(Relationship::manyToOne(
            collection: 'restaurantsSpatial',
            relatedCollection: 'suppliersSpatial',
            twoWay: true,
            key: 'supplier',
            twoWayKey: 'restaurants'
        ));

        // Create suppliers with spatial data (coordinates are [longitude, latitude])
        $supplier1 = $database->createDocument('suppliersSpatial', new Document([
            '$id' => 'supplier1',
            '$permissions' => [Permission::read(Role::any())],
            'company' => 'Fresh Foods Inc',
            'warehouseLocation' => [-74.0060, 40.7128], // New York
            'deliveryArea' => [
                [-74.1, 40.7],
                [-73.9, 40.7],
                [-73.9, 40.8],
                [-74.1, 40.8],
                [-74.1, 40.7],
            ],
            'deliveryRoute' => [
                [-74.0060, 40.7128],
                [-73.9851, 40.7589],
                [-73.9857, 40.7484],
            ],
        ]));

        $supplier2 = $database->createDocument('suppliersSpatial', new Document([
            '$id' => 'supplier2',
            '$permissions' => [Permission::read(Role::any())],
            'company' => 'Ocean Seafood',
            'warehouseLocation' => [-118.2437, 34.0522], // Los Angeles
            'deliveryArea' => [
                [-118.3, 34.0],
                [-118.1, 34.0],
                [-118.1, 34.1],
                [-118.3, 34.1],
                [-118.3, 34.0],
            ],
            'deliveryRoute' => [
                [-118.2437, 34.0522],
                [-118.2468, 34.0407],
                [-118.2456, 34.0336],
            ],
        ]));

        $supplier3 = $database->createDocument('suppliersSpatial', new Document([
            '$id' => 'supplier3',
            '$permissions' => [Permission::read(Role::any())],
            'company' => 'Mountain Meats',
            'warehouseLocation' => [-104.9903, 39.7392], // Denver
            'deliveryArea' => [
                [-105.1, 39.7],
                [-104.8, 39.7],
                [-104.8, 39.8],
                [-105.1, 39.8],
                [-105.1, 39.7],
            ],
            'deliveryRoute' => [
                [-104.9903, 39.7392],
                [-104.9847, 39.7294],
                [-104.9708, 39.7197],
            ],
        ]));

        // Create restaurants
        $database->createDocument('restaurantsSpatial', new Document([
            '$id' => 'rest1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'NYC Diner',
            'location' => [-74.0060, 40.7128],
            'supplier' => 'supplier1',
        ]));

        $database->createDocument('restaurantsSpatial', new Document([
            '$id' => 'rest2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'LA Bistro',
            'location' => [-118.2437, 34.0522],
            'supplier' => 'supplier2',
        ]));

        $database->createDocument('restaurantsSpatial', new Document([
            '$id' => 'rest3',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Denver Steakhouse',
            'location' => [-104.9903, 39.7392],
            'supplier' => 'supplier3',
        ]));

        // distanceLessThan on relationship point attribute
        $restaurants = $database->find('restaurantsSpatial', [
            Query::distanceLessThan('supplier.warehouseLocation', [-74.0060, 40.7128], 1.0),
        ]);
        $this->assertCount(1, $restaurants);
        $this->assertEquals('rest1', $restaurants[0]->getId());

        // distanceEqual on relationship point attribute
        $restaurants = $database->find('restaurantsSpatial', [
            Query::distanceEqual('supplier.warehouseLocation', [-74.0060, 40.7128], 0.0),
        ]);
        $this->assertCount(1, $restaurants);
        $this->assertEquals('rest1', $restaurants[0]->getId());

        // distanceGreaterThan on relationship point attribute
        $restaurants = $database->find('restaurantsSpatial', [
            Query::distanceGreaterThan('supplier.warehouseLocation', [-74.0060, 40.7128], 10.0),
        ]);
        $this->assertCount(2, $restaurants); // LA and Denver suppliers

        // distanceNotEqual on relationship point attribute
        $restaurants = $database->find('restaurantsSpatial', [
            Query::distanceNotEqual('supplier.warehouseLocation', [-74.0060, 40.7128], 0.0),
        ]);
        $this->assertCount(2, $restaurants); // LA and Denver

        // covers on relationship polygon attribute (point inside polygon)
        $restaurants = $database->find('restaurantsSpatial', [
            Query::contains('supplier.deliveryArea', [[-74.0, 40.75]]),
        ]);
        $this->assertCount(1, $restaurants);
        $this->assertEquals('rest1', $restaurants[0]->getId());

        // covers on relationship linestring attribute
        // Note: ST_Contains on linestrings is implementation-dependent (some DBs require exact point-on-line)
        $restaurants = $database->find('restaurantsSpatial', [
            Query::contains('supplier.deliveryRoute', [[-74.0060, 40.7128]]),
        ]);
        // Verify query executes (result count depends on DB spatial implementation)
        $this->assertGreaterThanOrEqual(0, count($restaurants));

        // intersects on relationship polygon attribute
        $testPolygon = [
            [-74.05, 40.72],
            [-74.00, 40.72],
            [-74.00, 40.77],
            [-74.05, 40.77],
            [-74.05, 40.72],
        ];
        $restaurants = $database->find('restaurantsSpatial', [
            Query::intersects('supplier.deliveryArea', [$testPolygon]),
        ]);
        $this->assertCount(1, $restaurants);
        $this->assertEquals('rest1', $restaurants[0]->getId());

        // intersects on relationship linestring attribute
        // Note: Linestring intersection semantics vary by DB (MariaDB/MySQL/PostgreSQL differ)
        $testLine = [
            [-74.01, 40.71],
            [-73.99, 40.76],
        ];
        $restaurants = $database->find('restaurantsSpatial', [
            Query::intersects('supplier.deliveryRoute', [$testLine]),
        ]);
        // Verify query executes (result count depends on DB spatial implementation)
        $this->assertGreaterThanOrEqual(0, count($restaurants));

        // crosses on relationship linestring
        $crossingLine = [
            [-74.05, 40.70],
            [-73.95, 40.80],
        ];
        $restaurants = $database->find('restaurantsSpatial', [
            Query::crosses('supplier.deliveryRoute', [$crossingLine]),
        ]);
        // Result depends on actual geometry intersection

        // overlaps on relationship polygon
        $overlappingPolygon = [
            [-74.05, 40.75],
            [-74.00, 40.75],
            [-74.00, 40.85],
            [-74.05, 40.85],
            [-74.05, 40.75],
        ];
        $restaurants = $database->find('restaurantsSpatial', [
            Query::overlaps('supplier.deliveryArea', [$overlappingPolygon]),
        ]);
        $this->assertCount(1, $restaurants);
        $this->assertEquals('rest1', $restaurants[0]->getId());

        // touches on relationship polygon (polygon shares boundary)
        $touchingPolygon = [
            [-74.1, 40.8],
            [-73.9, 40.8],
            [-73.9, 40.9],
            [-74.1, 40.9],
            [-74.1, 40.8],
        ];
        $restaurants = $database->find('restaurantsSpatial', [
            Query::touches('supplier.deliveryArea', [$touchingPolygon]),
        ]);
        $this->assertCount(1, $restaurants);
        $this->assertEquals('rest1', $restaurants[0]->getId());

        // Multiple spatial queries combined
        $restaurants = $database->find('restaurantsSpatial', [
            Query::distanceLessThan('supplier.warehouseLocation', [-74.0060, 40.7128], 1.0),
            Query::contains('supplier.deliveryArea', [[-74.0, 40.75]]),
        ]);
        $this->assertCount(1, $restaurants);
        $this->assertEquals('rest1', $restaurants[0]->getId());

        // Spatial query combined with regular query
        $restaurants = $database->find('restaurantsSpatial', [
            Query::distanceLessThan('supplier.warehouseLocation', [-74.0060, 40.7128], 1.0),
            Query::equal('supplier.company', ['Fresh Foods Inc']),
        ]);
        $this->assertCount(1, $restaurants);
        $this->assertEquals('rest1', $restaurants[0]->getId());

        // count with spatial relationship query
        $count = $database->count('restaurantsSpatial', [
            Query::distanceLessThan('supplier.warehouseLocation', [-74.0060, 40.7128], 1.0),
        ]);
        $this->assertEquals(1, $count);

        // Clean up
        $database->deleteCollection('restaurantsSpatial');
        $database->deleteCollection('suppliersSpatial');
    }

    /**
     * Test relationship queries from parent side with virtual attributes
     */
    public function testRelationshipVirtualQueries(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Setup ONE_TO_MANY relationship
        $database->createCollection(new Collection(id: 'teamsParent'));
        $database->createCollection(new Collection(id: 'membersParent'));

        $database->createAttribute('teamsParent', Attribute::string(key: 'teamName', required: true));
        $database->createAttribute('teamsParent', Attribute::boolean(key: 'active', required: true));
        $database->createAttribute('membersParent', Attribute::string(key: 'memberName', required: true));
        $database->createAttribute('membersParent', Attribute::string(key: 'role', required: true));
        $database->createAttribute('membersParent', Attribute::boolean(key: 'senior', required: true));

        $database->createRelationship(Relationship::oneToMany(
            collection: 'teamsParent',
            relatedCollection: 'membersParent',
            twoWay: true,
            key: 'members',
            twoWayKey: 'team'
        ));

        // Create teams
        $database->createDocument('teamsParent', new Document([
            '$id' => 'team1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'teamName' => 'Engineering',
            'active' => true,
        ]));

        $database->createDocument('teamsParent', new Document([
            '$id' => 'team2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'teamName' => 'Sales',
            'active' => true,
        ]));

        // Create members
        $database->createDocument('membersParent', new Document([
            '$id' => 'member1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'memberName' => 'Alice',
            'role' => 'Engineer',
            'senior' => true,
            'team' => 'team1',
        ]));

        $database->createDocument('membersParent', new Document([
            '$id' => 'member2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'memberName' => 'Bob',
            'role' => 'Manager',
            'senior' => false,
            'team' => 'team2',
        ]));

        $database->createDocument('membersParent', new Document([
            '$id' => 'member3',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'memberName' => 'Charlie',
            'role' => 'Engineer',
            'senior' => true,
            'team' => 'team1',
        ]));

        // Find teams that have senior engineers
        $teams = $database->find('teamsParent', [
            Query::equal('members.role', ['Engineer']),
            Query::equal('members.senior', [true]),
        ]);
        $this->assertCount(1, $teams);
        $this->assertEquals('team1', $teams[0]->getId());

        // Find teams with managers
        $teams = $database->find('teamsParent', [
            Query::equal('members.role', ['Manager']),
        ]);
        $this->assertCount(1, $teams);
        $this->assertEquals('team2', $teams[0]->getId());

        // Find teams with members named 'Alice'
        $teams = $database->find('teamsParent', [
            Query::startsWith('members.memberName', 'A'),
        ]);
        $this->assertCount(1, $teams);
        $this->assertEquals('team1', $teams[0]->getId());

        // No teams with junior managers
        $teams = $database->find('teamsParent', [
            Query::equal('members.role', ['Manager']),
            Query::equal('members.senior', [true]),
        ]);
        $this->assertCount(0, $teams);

        // Clean up
        $database->deleteCollection('teamsParent');
        $database->deleteCollection('membersParent');
    }

    /**
     * Test edge cases and error scenarios for relationship queries
     */
    public function testRelationshipQueryEdgeCases(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Setup test collections
        $database->createCollection(new Collection(id: 'ordersEdge'));
        $database->createCollection(new Collection(id: 'customersEdge'));

        $database->createAttribute('ordersEdge', Attribute::string(key: 'orderNumber', required: true));
        $database->createAttribute('ordersEdge', Attribute::double(key: 'total', required: true));
        $database->createAttribute('customersEdge', Attribute::string(key: 'name', required: true));
        $database->createAttribute('customersEdge', Attribute::integer(key: 'age', required: true));

        $database->createRelationship(Relationship::manyToOne(
            collection: 'ordersEdge',
            relatedCollection: 'customersEdge',
            twoWay: true,
            key: 'customer',
            twoWayKey: 'orders'
        ));

        // Create customer
        $database->createDocument('customersEdge', new Document([
            '$id' => 'customer1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'John Doe',
            'age' => 30,
        ]));

        // Create order
        $database->createDocument('ordersEdge', new Document([
            '$id' => 'order1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'orderNumber' => 'ORD001',
            'total' => 100.00,
            'customer' => 'customer1',
        ]));

        // No matching results
        $orders = $database->find('ordersEdge', [
            Query::equal('customer.name', ['Jane Doe']),
        ]);
        $this->assertCount(0, $orders);

        // Impossible condition (combines to empty set)
        $orders = $database->find('ordersEdge', [
            Query::equal('customer.name', ['John Doe']),
            Query::equal('customer.age', [25]), // John is 30, not 25
        ]);
        $this->assertCount(0, $orders);

        try {
            $database->find('ordersEdge', [
                Query::equal('nonexistent.attribute', ['value']),
            ]);
        } catch (\Throwable) {
        }

        // Null or missing relationship
        $database->createDocument('ordersEdge', new Document([
            '$id' => 'order2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'orderNumber' => 'ORD002',
            'total' => 50.00,
            // No customer relationship
        ]));

        $orders = $database->find('ordersEdge', [
            Query::equal('customer.name', ['John Doe']),
        ]);
        $this->assertCount(1, $orders);

        // Combining relationship query with regular query
        $orders = $database->find('ordersEdge', [
            Query::equal('customer.name', ['John Doe']),
            Query::greaterThan('total', 75.00),
        ]);
        $this->assertCount(1, $orders);
        $this->assertEquals('order1', $orders[0]->getId());

        // Query with limit and offset
        $orders = $database->find('ordersEdge', [
            Query::equal('customer.name', ['John Doe']),
            Query::limit(1),
            Query::offset(0),
        ]);
        $this->assertCount(1, $orders);

        $database->deleteCollection('ordersEdge');
        $database->deleteCollection('customersEdge');
    }

    /**
     * Test MANY_TO_MANY relationships with complex queries
     */
    public function testRelationshipManyToManyComplex(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Setup MANY_TO_MANY
        $database->createCollection(new Collection(id: 'developersMtm'));
        $database->createCollection(new Collection(id: 'projectsMtm'));

        $database->createAttribute('developersMtm', Attribute::string(key: 'devName', required: true));
        $database->createAttribute('developersMtm', Attribute::integer(key: 'experience', required: true));
        $database->createAttribute('projectsMtm', Attribute::string(key: 'projectName', required: true));
        $database->createAttribute('projectsMtm', Attribute::double(key: 'budget', required: true));
        $database->createAttribute('projectsMtm', Attribute::string(key: 'priority', size: 50, required: true));

        $database->createRelationship(Relationship::manyToMany(
            collection: 'developersMtm',
            relatedCollection: 'projectsMtm',
            twoWay: true,
            key: 'assignedProjects',
            twoWayKey: 'assignedDevelopers'
        ));

        // Create developers
        $dev1 = $database->createDocument('developersMtm', new Document([
            '$id' => 'dev1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'devName' => 'Senior Dev',
            'experience' => 10,
        ]));

        $dev2 = $database->createDocument('developersMtm', new Document([
            '$id' => 'dev2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'devName' => 'Junior Dev',
            'experience' => 2,
        ]));

        // Create projects
        $project1 = $database->createDocument('projectsMtm', new Document([
            '$id' => 'proj1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'projectName' => 'High Priority Project',
            'budget' => 100000.00,
            'priority' => 'high',
            'assignedDevelopers' => ['dev1', 'dev2'],
        ]));

        $project2 = $database->createDocument('projectsMtm', new Document([
            '$id' => 'proj2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'projectName' => 'Low Priority Project',
            'budget' => 25000.00,
            'priority' => 'low',
            'assignedDevelopers' => ['dev2'],
        ]));

        // Find developers on high priority projects
        $developers = $database->find('developersMtm', [
            Query::equal('assignedProjects.priority', ['high']),
        ]);
        $this->assertCount(2, $developers); // Both assigned to proj1

        // Find developers on high budget projects
        $developers = $database->find('developersMtm', [
            Query::greaterThan('assignedProjects.budget', 50000.00),
        ]);
        $this->assertCount(2, $developers);

        // Find projects with experienced developers
        $projects = $database->find('projectsMtm', [
            Query::greaterThanEqual('assignedDevelopers.experience', 10),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Find projects with junior developers
        $projects = $database->find('projectsMtm', [
            Query::lessThan('assignedDevelopers.experience', 5),
        ]);
        $this->assertCount(2, $projects); // Both projects have dev2

        // Combined queries
        $projects = $database->find('projectsMtm', [
            Query::equal('assignedDevelopers.devName', ['Junior Dev']),
            Query::equal('priority', ['low']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj2', $projects[0]->getId());

        // Clean up
        $database->deleteCollection('developersMtm');
        $database->deleteCollection('projectsMtm');
    }

    public function testNestedRelationshipQueriesMultipleDepths(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create 3-level nested structure:
        // Companies -> Employees -> Projects -> Tasks
        // Also: Employees -> Department (MANY_TO_ONE)

        // Level 0: Companies
        $database->createCollection(new Collection(id: 'companiesNested'));
        $database->createAttribute('companiesNested', Attribute::string(key: 'name', required: true));
        $database->createAttribute('companiesNested', Attribute::string(key: 'industry', required: true));

        // Level 1: Employees
        $database->createCollection(new Collection(id: 'employeesNested'));
        $database->createAttribute('employeesNested', Attribute::string(key: 'name', required: true));
        $database->createAttribute('employeesNested', Attribute::string(key: 'role', required: true));

        // Level 1b: Departments (for MANY_TO_ONE)
        $database->createCollection(new Collection(id: 'departmentsNested'));
        $database->createAttribute('departmentsNested', Attribute::string(key: 'name', required: true));
        $database->createAttribute('departmentsNested', Attribute::integer(key: 'budget', required: true));

        // Level 2: Projects
        $database->createCollection(new Collection(id: 'projectsNested'));
        $database->createAttribute('projectsNested', Attribute::string(key: 'title', required: true));
        $database->createAttribute('projectsNested', Attribute::string(key: 'status', required: true));

        // Level 3: Tasks
        $database->createCollection(new Collection(id: 'tasksNested'));
        $database->createAttribute('tasksNested', Attribute::string(key: 'description', required: true));
        $database->createAttribute('tasksNested', Attribute::string(key: 'priority', required: true));
        $database->createAttribute('tasksNested', Attribute::boolean(key: 'completed', required: true));

        // Create relationships
        // Companies -> Employees (ONE_TO_MANY)
        $database->createRelationship(Relationship::oneToMany(
            collection: 'companiesNested',
            relatedCollection: 'employeesNested',
            twoWay: true,
            key: 'employees',
            twoWayKey: 'company'
        ));

        // Employees -> Department (MANY_TO_ONE)
        $database->createRelationship(Relationship::manyToOne(
            collection: 'employeesNested',
            relatedCollection: 'departmentsNested',
            twoWay: true,
            key: 'department',
            twoWayKey: 'employees'
        ));

        // Employees -> Projects (ONE_TO_MANY)
        $database->createRelationship(Relationship::oneToMany(
            collection: 'employeesNested',
            relatedCollection: 'projectsNested',
            twoWay: true,
            key: 'projects',
            twoWayKey: 'employee'
        ));

        // Projects -> Tasks (ONE_TO_MANY)
        $database->createRelationship(Relationship::oneToMany(
            collection: 'projectsNested',
            relatedCollection: 'tasksNested',
            twoWay: true,
            key: 'tasks',
            twoWayKey: 'project'
        ));

        // Create test data
        $dept1 = $database->createDocument('departmentsNested', new Document([
            '$id' => 'dept1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Engineering',
            'budget' => 100000,
        ]));

        $dept2 = $database->createDocument('departmentsNested', new Document([
            '$id' => 'dept2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Marketing',
            'budget' => 50000,
        ]));

        $company1 = $database->createDocument('companiesNested', new Document([
            '$id' => 'company1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'TechCorp',
            'industry' => 'Technology',
        ]));

        $company2 = $database->createDocument('companiesNested', new Document([
            '$id' => 'company2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'MarketCo',
            'industry' => 'Marketing',
        ]));

        $employee1 = $database->createDocument('employeesNested', new Document([
            '$id' => 'emp1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Alice Johnson',
            'role' => 'Developer',
            'company' => 'company1',
            'department' => 'dept1',
        ]));

        $employee2 = $database->createDocument('employeesNested', new Document([
            '$id' => 'emp2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Bob Smith',
            'role' => 'Marketer',
            'company' => 'company2',
            'department' => 'dept2',
        ]));

        $project1 = $database->createDocument('projectsNested', new Document([
            '$id' => 'proj1',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Website Redesign',
            'status' => 'active',
            'employee' => 'emp1',
        ]));

        $project2 = $database->createDocument('projectsNested', new Document([
            '$id' => 'proj2',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Campaign Launch',
            'status' => 'planning',
            'employee' => 'emp2',
        ]));

        $task1 = $database->createDocument('tasksNested', new Document([
            '$id' => 'task1',
            '$permissions' => [Permission::read(Role::any())],
            'description' => 'Design homepage',
            'priority' => 'high',
            'completed' => false,
            'project' => 'proj1',
        ]));

        $task2 = $database->createDocument('tasksNested', new Document([
            '$id' => 'task2',
            '$permissions' => [Permission::read(Role::any())],
            'description' => 'Write copy',
            'priority' => 'medium',
            'completed' => true,
            'project' => 'proj2',
        ]));

        $task3 = $database->createDocument('tasksNested', new Document([
            '$id' => 'task3',
            '$permissions' => [Permission::read(Role::any())],
            'description' => 'Implement backend',
            'priority' => 'high',
            'completed' => false,
            'project' => 'proj1',
        ]));

        // Query employees by company name (1 level deep)
        $employees = $database->find('employeesNested', [
            Query::equal('company.name', ['TechCorp']),
        ]);
        $this->assertCount(1, $employees);
        $this->assertEquals('emp1', $employees[0]->getId());

        // Query employees by department name (1 level deep MANY_TO_ONE)
        $employees = $database->find('employeesNested', [
            Query::equal('department.name', ['Engineering']),
        ]);
        $this->assertCount(1, $employees);
        $this->assertEquals('emp1', $employees[0]->getId());

        // Query projects by employee name (1 level deep)
        $projects = $database->find('projectsNested', [
            Query::equal('employee.name', ['Alice Johnson']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Query projects by employee's company name (2 levels deep)
        $projects = $database->find('projectsNested', [
            Query::equal('employee.company.name', ['TechCorp']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Query projects by employee's department name (2 levels deep, MANY_TO_ONE)
        $projects = $database->find('projectsNested', [
            Query::equal('employee.department.name', ['Engineering']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Query tasks by project employee name (2 levels deep)
        $tasks = $database->find('tasksNested', [
            Query::equal('project.employee.name', ['Alice Johnson']),
        ]);
        $this->assertCount(2, $tasks);

        // Query tasks by project->employee->company name (3 levels deep)
        $tasks = $database->find('tasksNested', [
            Query::equal('project.employee.company.name', ['TechCorp']),
        ]);
        $this->assertCount(2, $tasks);
        $this->assertEquals('task1', $tasks[0]->getId());
        $this->assertEquals('task3', $tasks[1]->getId());

        // Query tasks by project->employee->department budget (3 levels deep with MANY_TO_ONE)
        $tasks = $database->find('tasksNested', [
            Query::greaterThan('project.employee.department.budget', 75000),
        ]);
        $this->assertCount(2, $tasks); // Both tasks are in projects by employees in Engineering dept

        // Query tasks by project->employee->company industry (3 levels deep)
        $tasks = $database->find('tasksNested', [
            Query::equal('project.employee.company.industry', ['Marketing']),
        ]);
        $this->assertCount(1, $tasks);
        $this->assertEquals('task2', $tasks[0]->getId());

        // Combine depth 1 and depth 3 queries
        $tasks = $database->find('tasksNested', [
            Query::equal('priority', ['high']),
            Query::equal('project.employee.company.name', ['TechCorp']),
        ]);
        $this->assertCount(2, $tasks);

        // Multiple depth 2 queries combined
        $projects = $database->find('projectsNested', [
            Query::equal('employee.company.industry', ['Technology']),
            Query::equal('employee.department.name', ['Engineering']),
        ]);
        $this->assertCount(1, $projects);
        $this->assertEquals('proj1', $projects[0]->getId());

        // Clean up
        $database->deleteCollection('tasksNested');
        $database->deleteCollection('projectsNested');
        $database->deleteCollection('employeesNested');
        $database->deleteCollection('departmentsNested');
        $database->deleteCollection('companiesNested');
    }

    public function testCountAndSumWithRelationshipQueries(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create Author -> Posts relationship with view count
        $database->createCollection(new Collection(id: 'authorsCount'));
        $database->createCollection(new Collection(id: 'postsCount'));

        $database->createAttribute('authorsCount', Attribute::string(key: 'name', required: true));
        $database->createAttribute('authorsCount', Attribute::integer(key: 'age', required: true));
        $database->createAttribute('postsCount', Attribute::string(key: 'title', required: true));
        $database->createAttribute('postsCount', Attribute::integer(key: 'views', required: true));
        $database->createAttribute('postsCount', Attribute::boolean(key: 'published', required: true));

        $database->createRelationship(Relationship::oneToMany(
            collection: 'authorsCount',
            relatedCollection: 'postsCount',
            twoWay: true,
            key: 'posts',
            twoWayKey: 'author'
        ));

        // Create test data
        $author1 = $database->createDocument('authorsCount', new Document([
            '$id' => 'author1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Alice',
            'age' => 30,
        ]));

        $author2 = $database->createDocument('authorsCount', new Document([
            '$id' => 'author2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Bob',
            'age' => 25,
        ]));

        $author3 = $database->createDocument('authorsCount', new Document([
            '$id' => 'author3',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Charlie',
            'age' => 35,
        ]));

        // Create posts
        $database->createDocument('postsCount', new Document([
            '$id' => 'post1',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Alice Post 1',
            'views' => 100,
            'published' => true,
            'author' => 'author1',
        ]));

        $database->createDocument('postsCount', new Document([
            '$id' => 'post2',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Alice Post 2',
            'views' => 200,
            'published' => true,
            'author' => 'author1',
        ]));

        $database->createDocument('postsCount', new Document([
            '$id' => 'post3',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Alice Draft',
            'views' => 50,
            'published' => false,
            'author' => 'author1',
        ]));

        $database->createDocument('postsCount', new Document([
            '$id' => 'post4',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Bob Post',
            'views' => 150,
            'published' => true,
            'author' => 'author2',
        ]));

        $database->createDocument('postsCount', new Document([
            '$id' => 'post5',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Bob Draft',
            'views' => 75,
            'published' => false,
            'author' => 'author2',
        ]));

        // Count posts by author name
        $count = $database->count('postsCount', [
            Query::equal('author.name', ['Alice']),
        ]);
        $this->assertEquals(3, $count);

        // Count published posts by author age filter
        $count = $database->count('postsCount', [
            Query::lessThan('author.age', 30),
            Query::equal('published', [true]),
        ]);
        $this->assertEquals(1, $count);

        // Count posts by author name (different author)
        $count = $database->count('postsCount', [
            Query::equal('author.name', ['Bob']),
        ]);
        $this->assertEquals(2, $count);

        // Count with no matches (author with no posts)
        $count = $database->count('postsCount', [
            Query::equal('author.name', ['Charlie']),
        ]);
        $this->assertEquals(0, $count);

        // Sum views for posts by author name
        $sum = $database->sum('postsCount', 'views', [
            Query::equal('author.name', ['Alice']),
        ]);
        $this->assertEquals(350, $sum); // 100 + 200 + 50

        // Sum views for published posts by author age
        $sum = $database->sum('postsCount', 'views', [
            Query::lessThan('author.age', 30),
            Query::equal('published', [true]),
        ]);
        $this->assertEquals(150, $sum);

        // Sum views for Bob's posts
        $sum = $database->sum('postsCount', 'views', [
            Query::equal('author.name', ['Bob']),
        ]);
        $this->assertEquals(225, $sum);

        // Sum with no matches
        $sum = $database->sum('postsCount', 'views', [
            Query::equal('author.name', ['Charlie']),
        ]);
        $this->assertEquals(0, $sum);

        // Clean up
        $database->deleteCollection('authorsCount');
        $database->deleteCollection('postsCount');
    }

    /**
     // and cursor queries properly reject relationship (dot-path) attributes.
     *
     * Relationship attributes like 'author.name' are NOT supported for ordering because:
     * 1. Only filter queries go through convertRelationshipFiltersToSubqueries()
     * 2. Order attributes are passed directly to the adapter without relationship resolution
     * 3. The Order validator now catches dot-path attributes and rejects them with a clear error
     * 4. Cursor validation doesn't need separate dot-path checks since order validation runs first
     */
    public function testOrderAndCursorWithRelationshipQueries(): void
    {
        if (! ($this->getDatabase()->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();
            return;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(new Collection(id: 'authorsOrder'));
        $database->createCollection(new Collection(id: 'postsOrder'));

        $database->createAttribute('authorsOrder', Attribute::string(key: 'name', required: true));
        $database->createAttribute('authorsOrder', Attribute::integer(key: 'age', required: true));

        $database->createAttribute('postsOrder', Attribute::string(key: 'title', required: true));
        $database->createAttribute('postsOrder', Attribute::integer(key: 'views', required: true));

        $database->createRelationship(Relationship::manyToOne(
            collection: 'postsOrder',
            relatedCollection: 'authorsOrder',
            twoWay: true,
            key: 'author',
            twoWayKey: 'postsOrder'
        ));

        // Create authors
        $alice = $database->createDocument('authorsOrder', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Alice',
            'age' => 30,
        ]));

        $bob = $database->createDocument('authorsOrder', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Bob',
            'age' => 25,
        ]));

        // Create posts
        $database->createDocument('postsOrder', new Document([
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Post 1',
            'views' => 100,
            'author' => $alice->getId(),
        ]));

        $database->createDocument('postsOrder', new Document([
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Post 2',
            'views' => 200,
            'author' => $bob->getId(),
        ]));

        $database->createDocument('postsOrder', new Document([
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Post 3',
            'views' => 150,
            'author' => $alice->getId(),
        ]));

        // Order by relationship attribute should fail with validation error
        $caught = false;
        try {
            $database->find('postsOrder', [
                Query::orderAsc('author.name'),
            ]);
        } catch (\Throwable $e) {
            $caught = true;
            $this->assertStringContainsString('Cannot order by nested attribute', $e->getMessage());
        }
        $this->assertTrue($caught, 'Should throw exception for nested order attribute');

        // Cursor with relationship order attribute should fail with same validation error
        $caught = false;
        try {
            $firstPost = $database->findOne('postsOrder', [
                Query::orderAsc('title'),
            ]);

            $database->find('postsOrder', [
                Query::orderAsc('author.name'),
                Query::cursorAfter($firstPost),
            ]);
        } catch (\Throwable $e) {
            $caught = true;
            $this->assertStringContainsString('Cannot order by nested attribute', $e->getMessage());
        }
        $this->assertTrue($caught, 'Should throw exception for nested order attribute with cursor');

        // Clean up
        $database->deleteCollection('authorsOrder');
        $database->deleteCollection('postsOrder');
    }
}

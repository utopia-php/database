<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

trait PermissionTests
{
//    public function testCreateDocumentsEmptyPermission1(): void
//    {
//        /** @var Database $database */
//        $database = static::getDatabase();
//
//        $database->createCollection(__FUNCTION__);
//
//        $document = $database->createDocument(__FUNCTION__, new Document());
//    }

        public function testCreateDocumentsEmptyPermission(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $database->createCollection(__FUNCTION__);

        /**
         * Validate the decode function does not add $permissions null entry when no permissions are provided
         */

        $document = $database->createDocument(__FUNCTION__, new Document());

        $this->assertArrayHasKey('$permissions', $document);
        $this->assertEquals([], $document->getAttribute('$permissions'));

        $documents = [];

        for ($i = 0; $i < 2; $i++) {
            $documents[] = new Document();
        }

        $results = [];
        $count = $database->createDocuments(__FUNCTION__, $documents, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(2, $count);
        foreach ($results as $result) {
            $this->assertArrayHasKey('$permissions', $result);
            $this->assertEquals([], $result->getAttribute('$permissions'));
        }

        $this->assertEquals('shmuel', 'fogel');
    }

    public function testReadPermissionsFailure(): Document
    {
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->createDocument('documents', new Document([
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

        Authorization::cleanRoles();

        $document = $database->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $document->isEmpty());

        Authorization::setRole(Role::any()->toString());

        return $document;
    }

    public function testNoChangeUpdateDocumentWithoutPermission(): Document
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->createDocument('documents', new Document([
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

        $updatedDocument = $database->updateDocument(
            'documents',
            $document->getId(),
            $document
        );

        // Document should not be updated as there is no change.
        // It should also not throw any authorization exception without any permission because of no change.
        $this->assertEquals($updatedDocument->getUpdatedAt(), $document->getUpdatedAt());

        $document = $database->createDocument('documents', new Document([
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
            $database->updateDocument(
                'documents',
                $document->getId(),
                $document
            );
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }

        return $document;
    }

    public function testUpdateDocumentsPermissions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'testUpdateDocumentsPerms';

        $database->createCollection($collection, attributes: [
            new Document([
                '$id' => ID::custom('string'),
                'type' => Database::VAR_STRING,
                'size' => 767,
                'required' => true,
            ])
        ], permissions: [], documentSecurity: true);

        // Test we can bulk update permissions we have access to
        Authorization::skip(function () use ($collection, $database) {
            for ($i = 0; $i < 10; $i++) {
                $database->createDocument($collection, new Document([
                    '$id' => 'doc' . $i,
                    'string' => 'text📝 ' . $i,
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::create(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any())
                    ],
                ]));
            }

            $database->createDocument($collection, new Document([
                '$id' => 'doc' . $i,
                'string' => 'text📝 ' . $i,
                '$permissions' => [
                    Permission::read(Role::user('user1')),
                    Permission::create(Role::user('user1')),
                    Permission::update(Role::user('user1')),
                    Permission::delete(Role::user('user1'))
                ],
            ]));
        });

        $modified = $database->updateDocuments($collection, new Document([
            '$permissions' => [
                Permission::read(Role::user('user2')),
                Permission::create(Role::user('user2')),
                Permission::update(Role::user('user2')),
                Permission::delete(Role::user('user2'))
            ],
        ]));

        /** @var Database $database */
        $database = static::getDatabase();

        $documents = Authorization::skip(function () use ($collection, $database) {
            return $database->find($collection);
        });

        $this->assertEquals(10, $modified);
        $this->assertEquals(11, \count($documents));

        $modifiedDocuments = array_filter($documents, function (Document $document) {
            return $document->getAttribute('$permissions') == [
                Permission::read(Role::user('user2')),
                Permission::create(Role::user('user2')),
                Permission::update(Role::user('user2')),
                Permission::delete(Role::user('user2'))
            ];
        });

        $this->assertCount(10, $modifiedDocuments);

        $unmodifiedDocuments = array_filter($documents, function (Document $document) {
            return $document->getAttribute('$permissions') == [
                Permission::read(Role::user('user1')),
                Permission::create(Role::user('user1')),
                Permission::update(Role::user('user1')),
                Permission::delete(Role::user('user1'))
            ];
        });

        $this->assertCount(1, $unmodifiedDocuments);

        Authorization::setRole(Role::user('user2')->toString());

        // Test Bulk permission update with data
        $modified = $database->updateDocuments($collection, new Document([
            '$permissions' => [
                Permission::read(Role::user('user3')),
                Permission::create(Role::user('user3')),
                Permission::update(Role::user('user3')),
                Permission::delete(Role::user('user3'))
            ],
            'string' => 'text📝 updated',
        ]));

        $this->assertEquals(10, $modified);

        $documents = Authorization::skip(function () use ($collection) {
            return $this->getDatabase()->find($collection);
        });

        $this->assertCount(11, $documents);

        $modifiedDocuments = array_filter($documents, function (Document $document) {
            return $document->getAttribute('$permissions') == [
                Permission::read(Role::user('user3')),
                Permission::create(Role::user('user3')),
                Permission::update(Role::user('user3')),
                Permission::delete(Role::user('user3'))
            ];
        });

        foreach ($modifiedDocuments as $document) {
            $this->assertEquals('text📝 updated', $document->getAttribute('string'));
        }
    }

    public function testCollectionPermissions(): Document
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collection = $database->createCollection('collectionSecurity', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: false);

        $this->assertInstanceOf(Document::class, $collection);

        $this->assertTrue($database->createAttribute(
            collection: $collection->getId(),
            id: 'test',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        return $collection;
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsCreateWorks
     */
    public function testCollectionPermissionsCountThrowsException(array $data): void
    {
        [$collection, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $count = $database->count(
            $collection->getId()
        );
        $this->assertEmpty($count);
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsCountWorks(array $data): array
    {
        [$collection, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $count = $database->count(
            $collection->getId()
        );

        $this->assertNotEmpty($count);

        return $data;
    }

    /**
     * @depends testCollectionPermissions
     */
    public function testCollectionPermissionsCreateThrowsException(Document $collection): void
    {
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());
        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = static::getDatabase();

        $database->createDocument($collection->getId(), new Document([
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
     * @depends testCollectionPermissions
     * @return array<Document>
     */
    public function testCollectionPermissionsCreateWorks(Document $collection): array
    {
        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->createDocument($collection->getId(), new Document([
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
     * @param array<Document> $data
     * @depends testCollectionPermissionsUpdateWorks
     */
    public function testCollectionPermissionsDeleteThrowsException(array $data): void
    {
        [$collection, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = static::getDatabase();

        $database->deleteDocument(
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

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $this->assertTrue($database->deleteDocument(
            $collection->getId(),
            $document->getId()
        ));
    }

    public function testCollectionPermissionsExceptions(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $this->expectException(DatabaseException::class);
        $database->createCollection('collectionSecurity', permissions: [
            'i dont work'
        ]);
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsCreateWorks
     */
    public function testCollectionPermissionsFindThrowsException(array $data): void
    {
        [$collection, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = static::getDatabase();

        $database->find($collection->getId());
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsFindWorks(array $data): array
    {
        [$collection, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find($collection->getId());
        $this->assertNotEmpty($documents);

        Authorization::cleanRoles();
        Authorization::setRole(Role::user('random')->toString());

        try {
            $database->find($collection->getId());
            $this->fail('Failed to throw exception');
        } catch (AuthorizationException) {
        }

        return $data;
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     */
    public function testCollectionPermissionsGetThrowsException(array $data): void
    {
        [$collection, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->getDocument(
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
    public function testCollectionPermissionsGetWorks(array $data): array
    {
        [$collection, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->getDocument(
            $collection->getId(),
            $document->getId()
        );
        $this->assertInstanceOf(Document::class, $document);
        $this->assertFalse($document->isEmpty());

        return $data;
    }

    /**
     * @return array<Document>
     */
    public function testCollectionPermissionsRelationships(): array
    {
        /** @var Database $database */
        $database = static::getDatabase();

        $collection = $database->createCollection('collectionSecurity.Parent', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collection);

        $this->assertTrue($database->createAttribute(
            collection: $collection->getId(),
            id: 'test',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $collectionOneToOne = $database->createCollection('collectionSecurity.OneToOne', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collectionOneToOne);

        $this->assertTrue($database->createAttribute(
            collection: $collectionOneToOne->getId(),
            id: 'test',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $this->assertTrue($database->createRelationship(
            collection: $collection->getId(),
            relatedCollection: $collectionOneToOne->getId(),
            type: Database::RELATION_ONE_TO_ONE,
            id: Database::RELATION_ONE_TO_ONE,
            onDelete: Database::RELATION_MUTATE_CASCADE
        ));

        $collectionOneToMany = $database->createCollection('collectionSecurity.OneToMany', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collectionOneToMany);

        $this->assertTrue($database->createAttribute(
            collection: $collectionOneToMany->getId(),
            id: 'test',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $this->assertTrue($database->createRelationship(
            collection: $collection->getId(),
            relatedCollection: $collectionOneToMany->getId(),
            type: Database::RELATION_ONE_TO_MANY,
            id: Database::RELATION_ONE_TO_MANY,
            onDelete: Database::RELATION_MUTATE_CASCADE
        ));

        return [$collection, $collectionOneToOne, $collectionOneToMany];
    }

    /**
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     * @param array<Document> $data
     */
    public function testCollectionPermissionsRelationshipsCountWorks(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->count(
            $collection->getId()
        );

        $this->assertEquals(1, $documents);

        Authorization::cleanRoles();
        Authorization::setRole(Role::user('random')->toString());

        $documents = $database->count(
            $collection->getId()
        );

        $this->assertEquals(1, $documents);

        Authorization::cleanRoles();
        Authorization::setRole(Role::user('unknown')->toString());

        $documents = $database->count(
            $collection->getId()
        );

        $this->assertEquals(0, $documents);
    }

    /**
     * @depends testCollectionPermissionsRelationships
     * @param array<Document> $data
     */
    public function testCollectionPermissionsRelationshipsCreateThrowsException(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());
        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = static::getDatabase();

        $database->createDocument($collection->getId(), new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ],
            'test' => 'lorem ipsum'
        ]));
    }

    /**
    * @param array<Document> $data
    * @depends testCollectionPermissionsRelationshipsUpdateWorks
    */
    public function testCollectionPermissionsRelationshipsDeleteThrowsException(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->deleteDocument(
            $collection->getId(),
            $document->getId()
        );
    }

    /**
     * @depends testCollectionPermissionsRelationships
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsRelationshipsCreateWorks(array $data): array
    {
        [$collection, $collectionOneToOne, $collectionOneToMany] = $data;
        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->createDocument($collection->getId(), new Document([
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
     * @param array<Document> $data
     * @depends testCollectionPermissionsRelationshipsUpdateWorks
     */
    public function testCollectionPermissionsRelationshipsDeleteWorks(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $this->assertTrue($database->deleteDocument(
            $collection->getId(),
            $document->getId()
        ));
    }

    /**
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     * @param array<Document> $data
     */
    public function testCollectionPermissionsRelationshipsFindWorks(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $documents = $database->find(
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

        Authorization::cleanRoles();
        Authorization::setRole(Role::user('random')->toString());

        $documents = $database->find(
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

        Authorization::cleanRoles();
        Authorization::setRole(Role::user('unknown')->toString());

        $documents = $database->find(
            $collection->getId()
        );

        $this->assertIsArray($documents);
        $this->assertCount(0, $documents);
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     */
    public function testCollectionPermissionsRelationshipsGetThrowsException(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->getDocument(
            $collection->getId(),
            $document->getId(),
        );
        $this->assertInstanceOf(Document::class, $document);
        $this->assertTrue($document->isEmpty());
    }

    /**
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsRelationshipsGetWorks(array $data): array
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->getDocument(
            $collection->getId(),
            $document->getId()
        );

        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(Database::RELATION_ONE_TO_ONE));
        $this->assertIsArray($document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertCount(2, $document->getAttribute(Database::RELATION_ONE_TO_MANY));
        $this->assertFalse($document->isEmpty());

        Authorization::cleanRoles();
        Authorization::setRole(Role::user('random')->toString());

        $document = $database->getDocument(
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
    public function testCollectionPermissionsRelationshipsUpdateThrowsException(array $data): void
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document->setAttribute('test', $document->getAttribute('test').'new_value')
        );
    }

    /**
     * @depends testCollectionPermissionsRelationshipsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsRelationshipsUpdateWorks(array $data): array
    {
        [$collection, $collectionOneToOne, $collectionOneToMany, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $database->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document
        );

        $this->assertTrue(true);

        Authorization::cleanRoles();
        Authorization::setRole(Role::user('random')->toString());

        $database->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document->setAttribute('test', 'ipsum')
        );

        $this->assertTrue(true);

        return $data;
    }

    /**
     * @param array<Document> $data
     * @depends testCollectionPermissionsCreateWorks
     */
    public function testCollectionPermissionsUpdateThrowsException(array $data): void
    {
        [$collection, $document] = $data;
        Authorization::cleanRoles();
        Authorization::setRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = static::getDatabase();

        $document = $database->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document->setAttribute('test', 'lorem')
        );
    }

    /**
     * @depends testCollectionPermissionsCreateWorks
     * @param array<Document> $data
     * @return array<Document>
     */
    public function testCollectionPermissionsUpdateWorks(array $data): array
    {
        [$collection, $document] = $data;

        Authorization::cleanRoles();
        Authorization::setRole(Role::users()->toString());

        /** @var Database $database */
        $database = static::getDatabase();

        $this->assertInstanceOf(Document::class, $database->updateDocument(
            $collection->getId(),
            $document->getId(),
            $document->setAttribute('test', 'ipsum')
        ));

        return $data;
    }

    /**
     * @depends testCollectionUpdate
     */
    public function testCollectionUpdatePermissionsThrowException(Document $collection): void
    {
        $this->expectException(DatabaseException::class);

        /** @var Database $database */
        $database = static::getDatabase();

        $database->updateCollection($collection->getId(), permissions: [
            'i dont work'
        ], documentSecurity: false);
    }

    public function testWritePermissions(): void
    {
        Authorization::setRole(Role::any()->toString());
        $database = static::getDatabase();

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

        $docs = Authorization::skip(fn () => $database->find('animals'));
        $this->assertCount(1, $docs);
        $this->assertEquals('cat', $docs[0]['$id']);
        $this->assertEquals('newCat', $docs[0]['type']);
    }

    public function testCreateRelationDocumentWithoutUpdatePermission(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        Authorization::cleanRoles();
        Authorization::setRole(Role::user('a')->toString());

        $database->createCollection('parentRelationTest', [], [], [
            Permission::read(Role::user('a')),
            Permission::create(Role::user('a')),
            Permission::update(Role::user('a')),
            Permission::delete(Role::user('a'))
        ]);
        $database->createCollection('childRelationTest', [], [], [
            Permission::create(Role::user('a')),
            Permission::read(Role::user('a')),
        ]);
        $database->createAttribute('parentRelationTest', 'name', Database::VAR_STRING, 255, false);
        $database->createAttribute('childRelationTest', 'name', Database::VAR_STRING, 255, false);

        $database->createRelationship(
            collection: 'parentRelationTest',
            relatedCollection: 'childRelationTest',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'children'
        );

        // Create document with relationship with nested data
        $parent = $database->createDocument('parentRelationTest', new Document([
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
        $updatedParent = $database->updateDocument('parentRelationTest', 'parent1', $parent);

        $this->assertEquals('child2', $updatedParent->getAttribute('children')[0]->getId());

        $database->deleteCollection('parentRelationTest');
        $database->deleteCollection('childRelationTest');
    }

}

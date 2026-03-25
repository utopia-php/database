<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;

trait PermissionTests
{
    private static string $collSecurityCollection = '';

    private static string $collSecurityParentCollection = '';

    private static string $collSecurityOneToOneCollection = '';

    private static string $collSecurityOneToManyCollection = '';

    private static string $collUpdateCollection = '';

    protected function getCollSecurityCollection(): string
    {
        if (self::$collSecurityCollection === '') {
            self::$collSecurityCollection = 'collectionSecurity_' . uniqid();
        }
        return self::$collSecurityCollection;
    }

    protected function getCollSecurityParentCollection(): string
    {
        if (self::$collSecurityParentCollection === '') {
            self::$collSecurityParentCollection = 'csParent_' . uniqid();
        }
        return self::$collSecurityParentCollection;
    }

    protected function getCollSecurityOneToOneCollection(): string
    {
        if (self::$collSecurityOneToOneCollection === '') {
            self::$collSecurityOneToOneCollection = 'csO2O_' . uniqid();
        }
        return self::$collSecurityOneToOneCollection;
    }

    protected function getCollSecurityOneToManyCollection(): string
    {
        if (self::$collSecurityOneToManyCollection === '') {
            self::$collSecurityOneToManyCollection = 'csO2M_' . uniqid();
        }
        return self::$collSecurityOneToManyCollection;
    }

    protected function getCollUpdateCollection(): string
    {
        if (self::$collUpdateCollection === '') {
            self::$collUpdateCollection = 'collectionUpdate_' . uniqid();
        }
        return self::$collUpdateCollection;
    }

    private static bool $collPermFixtureInit = false;

    /** @var array{collectionId: string, docId: string}|null */
    private static ?array $collPermFixtureData = null;

    private static bool $relPermFixtureInit = false;

    /** @var array{collectionId: string, oneToOneId: string, oneToManyId: string, docId: string}|null */
    private static ?array $relPermFixtureData = null;

    private static bool $collUpdateFixtureInit = false;

    /** @var array{collectionId: string}|null */
    private static ?array $collUpdateFixtureData = null;

    /**
     * Create the $this->getCollSecurityCollection() collection with a document.
     * Combines the setup from testCollectionPermissions + testCollectionPermissionsCreateWorks.
     *
     * @return array{collectionId: string, docId: string}
     */
    protected function initCollectionPermissionFixture(): array
    {
        if (self::$collPermFixtureInit && self::$collPermFixtureData !== null) {
            /** @var Database $database */
            $database = $this->getDatabase();
            $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());
            $doc = $database->getDocument(self::$collPermFixtureData['collectionId'], self::$collPermFixtureData['docId']);
            if (!$doc->isEmpty()) {
                return self::$collPermFixtureData;
            }
            self::$collPermFixtureInit = false;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->deleteCollection($this->getCollSecurityCollection());
        } catch (\Throwable) {
        }

        $collection = $database->createCollection($this->getCollSecurityCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: false);

        $database->createAttribute($collection->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false));

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        $document = $database->createDocument($collection->getId(), new Document([
            '$id' => \Utopia\Database\Helpers\ID::unique(),
            '$permissions' => [
                Permission::read(Role::user('random')),
                Permission::update(Role::user('random')),
                Permission::delete(Role::user('random')),
            ],
            'test' => 'lorem',
        ]));

        self::$collPermFixtureInit = true;
        self::$collPermFixtureData = [
            'collectionId' => $collection->getId(),
            'docId' => $document->getId(),
        ];

        return self::$collPermFixtureData;
    }

    /**
     * Create the relationship permission test collections with a document.
     * Combines testCollectionPermissionsRelationships + testCollectionPermissionsRelationshipsCreateWorks.
     *
     * @return array{collectionId: string, oneToOneId: string, oneToManyId: string, docId: string}
     */
    protected function initRelationshipPermissionFixture(): array
    {
        if (self::$relPermFixtureInit && self::$relPermFixtureData !== null) {
            /** @var Database $database */
            $database = $this->getDatabase();
            $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());
            $doc = $database->getDocument(self::$relPermFixtureData['collectionId'], self::$relPermFixtureData['docId']);
            if (!$doc->isEmpty()) {
                return self::$relPermFixtureData;
            }
            self::$relPermFixtureInit = false;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        foreach ([$this->getCollSecurityParentCollection(), $this->getCollSecurityOneToOneCollection(), $this->getCollSecurityOneToManyCollection()] as $col) {
            try {
                $database->deleteCollection($col);
            } catch (\Throwable) {
            }
        }

        $collection = $database->createCollection($this->getCollSecurityParentCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $database->createAttribute($collection->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false));

        $collectionOneToOne = $database->createCollection($this->getCollSecurityOneToOneCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $database->createAttribute($collectionOneToOne->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false));

        $database->createRelationship(new Relationship(collection: $collection->getId(), relatedCollection: $collectionOneToOne->getId(), type: RelationType::OneToOne, key: RelationType::OneToOne->value, onDelete: ForeignKeyAction::Cascade));

        $collectionOneToMany = $database->createCollection($this->getCollSecurityOneToManyCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $database->createAttribute($collectionOneToMany->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false));

        $database->createRelationship(new Relationship(collection: $collection->getId(), relatedCollection: $collectionOneToMany->getId(), type: RelationType::OneToMany, key: RelationType::OneToMany->value, onDelete: ForeignKeyAction::Cascade));

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        $document = $database->createDocument($collection->getId(), new Document([
            '$id' => \Utopia\Database\Helpers\ID::unique(),
            '$permissions' => [
                Permission::read(Role::user('random')),
                Permission::update(Role::user('random')),
                Permission::delete(Role::user('random')),
            ],
            'test' => 'lorem',
            RelationType::OneToOne->value => [
                '$id' => \Utopia\Database\Helpers\ID::unique(),
                '$permissions' => [
                    Permission::read(Role::user('random')),
                    Permission::update(Role::user('random')),
                    Permission::delete(Role::user('random')),
                ],
                'test' => 'lorem ipsum',
            ],
            RelationType::OneToMany->value => [
                [
                    '$id' => \Utopia\Database\Helpers\ID::unique(),
                    '$permissions' => [
                        Permission::read(Role::user('random')),
                        Permission::update(Role::user('random')),
                        Permission::delete(Role::user('random')),
                    ],
                    'test' => 'lorem ipsum',
                ], [
                    '$id' => \Utopia\Database\Helpers\ID::unique(),
                    '$permissions' => [
                        Permission::read(Role::user('torsten')),
                        Permission::update(Role::user('random')),
                        Permission::delete(Role::user('random')),
                    ],
                    'test' => 'dolor',
                ],
            ],
        ]));

        self::$relPermFixtureInit = true;
        self::$relPermFixtureData = [
            'collectionId' => $collection->getId(),
            'oneToOneId' => $collectionOneToOne->getId(),
            'oneToManyId' => $collectionOneToMany->getId(),
            'docId' => $document->getId(),
        ];

        return self::$relPermFixtureData;
    }

    /**
     * Create the $this->getCollUpdateCollection() collection.
     * Replicates the setup from testCollectionUpdate in CollectionTests.
     *
     * @return array{collectionId: string}
     */
    protected function initCollectionUpdateFixture(): array
    {
        if (self::$collUpdateFixtureInit && self::$collUpdateFixtureData !== null) {
            return self::$collUpdateFixtureData;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->deleteCollection($this->getCollUpdateCollection());
        } catch (\Throwable) {
        }

        $collection = $database->createCollection($this->getCollUpdateCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: false);

        $database->updateCollection($this->getCollUpdateCollection(), [], true);

        self::$collUpdateFixtureInit = true;
        self::$collUpdateFixtureData = [
            'collectionId' => $collection->getId(),
        ];

        return self::$collUpdateFixtureData;
    }

    public function testCollectionPermissionsRelationships(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = $database->createCollection($this->getCollSecurityParentCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collection);

        $this->assertTrue($database->createAttribute($collection->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false)));

        $collectionOneToOne = $database->createCollection($this->getCollSecurityOneToOneCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collectionOneToOne);

        $this->assertTrue($database->createAttribute($collectionOneToOne->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false)));

        $this->assertTrue($database->createRelationship(new Relationship(collection: $collection->getId(), relatedCollection: $collectionOneToOne->getId(), type: RelationType::OneToOne, key: RelationType::OneToOne->value, onDelete: ForeignKeyAction::Cascade)));

        $collectionOneToMany = $database->createCollection($this->getCollSecurityOneToManyCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collectionOneToMany);

        $this->assertTrue($database->createAttribute($collectionOneToMany->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false)));

        $this->assertTrue($database->createRelationship(new Relationship(collection: $collection->getId(), relatedCollection: $collectionOneToMany->getId(), type: RelationType::OneToMany, key: RelationType::OneToMany->value, onDelete: ForeignKeyAction::Cascade)));
    }

    public function testUnsetPermissions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(__FUNCTION__);
        $this->assertTrue($database->createAttribute(__FUNCTION__, new Attribute(key: 'president', type: ColumnType::String, size: 255, required: false)));

        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $documents = [];

        for ($i = 0; $i < 3; $i++) {
            $documents[] = new Document([
                '$permissions' => $permissions,
                'president' => 'Donald Trump'
            ]);
        }

        $results = [];
        $count = $database->createDocuments(__FUNCTION__, $documents, onNext: function ($doc) use (&$results) {
            $results[] = $doc;
        });

        $this->assertEquals(3, $count);

        foreach ($results as $result) {
            $this->assertEquals('Donald Trump', $result->getAttribute('president'));
            $this->assertEquals($permissions, $result->getPermissions());
        }

        /**
         * No permissions passed, Check old is preserved
         */
        $updates = new Document([
            'president' => 'George Washington'
        ]);

        $results = [];
        $modified = $database->updateDocuments(
            __FUNCTION__,
            $updates,
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );

        $this->assertEquals(3, $modified);

        foreach ($results as $result) {
            $this->assertEquals('George Washington', $result->getAttribute('president'));
            $this->assertEquals($permissions, $result->getPermissions());
        }

        $documents = $database->find(__FUNCTION__);

        $this->assertEquals(3, count($documents));

        foreach ($documents as $document) {
            $this->assertEquals('George Washington', $document->getAttribute('president'));
            $this->assertEquals($permissions, $document->getPermissions());
        }

        /**
         * Change permissions remove delete
         */
        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ];

        $updates = new Document([
            '$permissions' => $permissions,
            'president' => 'Joe biden'
        ]);

        $results = [];
        $modified = $database->updateDocuments(
            __FUNCTION__,
            $updates,
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );

        $this->assertEquals(3, $modified);

        foreach ($results as $result) {
            $this->assertEquals('Joe biden', $result->getAttribute('president'));
            $this->assertEquals($permissions, $result->getPermissions());
            $this->assertArrayNotHasKey('$skipPermissionsUpdate', $result);
        }

        $documents = $database->find(__FUNCTION__);

        $this->assertEquals(3, count($documents));

        foreach ($documents as $document) {
            $this->assertEquals('Joe biden', $document->getAttribute('president'));
            $this->assertEquals($permissions, $document->getPermissions());
        }

        /**
         * Unset permissions
         */
        $updates = new Document([
            '$permissions' => [],
            'president' => 'Richard Nixon'
        ]);

        $results = [];
        $modified = $database->updateDocuments(
            __FUNCTION__,
            $updates,
            onNext: function ($doc) use (&$results) {
                $results[] = $doc;
            }
        );

        $this->assertEquals(3, $modified);

        foreach ($results as $result) {
            $this->assertEquals('Richard Nixon', $result->getAttribute('president'));
            $this->assertEquals([], $result->getPermissions());
        }

        $documents = $database->find(__FUNCTION__);
        $this->assertEquals(0, count($documents));

        $this->getDatabase()->getAuthorization()->disable();
        $documents = $database->find(__FUNCTION__);
        $this->getDatabase()->getAuthorization()->reset();

        $this->assertEquals(3, count($documents));

        foreach ($documents as $document) {
            $this->assertEquals('Richard Nixon', $document->getAttribute('president'));
            $this->assertEquals([], $document->getPermissions());
            $this->assertArrayNotHasKey('$skipPermissionsUpdate', $document);
        }
    }

    public function testCreateDocumentsEmptyPermission(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

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
    }

    public function testReadPermissionsFailure(): void
    {
        $this->initDocumentsFixture();
        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->createDocument($this->getDocumentsCollection(), new Document([
            '$permissions' => [
                Permission::read(Role::user('1')),
                Permission::create(Role::user('1')),
                Permission::update(Role::user('1')),
                Permission::delete(Role::user('1')),
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

        $document = $database->getDocument($document->getCollection(), $document->getId());

        $this->assertEquals(true, $document->isEmpty());

        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
    }

    public function testNoChangeUpdateDocumentWithoutPermission(): void
    {
        $this->initDocumentsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->createDocument($this->getDocumentsCollection(), new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any())
            ],
            'string' => 'text📝',
            'integer_signed' => -Database::MAX_INT,
            'integer_unsigned' => Database::MAX_INT,
            'bigint_signed' => -Database::MAX_BIG_INT,
            'bigint_unsigned' => Database::MAX_BIG_INT,
            'float_signed' => -123456789.12346,
            'float_unsigned' => 123456789.12346,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        $updatedDocument = $database->updateDocument(
            $this->getDocumentsCollection(),
            $document->getId(),
            $document
        );

        // Document should not be updated as there is no change.
        // It should also not throw any authorization exception without any permission because of no change.
        $this->assertEquals($updatedDocument->getUpdatedAt(), $document->getUpdatedAt());

        $document = $database->createDocument($this->getDocumentsCollection(), new Document([
            '$id' => ID::unique(),
            '$permissions' => [],
            'string' => 'text📝',
            'integer_signed' => -Database::MAX_INT,
            'integer_unsigned' => Database::MAX_INT,
            'bigint_signed' => -Database::MAX_BIG_INT,
            'bigint_unsigned' => Database::MAX_BIG_INT,
            'float_signed' => -123456789.12346,
            'float_unsigned' => 123456789.12346,
            'boolean' => true,
            'colors' => ['pink', 'green', 'blue'],
        ]));

        // Should throw exception, because nothing was updated, but there was no read permission
        try {
            $database->updateDocument(
                $this->getDocumentsCollection(),
                $document->getId(),
                $document
            );
        } catch (Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }
    }

    public function testUpdateDocumentsPermissions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (!$database->getAdapter()->supports(Capability::BatchOperations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = 'testUpdateDocumentsPerms';

        $database->createCollection($collection, attributes: [
            new Document([
                '$id' => ID::custom('string'),
                'type' => ColumnType::String->value,
                'size' => 767,
                'required' => true,
            ])
        ], permissions: [], documentSecurity: true);

        // Test we can bulk update permissions we have access to
        $this->getDatabase()->getAuthorization()->skip(function () use ($collection, $database) {
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
        $database = $this->getDatabase();

        $documents = $this->getDatabase()->getAuthorization()->skip(function () use ($collection, $database) {
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

        $this->getDatabase()->getAuthorization()->addRole(Role::user('user2')->toString());

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

        $documents = $this->getDatabase()->getAuthorization()->skip(function () use ($collection) {
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

    public function testCollectionPermissions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = $database->createCollection($this->getCollSecurityCollection(), permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users())
        ], documentSecurity: false);

        $this->assertInstanceOf(Document::class, $collection);

        $this->assertTrue($database->createAttribute($collection->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false)));
    }

    public function testCollectionPermissionsCountThrowsException(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->count($collectionId);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $th) {
            $this->assertInstanceOf(AuthorizationException::class, $th);
        }
    }

    public function testCollectionPermissionsCountWorks(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $count = $database->count(
            $collectionId
        );

        $this->assertNotEmpty($count);
    }
    public function testCollectionPermissionsCreateThrowsException(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];
        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createDocument($collectionId, new Document([
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
     * @return array<Document>
     */
    public function testCollectionPermissionsCreateWorks(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];
        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->createDocument($collectionId, new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::user('random')),
                Permission::update(Role::user('random')),
                Permission::delete(Role::user('random'))
            ],
            'test' => 'lorem'
        ]));
        $this->assertInstanceOf(Document::class, $document);

        $database->deleteDocument($collectionId, $document->getId());
    }

    public function testCollectionPermissionsDeleteThrowsException(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->deleteDocument(
            $collectionId,
            $docId
        );
    }

    public function testCollectionPermissionsDeleteWorks(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $this->assertTrue($database->deleteDocument(
            $collectionId,
            $docId
        ));
    }

    public function testCollectionPermissionsExceptions(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $this->expectException(DatabaseException::class);
        $database->createCollection($this->getCollSecurityCollection(), permissions: [
            'i dont work'
        ]);
    }

    public function testCollectionPermissionsFindThrowsException(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->find($collectionId);
    }

    public function testCollectionPermissionsFindWorks(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $documents = $database->find($collectionId);
        $this->assertNotEmpty($documents);

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::user('random')->toString());

        try {
            $database->find($collectionId);
            $this->fail('Failed to throw exception');
        } catch (AuthorizationException) {
        }
    }

    public function testCollectionPermissionsGetThrowsException(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->getDocument(
            $collectionId,
            $docId,
        );
        $this->assertInstanceOf(Document::class, $document);
        $this->assertTrue($document->isEmpty());
    }

    public function testCollectionPermissionsGetWorks(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->getDocument(
            $collectionId,
            $docId
        );
        $this->assertInstanceOf(Document::class, $document);
        $this->assertFalse($document->isEmpty());
    }

    public function testCollectionPermissionsRelationshipsCountWorks(): void
    {
        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $documents = $database->count(
            $collectionId
        );

        $this->assertEquals(1, $documents);

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::user('random')->toString());

        $documents = $database->count(
            $collectionId
        );

        $this->assertEquals(1, $documents);

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::user('unknown')->toString());

        $documents = $database->count(
            $collectionId
        );

        $this->assertEquals(0, $documents);
    }

    public function testCollectionPermissionsRelationshipsCreateThrowsException(): void
    {
        if (! ($this->getDatabase()->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createDocument($collectionId, new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ],
            'test' => 'lorem ipsum'
        ]));
    }

    public function testCollectionPermissionsRelationshipsDeleteThrowsException(): void
    {
        if (! ($this->getDatabase()->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        $this->expectException(AuthorizationException::class);

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->deleteDocument(
            $collectionId,
            $docId
        );
    }

    public function testCollectionPermissionsRelationshipsCreateWorks(): void
    {
        if (! ($this->getDatabase()->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->createDocument($collectionId, new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::user('random')),
                Permission::update(Role::user('random')),
                Permission::delete(Role::user('random'))
            ],
            'test' => 'lorem',
            RelationType::OneToOne->value => [
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::user('random')),
                    Permission::update(Role::user('random')),
                    Permission::delete(Role::user('random'))
                ],
                'test' => 'lorem ipsum'
            ],
            RelationType::OneToMany->value => [
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

        $database->deleteDocument($collectionId, $document->getId());
    }

    public function testCollectionPermissionsRelationshipsDeleteWorks(): void
    {
        if (! ($this->getDatabase()->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $this->assertTrue($database->deleteDocument(
            $collectionId,
            $docId
        ));
    }

    public function testCollectionPermissionsRelationshipsFindWorks(): void
    {
        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        if (!($database->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $documents = $database->find(
            $collectionId
        );

        $this->assertIsArray($documents);
        $this->assertCount(1, $documents);
        $document = $documents[0];
        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(RelationType::OneToOne->value));
        $this->assertIsArray($document->getAttribute(RelationType::OneToMany->value));
        $this->assertCount(2, $document->getAttribute(RelationType::OneToMany->value));
        $this->assertFalse($document->isEmpty());

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::user('random')->toString());

        $documents = $database->find(
            $collectionId
        );

        $this->assertIsArray($documents);
        $this->assertCount(1, $documents);
        $document = $documents[0];
        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(RelationType::OneToOne->value));
        $this->assertIsArray($document->getAttribute(RelationType::OneToMany->value));
        $this->assertCount(1, $document->getAttribute(RelationType::OneToMany->value));
        $this->assertFalse($document->isEmpty());

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::user('unknown')->toString());

        $documents = $database->find(
            $collectionId
        );

        $this->assertIsArray($documents);
        $this->assertCount(0, $documents);
    }

    public function testCollectionPermissionsRelationshipsGetThrowsException(): void
    {
        if (! ($this->getDatabase()->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->getDocument(
            $collectionId,
            $docId,
        );
        $this->assertInstanceOf(Document::class, $document);
        $this->assertTrue($document->isEmpty());
    }

    public function testCollectionPermissionsRelationshipsGetWorks(): void
    {
        if (! ($this->getDatabase()->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->getDocument(
            $collectionId,
            $docId
        );

        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(RelationType::OneToOne->value));
        $this->assertIsArray($document->getAttribute(RelationType::OneToMany->value));
        $this->assertCount(2, $document->getAttribute(RelationType::OneToMany->value));
        $this->assertFalse($document->isEmpty());

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::user('random')->toString());

        $document = $database->getDocument(
            $collectionId,
            $docId
        );

        $this->assertInstanceOf(Document::class, $document);
        $this->assertInstanceOf(Document::class, $document->getAttribute(RelationType::OneToOne->value));
        $this->assertIsArray($document->getAttribute(RelationType::OneToMany->value));
        $this->assertCount(1, $document->getAttribute(RelationType::OneToMany->value));
        $this->assertFalse($document->isEmpty());
    }

    public function testCollectionPermissionsRelationshipsUpdateThrowsException(): void
    {
        if (! ($this->getDatabase()->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        /** @var Database $database */
        $database = $this->getDatabase();

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());
        $document = $database->getDocument($collectionId, $docId);

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
        $this->expectException(AuthorizationException::class);

        $database->updateDocument(
            $collectionId,
            $docId,
            $document->setAttribute('test', $document->getAttribute('test').'new_value')
        );
    }

    public function testCollectionPermissionsRelationshipsUpdateWorks(): void
    {
        $data = $this->initRelationshipPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->getDocument($collectionId, $docId);

        $database->updateDocument(
            $collectionId,
            $docId,
            $document
        );

        $this->assertTrue(true);

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::user('random')->toString());

        $database->updateDocument(
            $collectionId,
            $docId,
            $document->setAttribute('test', 'ipsum')
        );

        $this->assertTrue(true);
    }

    public function testCollectionPermissionsUpdateThrowsException(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        /** @var Database $database */
        $database = $this->getDatabase();

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());
        $document = $database->getDocument($collectionId, $docId);

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
        $this->expectException(AuthorizationException::class);

        $database->updateDocument(
            $collectionId,
            $docId,
            $document->setAttribute('test', 'changed_value')
        );
    }

    public function testCollectionPermissionsUpdateWorks(): void
    {
        $data = $this->initCollectionPermissionFixture();
        $collectionId = $data['collectionId'];
        $docId = $data['docId'];

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::users()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $document = $database->getDocument($collectionId, $docId);

        $this->assertInstanceOf(Document::class, $database->updateDocument(
            $collectionId,
            $docId,
            $document->setAttribute('test', 'ipsum')
        ));
    }
    public function testCollectionUpdatePermissionsThrowException(): void
    {
        $data = $this->initCollectionUpdateFixture();
        $collectionId = $data['collectionId'];
        $this->expectException(DatabaseException::class);

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->updateCollection($collectionId, permissions: [
            'i dont work'
        ], documentSecurity: false);
    }

    public function testWritePermissions(): void
    {
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());
        $database = $this->getDatabase();

        $database->createCollection('animals', permissions: [
            Permission::create(Role::any()),
        ], documentSecurity: true);

        $database->createAttribute('animals', new Attribute(key: 'type', type: ColumnType::String, size: 128, required: true));

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

        $docs = $this->getDatabase()->getAuthorization()->skip(fn () => $database->find('animals'));
        $this->assertCount(1, $docs);
        $this->assertEquals('cat', $docs[0]['$id']);
        $this->assertEquals('newCat', $docs[0]['type']);
    }

    public function testCreateRelationDocumentWithoutUpdatePermission(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (!($database->getAdapter() instanceof Feature\Relationships)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->getAuthorization()->cleanRoles();
        $this->getDatabase()->getAuthorization()->addRole(Role::user('a')->toString());

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
        $database->createAttribute('parentRelationTest', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: false));
        $database->createAttribute('childRelationTest', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: false));

        $database->createRelationship(new Relationship(collection: 'parentRelationTest', relatedCollection: 'childRelationTest', type: RelationType::OneToMany, key: 'children'));

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

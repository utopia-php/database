<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;

trait PermissionTests
{
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
     * Create the 'collectionSecurity' collection with a document.
     * Combines the setup from testCollectionPermissions + testCollectionPermissionsCreateWorks.
     *
     * @return array{collectionId: string, docId: string}
     */
    protected function initCollectionPermissionFixture(): array
    {
        if (self::$collPermFixtureInit && self::$collPermFixtureData !== null) {
            return self::$collPermFixtureData;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        try {
            $database->deleteCollection('collectionSecurity');
        } catch (\Throwable) {
        }

        $collection = $database->createCollection('collectionSecurity', permissions: [
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
            return self::$relPermFixtureData;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        foreach (['collectionSecurity.Parent', 'collectionSecurity.OneToOne', 'collectionSecurity.OneToMany'] as $col) {
            try {
                $database->deleteCollection($col);
            } catch (\Throwable) {
            }
        }

        $collection = $database->createCollection('collectionSecurity.Parent', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $database->createAttribute($collection->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false));

        $collectionOneToOne = $database->createCollection('collectionSecurity.OneToOne', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $database->createAttribute($collectionOneToOne->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false));

        $database->createRelationship(new Relationship(collection: $collection->getId(), relatedCollection: $collectionOneToOne->getId(), type: RelationType::OneToOne, key: RelationType::OneToOne->value, onDelete: ForeignKeyAction::Cascade));

        $collectionOneToMany = $database->createCollection('collectionSecurity.OneToMany', permissions: [
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
     * Create the 'collectionUpdate' collection.
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
            $database->deleteCollection('collectionUpdate');
        } catch (\Throwable) {
        }

        $collection = $database->createCollection('collectionUpdate', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: false);

        $database->updateCollection('collectionUpdate', [], true);

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

        $collection = $database->createCollection('collectionSecurity.Parent', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collection);

        $this->assertTrue($database->createAttribute($collection->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false)));

        $collectionOneToOne = $database->createCollection('collectionSecurity.OneToOne', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collectionOneToOne);

        $this->assertTrue($database->createAttribute($collectionOneToOne->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false)));

        $this->assertTrue($database->createRelationship(new Relationship(collection: $collection->getId(), relatedCollection: $collectionOneToOne->getId(), type: RelationType::OneToOne, key: RelationType::OneToOne->value, onDelete: ForeignKeyAction::Cascade)));

        $collectionOneToMany = $database->createCollection('collectionSecurity.OneToMany', permissions: [
            Permission::create(Role::users()),
            Permission::read(Role::users()),
            Permission::update(Role::users()),
            Permission::delete(Role::users()),
        ], documentSecurity: true);

        $this->assertInstanceOf(Document::class, $collectionOneToMany);

        $this->assertTrue($database->createAttribute($collectionOneToMany->getId(), new Attribute(key: 'test', type: ColumnType::String, size: 255, required: false)));

        $this->assertTrue($database->createRelationship(new Relationship(collection: $collection->getId(), relatedCollection: $collectionOneToMany->getId(), type: RelationType::OneToMany, key: RelationType::OneToMany->value, onDelete: ForeignKeyAction::Cascade)));
    }
}

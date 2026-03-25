<?php

namespace Tests\Unit\Relationships;

use DateTime;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\RelationshipHandler;
use Utopia\Database\Operator;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;

/** @internal */
abstract class RelationshipsAdapter extends Adapter implements Feature\Relationships
{
}

class RelationshipValidationTest extends TestCase
{
    private function metaCollection(): Document
    {
        return new Document([
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
            '$version' => 1,
            'name' => 'collections',
            'attributes' => [
                new Document(['$id' => 'name', 'key' => 'name', 'type' => 'string', 'size' => 256, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
                new Document(['$id' => 'attributes', 'key' => 'attributes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
                new Document(['$id' => 'indexes', 'key' => 'indexes', 'type' => 'string', 'size' => 1000000, 'required' => false, 'signed' => true, 'array' => false, 'filters' => ['json']]),
                new Document(['$id' => 'documentSecurity', 'key' => 'documentSecurity', 'type' => 'boolean', 'size' => 0, 'required' => true, 'signed' => true, 'array' => false, 'filters' => []]),
            ],
            'indexes' => [],
            'documentSecurity' => true,
        ]);
    }

    private function makeCollection(string $id, array $attributes = [], array $permissions = []): Document
    {
        if (empty($permissions)) {
            $permissions = [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ];
        }

        return new Document([
            '$id' => $id,
            '$sequence' => $id,
            '$collection' => Database::METADATA,
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => $permissions,
            '$version' => 1,
            'name' => $id,
            'attributes' => $attributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);
    }

    /**
     * @param array<Document> $collections
     * @param array<string, Document> $documents keyed by "collectionId:docId"
     */
    private function buildDatabase(array $collections, array $documents = [], bool $withRelationshipHook = false): Database
    {
        $adapter = self::createStub(RelationshipsAdapter::class);
        $adapter->method('getSharedTables')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getTenantPerDocument')->willReturn(false);
        $adapter->method('getNamespace')->willReturn('');
        $adapter->method('getIdAttributeType')->willReturn('string');
        $adapter->method('getMaxUIDLength')->willReturn(36);
        $adapter->method('getMinDateTime')->willReturn(new DateTime('0000-01-01'));
        $adapter->method('getMaxDateTime')->willReturn(new DateTime('9999-12-31'));
        $adapter->method('getLimitForString')->willReturn(16777215);
        $adapter->method('getLimitForInt')->willReturn(2147483647);
        $adapter->method('getLimitForAttributes')->willReturn(0);
        $adapter->method('getLimitForIndexes')->willReturn(64);
        $adapter->method('getMaxIndexLength')->willReturn(768);
        $adapter->method('getMaxVarcharLength')->willReturn(16383);
        $adapter->method('getDocumentSizeLimit')->willReturn(0);
        $adapter->method('getCountOfAttributes')->willReturn(0);
        $adapter->method('getCountOfIndexes')->willReturn(0);
        $adapter->method('getAttributeWidth')->willReturn(0);
        $adapter->method('getInternalIndexesKeys')->willReturn([]);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            return in_array($cap, [
                Capability::Index,
                Capability::IndexArray,
                Capability::UniqueIndex,
                Capability::DefinedAttributes,
                Capability::Operators,
            ]);
        });
        $adapter->method('castingBefore')->willReturnArgument(1);
        $adapter->method('castingAfter')->willReturnArgument(1);
        $adapter->method('startTransaction')->willReturn(true);
        $adapter->method('commitTransaction')->willReturn(true);
        $adapter->method('rollbackTransaction')->willReturn(true);
        $adapter->method('withTransaction')->willReturnCallback(function (callable $callback) {
            return $callback();
        });
        $adapter->method('createDocument')->willReturnArgument(1);
        $adapter->method('updateDocument')->willReturnArgument(2);
        $adapter->method('createRelationship')->willReturn(true);
        $adapter->method('deleteRelationship')->willReturn(true);
        $adapter->method('updateRelationship')->willReturn(true);
        $adapter->method('createIndex')->willReturn(true);
        $adapter->method('deleteIndex')->willReturn(true);
        $adapter->method('renameIndex')->willReturn(true);
        $adapter->method('getSequences')->willReturnArgument(1);

        $meta = $this->metaCollection();
        $colMap = [];
        foreach ($collections as $col) {
            $colMap[$col->getId()] = $col;
        }

        $adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($meta, $colMap, $documents) {
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return $meta;
                }
                if ($col->getId() === Database::METADATA && isset($colMap[$docId])) {
                    return $colMap[$docId];
                }
                $key = $col->getId() . ':' . $docId;
                if (isset($documents[$key])) {
                    return $documents[$key];
                }

                return new Document();
            }
        );

        $cache = new Cache(new None());
        $database = new Database($adapter, $cache);
        $database->getAuthorization()->addRole(Role::any()->toString());

        if ($withRelationshipHook) {
            $database->setRelationshipHook(new RelationshipHandler($database));
        }

        return $database;
    }

    public function testStructureValidationAfterRelationsAttribute(): void
    {
        $relAttr = new Document([
            '$id' => 'structure_2', 'key' => 'structure_2',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'structure_2',
                'relationType' => RelationType::OneToOne,
                'twoWay' => false,
                'twoWayKey' => 'structure_1',
                'onDelete' => 'restrict',
                'side' => 'parent',
            ],
        ]);

        $db = $this->buildDatabase([
            $this->makeCollection('structure_1', [$relAttr]),
            $this->makeCollection('structure_2'),
        ]);

        $this->expectException(StructureException::class);

        $db->createDocument('structure_1', new Document([
            '$permissions' => [Permission::read(Role::any())],
            'structure_2' => '100',
            'name' => 'Frozen',
        ]));
    }

    public function testNoChangeUpdateDocumentWithRelationWithoutPermission(): void
    {
        $nameAttr = new Document([
            '$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value,
            'size' => 100, 'required' => false, 'default' => null,
            'signed' => false, 'array' => false, 'filters' => [],
        ]);

        $perms = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::delete(Role::any()),
        ];

        $doc = new Document([
            '$id' => 'level1',
            '$collection' => 'level1',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [],
            'name' => 'Level 1',
        ]);

        $db = $this->buildDatabase(
            [$this->makeCollection('level1', [$nameAttr], $perms)],
            ['level1:level1' => $doc]
        );

        $created = $db->createDocument('level1', new Document([
            '$id' => 'level1',
            '$permissions' => [],
            'name' => 'Level 1',
        ]));

        $this->expectException(AuthorizationException::class);

        $db->updateDocument('level1', 'level1', $created->setAttribute('name', 'haha'));
    }

    public function testNoInvalidKeysWithRelationships(): void
    {
        $nameAttr = new Document([
            '$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $speciesRelAttr = new Document([
            '$id' => 'creature', 'key' => 'creature',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'creatures',
                'relationType' => RelationType::OneToOne,
                'twoWay' => true,
                'twoWayKey' => 'species',
                'onDelete' => 'restrict',
                'side' => 'parent',
            ],
        ]);

        $db = $this->buildDatabase([
            $this->makeCollection('species', [$nameAttr, $speciesRelAttr]),
            $this->makeCollection('creatures', [$nameAttr]),
            $this->makeCollection('characteristics', [$nameAttr]),
        ]);

        $doc = $db->createDocument('species', new Document([
            '$id' => ID::custom('1'),
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Canine',
            'creature' => null,
        ]));

        $this->assertEquals('1', $doc->getId());
    }

    public function testEnforceRelationshipPermissions(): void
    {
        $nameAttr = new Document([
            '$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);

        $perms = [
            Permission::read(Role::any()),
            Permission::update(Role::user('user1')),
            Permission::delete(Role::user('user2')),
        ];

        $doc = new Document([
            '$id' => 'lawn1',
            '$collection' => 'lawns',
            '$sequence' => '1',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => $perms,
            'name' => 'Lawn 1',
        ]);

        $colPerms = [Permission::create(Role::any()), Permission::read(Role::any())];

        $db = $this->buildDatabase(
            [$this->makeCollection('lawns', [$nameAttr], $colPerms)],
            ['lawns:lawn1' => $doc]
        );

        $db->getAuthorization()->cleanRoles();
        $db->getAuthorization()->addRole(Role::any()->toString());

        try {
            $db->updateDocument('lawns', 'lawn1', new Document([
                '$permissions' => $perms,
                'name' => 'Lawn 1 Updated',
            ]));
            $this->fail('Failed to throw exception');
        } catch (\Exception $e) {
            $this->assertInstanceOf(AuthorizationException::class, $e);
        }
    }

    public function testCreateRelationshipMissingCollection(): void
    {
        $db = $this->buildDatabase([]);

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $db->createRelationship(new Relationship(
            collection: 'missing',
            relatedCollection: 'missing',
            type: RelationType::OneToMany,
            twoWay: true
        ));
    }

    public function testCreateRelationshipMissingRelatedCollection(): void
    {
        $db = $this->buildDatabase([$this->makeCollection('test')]);

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Related collection not found');

        $db->createRelationship(new Relationship(
            collection: 'test',
            relatedCollection: 'missing',
            type: RelationType::OneToMany,
            twoWay: true
        ));
    }

    public function testCreateDuplicateRelationship(): void
    {
        $relAttr = new Document([
            '$id' => 'test2', 'key' => 'test2',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'test2',
                'relationType' => RelationType::OneToMany,
                'twoWay' => true, 'twoWayKey' => 'test1',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);

        $db = $this->buildDatabase([
            $this->makeCollection('test1', [$relAttr]),
            $this->makeCollection('test2'),
        ]);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Attribute already exists');

        $db->createRelationship(new Relationship(
            collection: 'test1',
            relatedCollection: 'test2',
            type: RelationType::OneToMany,
            twoWay: true
        ));
    }

    public function testCreateInvalidRelationship(): void
    {
        $this->expectException(\TypeError::class);

        new Relationship(collection: 'test3', relatedCollection: 'test4', type: 'invalid', twoWay: true);
    }

    public function testDeleteMissingRelationship(): void
    {
        $db = $this->buildDatabase([$this->makeCollection('test')]);

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Relationship not found');

        $db->deleteRelationship('test', 'test2');
    }

    public function testCreateInvalidIntValueRelationship(): void
    {
        $relAttr = new Document([
            '$id' => 'invalid2', 'key' => 'invalid2',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'invalid2',
                'relationType' => RelationType::OneToOne,
                'twoWay' => true, 'twoWayKey' => 'invalid1',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);

        $db = $this->buildDatabase([
            $this->makeCollection('invalid1', [$relAttr]),
            $this->makeCollection('invalid2'),
        ], [], true);

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $db->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => 10,
        ]));
    }

    public function testCreateInvalidObjectValueRelationship(): void
    {
        $relAttr = new Document([
            '$id' => 'invalid2', 'key' => 'invalid2',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'invalid2',
                'relationType' => RelationType::OneToOne,
                'twoWay' => true, 'twoWayKey' => 'invalid1',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);

        $db = $this->buildDatabase([
            $this->makeCollection('invalid1', [$relAttr]),
            $this->makeCollection('invalid2'),
        ], [], true);

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $db->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid2' => new \stdClass(),
        ]));
    }

    public function testCreateInvalidArrayIntValueRelationship(): void
    {
        $relAttr = new Document([
            '$id' => 'invalid3', 'key' => 'invalid3',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'invalid2',
                'relationType' => RelationType::OneToMany,
                'twoWay' => true, 'twoWayKey' => 'invalid4',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);

        $db = $this->buildDatabase([
            $this->makeCollection('invalid1', [$relAttr]),
            $this->makeCollection('invalid2'),
        ], [], true);

        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('Invalid relationship value. Must be either a document, document ID, or an array of documents or document IDs.');

        $db->createDocument('invalid1', new Document([
            '$id' => ID::unique(),
            'invalid3' => [10],
        ]));
    }

    public function testCreateEmptyValueRelationship(): void
    {
        $o2oRel = new Document([
            '$id' => 'null2', 'key' => 'null2',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'null2',
                'relationType' => RelationType::OneToOne,
                'twoWay' => true, 'twoWayKey' => 'null1',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);

        $db = $this->buildDatabase([
            $this->makeCollection('null1', [$o2oRel]),
            $this->makeCollection('null2'),
        ], [], true);

        $doc = $db->createDocument('null1', new Document([
            '$id' => ID::unique(),
            'null2' => null,
        ]));

        $this->assertNull($doc->getAttribute('null2'));
    }

    public function testUpdateRelationshipToExistingKey(): void
    {
        $ownerAttr = new Document([
            '$id' => 'owner', 'key' => 'owner', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $cakesRelAttr = new Document([
            '$id' => 'cakes', 'key' => 'cakes',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'cakes',
                'relationType' => RelationType::OneToMany,
                'twoWay' => true, 'twoWayKey' => 'oven',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);
        $ovenRelAttr = new Document([
            '$id' => 'oven', 'key' => 'oven',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'ovens',
                'relationType' => RelationType::OneToMany,
                'twoWay' => true, 'twoWayKey' => 'cakes',
                'onDelete' => 'restrict', 'side' => 'child',
            ],
        ]);

        $db = $this->buildDatabase([
            $this->makeCollection('ovens', [$ownerAttr, $cakesRelAttr]),
            $this->makeCollection('cakes', [$ovenRelAttr]),
        ]);

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Relationship already exists');

        $db->updateRelationship('ovens', 'cakes', newKey: 'owner');
    }

    public function testOneToOneRelationshipRejectsArrayOperators(): void
    {
        $nameAttr = new Document([
            '$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $relAttr = new Document([
            '$id' => 'profile', 'key' => 'profile',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'profile_o2o',
                'relationType' => RelationType::OneToOne,
                'twoWay' => true, 'twoWayKey' => 'user',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);

        $existingDoc = new Document([
            '$id' => 'user1', '$collection' => 'user_o2o',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'User 1', 'profile' => null,
        ]);

        $db = $this->buildDatabase(
            [$this->makeCollection('user_o2o', [$nameAttr, $relAttr]), $this->makeCollection('profile_o2o')],
            ['user_o2o:user1' => $existingDoc]
        );

        $this->expectException(StructureException::class);
        $this->expectExceptionMessage('single-value relationship');

        $db->updateDocument('user_o2o', 'user1', new Document([
            'profile' => Operator::arrayAppend(['profile2']),
        ]));
    }

    public function testOneToManyRelationshipWithArrayOperators(): void
    {
        $nameAttr = new Document([
            '$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $relAttr = new Document([
            '$id' => 'articles', 'key' => 'articles',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'article',
                'relationType' => RelationType::OneToMany,
                'twoWay' => true, 'twoWayKey' => 'author',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);
        $authorRel = new Document([
            '$id' => 'author', 'key' => 'author',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'author',
                'relationType' => RelationType::OneToMany,
                'twoWay' => true, 'twoWayKey' => 'articles',
                'onDelete' => 'restrict', 'side' => 'child',
            ],
        ]);

        $existingDoc = new Document([
            '$id' => 'author1', '$collection' => 'author',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Author 1', 'articles' => [],
        ]);

        $db = $this->buildDatabase(
            [$this->makeCollection('author', [$nameAttr, $relAttr]), $this->makeCollection('article', [$authorRel])],
            ['author:author1' => $existingDoc]
        );

        $updated = $db->updateDocument('author', 'author1', new Document([
            'articles' => Operator::arrayAppend(['article2']),
        ]));

        $this->assertNotNull($updated);
    }

    public function testOneToManyChildSideRejectsArrayOperators(): void
    {
        $titleAttr = new Document([
            '$id' => 'title', 'key' => 'title', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $childRelAttr = new Document([
            '$id' => 'parent', 'key' => 'parent',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'parent_o2m',
                'relationType' => RelationType::OneToMany,
                'twoWay' => true, 'twoWayKey' => 'children',
                'onDelete' => 'restrict', 'side' => 'child',
            ],
        ]);

        $existingDoc = new Document([
            '$id' => 'child1', '$collection' => 'child_o2m',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Child 1', 'parent' => null,
        ]);

        $db = $this->buildDatabase(
            [$this->makeCollection('parent_o2m'), $this->makeCollection('child_o2m', [$titleAttr, $childRelAttr])],
            ['child_o2m:child1' => $existingDoc]
        );

        $this->expectException(StructureException::class);
        $this->expectExceptionMessage('single-value relationship');

        $db->updateDocument('child_o2m', 'child1', new Document([
            'parent' => Operator::arrayAppend(['parent2']),
        ]));
    }

    public function testManyToManyRelationshipWithArrayOperators(): void
    {
        $nameAttr = new Document([
            '$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value,
            'size' => 255, 'required' => true, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
        ]);
        $relAttr = new Document([
            '$id' => 'books', 'key' => 'books',
            'type' => ColumnType::Relationship->value,
            'size' => 0, 'required' => false, 'default' => null,
            'signed' => true, 'array' => false, 'filters' => [],
            'options' => [
                'relatedCollection' => 'book',
                'relationType' => RelationType::ManyToMany,
                'twoWay' => true, 'twoWayKey' => 'libraries',
                'onDelete' => 'restrict', 'side' => 'parent',
            ],
        ]);

        $existingDoc = new Document([
            '$id' => 'library1', '$collection' => 'library',
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Library 1', 'books' => [],
        ]);

        $db = $this->buildDatabase(
            [$this->makeCollection('library', [$nameAttr, $relAttr]), $this->makeCollection('book')],
            ['library:library1' => $existingDoc]
        );

        $updated = $db->updateDocument('library', 'library1', new Document([
            'books' => Operator::arrayAppend(['book2']),
        ]));

        $this->assertNotNull($updated);
    }
}

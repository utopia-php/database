<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionParameter;
use Utopia\Cache\Adapter\None as NoneAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ForeignKeyAction;

class RelationshipModelTest extends TestCase
{
    public function testConstructor(): void
    {
        $rel = Relationship::oneToMany(
            collection: 'posts',
            relatedCollection: 'comments',
            twoWay: true,
            key: 'comments',
            twoWayKey: 'post',
            onDelete: ForeignKeyAction::Cascade,
            side: RelationSide::Parent,
        );

        $this->assertSame('posts', $rel->collection);
        $this->assertSame('comments', $rel->relatedCollection);
        $this->assertSame(RelationType::OneToMany, $rel->type);
        $this->assertTrue($rel->twoWay);
        $this->assertSame('comments', $rel->key);
        $this->assertSame('post', $rel->twoWayKey);
        $this->assertSame(ForeignKeyAction::Cascade, $rel->onDelete);
        $this->assertSame(RelationSide::Parent, $rel->side);
    }

    public function testConstructorDefaults(): void
    {
        $rel = Relationship::oneToOne(
            collection: 'a',
            relatedCollection: 'b',
        );

        $this->assertFalse($rel->twoWay);
        $this->assertSame('', $rel->key);
        $this->assertSame('', $rel->twoWayKey);
        $this->assertSame(ForeignKeyAction::Restrict, $rel->onDelete);
        $this->assertSame(RelationSide::Parent, $rel->side);
    }

    public function testToDocumentProducesCorrectStructure(): void
    {
        $rel = Relationship::oneToOne(
            collection: 'users',
            relatedCollection: 'profiles',
            twoWay: true,
            key: 'profile',
            twoWayKey: 'user',
            onDelete: ForeignKeyAction::SetNull,
            side: RelationSide::Parent,
        );

        $doc = $rel->toDocument();

        $this->assertInstanceOf(Document::class, $doc);
        $this->assertSame('profiles', $doc->getAttribute('relatedCollection'));
        $this->assertSame('oneToOne', $doc->getAttribute('relationType'));
        $this->assertTrue($doc->getAttribute('twoWay'));
        $this->assertSame('user', $doc->getAttribute('twoWayKey'));
        $this->assertSame('setNull', $doc->getAttribute('onDelete'));
        $this->assertSame('parent', $doc->getAttribute('side'));
    }

    public function testToDocumentDoesNotIncludeCollectionOrKey(): void
    {
        $rel = Relationship::manyToMany(
            collection: 'posts',
            relatedCollection: 'tags',
            key: 'tags',
        );

        $doc = $rel->toDocument();

        $this->assertNull($doc->getAttribute('collection'));
        $this->assertNull($doc->getAttribute('key'));
    }

    public function testFromDocumentRoundtrip(): void
    {
        $attrDoc = new Document([
            '$id' => 'comments',
            'key' => 'comments',
            'type' => 'relationship',
            'options' => new Document([
                'relatedCollection' => 'comments',
                'relationType' => 'oneToMany',
                'twoWay' => true,
                'twoWayKey' => 'post',
                'onDelete' => 'cascade',
                'side' => 'parent',
            ]),
        ]);

        $rel = Relationship::fromDocument('posts', $attrDoc);

        $this->assertSame('posts', $rel->collection);
        $this->assertSame('comments', $rel->relatedCollection);
        $this->assertSame(RelationType::OneToMany, $rel->type);
        $this->assertTrue($rel->twoWay);
        $this->assertSame('comments', $rel->key);
        $this->assertSame('post', $rel->twoWayKey);
        $this->assertSame(ForeignKeyAction::Cascade, $rel->onDelete);
        $this->assertSame(RelationSide::Parent, $rel->side);
    }

    public function testFromDocumentWithArrayOptions(): void
    {
        $attrDoc = new Document([
            '$id' => 'author',
            'key' => 'author',
            'type' => 'relationship',
            'options' => [
                'relatedCollection' => 'users',
                'relationType' => 'manyToOne',
                'twoWay' => false,
                'twoWayKey' => 'posts',
                'onDelete' => 'restrict',
                'side' => 'child',
            ],
        ]);

        $rel = Relationship::fromDocument('posts', $attrDoc);

        $this->assertSame('users', $rel->relatedCollection);
        $this->assertSame(RelationType::ManyToOne, $rel->type);
        $this->assertFalse($rel->twoWay);
        $this->assertSame(RelationSide::Child, $rel->side);
    }

    public function testFromDocumentWithMissingOptions(): void
    {
        $attrDoc = new Document([
            '$id' => 'ref',
            'key' => 'ref',
            'type' => 'relationship',
        ]);

        $rel = Relationship::fromDocument('coll', $attrDoc);

        $this->assertSame('coll', $rel->collection);
        $this->assertSame('', $rel->relatedCollection);
        $this->assertSame(RelationType::OneToOne, $rel->type);
        $this->assertFalse($rel->twoWay);
        $this->assertSame('', $rel->twoWayKey);
        $this->assertSame(ForeignKeyAction::Restrict, $rel->onDelete);
        $this->assertSame(RelationSide::Parent, $rel->side);
    }

    public function testAllRelationTypeValues(): void
    {
        $types = [
            RelationType::OneToOne,
            RelationType::OneToMany,
            RelationType::ManyToOne,
            RelationType::ManyToMany,
        ];

        foreach ($types as $type) {
            $attrDoc = new Document([
                '$id' => 'rel',
                'key' => 'rel',
                'options' => [
                    'relatedCollection' => 'target',
                    'relationType' => $type->value,
                ],
            ]);

            $rel = Relationship::fromDocument('source', $attrDoc);
            $this->assertSame($type, $rel->type, "Failed for type: {$type->value}");
        }
    }

    public function testTwoWayFlag(): void
    {
        $twoWay = new Document([
            '$id' => 'rel',
            'key' => 'rel',
            'options' => [
                'relatedCollection' => 'b',
                'relationType' => 'oneToOne',
                'twoWay' => true,
                'twoWayKey' => 'back',
            ],
        ]);

        $rel = Relationship::fromDocument('a', $twoWay);
        $this->assertTrue($rel->twoWay);
        $this->assertSame('back', $rel->twoWayKey);

        $oneWay = new Document([
            '$id' => 'rel',
            'key' => 'rel',
            'options' => [
                'relatedCollection' => 'b',
                'relationType' => 'oneToOne',
                'twoWay' => false,
            ],
        ]);

        $rel2 = Relationship::fromDocument('a', $oneWay);
        $this->assertFalse($rel2->twoWay);
    }

    public function testAllForeignKeyActionValues(): void
    {
        $actions = [
            ForeignKeyAction::Cascade,
            ForeignKeyAction::SetNull,
            ForeignKeyAction::SetDefault,
            ForeignKeyAction::Restrict,
            ForeignKeyAction::NoAction,
        ];

        foreach ($actions as $action) {
            $attrDoc = new Document([
                '$id' => 'rel',
                'key' => 'rel',
                'options' => [
                    'relatedCollection' => 'target',
                    'relationType' => 'oneToOne',
                    'onDelete' => $action->value,
                ],
            ]);

            $rel = Relationship::fromDocument('source', $attrDoc);
            $this->assertSame($action, $rel->onDelete, "Failed for action: {$action->value}");
        }
    }

    public function testFromDocumentWithEnumInstances(): void
    {
        $attrDoc = new Document([
            '$id' => 'rel',
            'key' => 'rel',
            'options' => [
                'relatedCollection' => 'target',
                'relationType' => RelationType::ManyToMany,
                'onDelete' => ForeignKeyAction::Cascade,
                'side' => RelationSide::Child,
            ],
        ]);

        $rel = Relationship::fromDocument('source', $attrDoc);
        $this->assertSame(RelationType::ManyToMany, $rel->type);
        $this->assertSame(ForeignKeyAction::Cascade, $rel->onDelete);
        $this->assertSame(RelationSide::Child, $rel->side);
    }

    /**
     * @return array<string, array{string, RelationType}>
     */
    public static function factories(): array
    {
        return [
            'oneToOne' => ['oneToOne', RelationType::OneToOne],
            'oneToMany' => ['oneToMany', RelationType::OneToMany],
            'manyToOne' => ['manyToOne', RelationType::ManyToOne],
            'manyToMany' => ['manyToMany', RelationType::ManyToMany],
        ];
    }

    #[DataProvider('factories')]
    public function testFactorySetsTypeAndDefaults(string $factory, RelationType $type): void
    {
        $relationship = Relationship::{$factory}(
            collection: 'posts',
            relatedCollection: 'comments',
        );

        $this->assertInstanceOf(Relationship::class, $relationship);
        $this->assertSame('posts', $relationship->collection);
        $this->assertSame('comments', $relationship->relatedCollection);
        $this->assertSame($type, $relationship->type);
        $this->assertFalse($relationship->twoWay);
        $this->assertSame('', $relationship->key);
        $this->assertSame('', $relationship->twoWayKey);
        $this->assertSame(ForeignKeyAction::Restrict, $relationship->onDelete);
        $this->assertSame(RelationSide::Parent, $relationship->side);
    }

    public function testFactoryOmitsTypeParameter(): void
    {
        $names = array_map(
            static fn (ReflectionParameter $parameter): string => $parameter->getName(),
            (new ReflectionMethod(Relationship::class, 'oneToOne'))->getParameters(),
        );

        $this->assertSame(false, in_array('type', $names, true));
    }

    public function testFactoryForwardsOptionalArguments(): void
    {
        $relationship = Relationship::manyToOne(
            collection: 'reviews',
            relatedCollection: 'movies',
            twoWay: true,
            key: 'movie',
            twoWayKey: 'reviews',
            onDelete: ForeignKeyAction::Cascade,
            side: RelationSide::Child,
        );

        $this->assertSame(RelationType::ManyToOne, $relationship->type);
        $this->assertTrue($relationship->twoWay);
        $this->assertSame('movie', $relationship->key);
        $this->assertSame('reviews', $relationship->twoWayKey);
        $this->assertSame(ForeignKeyAction::Cascade, $relationship->onDelete);
        $this->assertSame(RelationSide::Child, $relationship->side);
    }

    public function testBaseConstructorWithDynamicType(): void
    {
        $type = RelationType::OneToOne;
        $relationship = new Relationship(
            collection: 'a',
            relatedCollection: 'b',
            type: $type,
        );

        $this->assertSame(Relationship::class, $relationship::class);
        $this->assertSame(RelationType::OneToOne, $relationship->type);
    }

    public function testExtendsDocument(): void
    {
        $relationship = Relationship::oneToOne(
            collection: 'posts',
            relatedCollection: 'comments',
            key: 'comments',
        );

        $this->assertInstanceOf(Document::class, $relationship);
        $this->assertSame('comments', $relationship->getId());
        $this->assertSame('posts', $relationship->getAttribute('collection'));
    }

    public function testFromArrayHydratesFlatDocument(): void
    {
        $relationship = Relationship::fromArray([
            '$id' => 'comments',
            'key' => 'comments',
            'collection' => 'posts',
            'relatedCollection' => 'comments',
            'relationType' => RelationType::OneToMany->value,
            'twoWay' => true,
            'twoWayKey' => 'post',
            'onDelete' => ForeignKeyAction::Cascade->value,
            'side' => RelationSide::Parent->value,
        ]);

        $this->assertSame('posts', $relationship->collection);
        $this->assertSame('comments', $relationship->relatedCollection);
        $this->assertSame(RelationType::OneToMany, $relationship->type);
        $this->assertTrue($relationship->twoWay);
        $this->assertSame('comments', $relationship->key);
        $this->assertSame('post', $relationship->twoWayKey);
        $this->assertSame(ForeignKeyAction::Cascade, $relationship->onDelete);
        $this->assertSame(RelationSide::Parent, $relationship->side);
    }

    public function testFromArrayHydratesAttributeOptions(): void
    {
        $relationship = Relationship::fromArray([
            '$id' => 'author',
            'key' => 'author',
            'collection' => 'posts',
            'type' => 'relationship',
            'options' => [
                'relatedCollection' => 'users',
                'relationType' => RelationType::ManyToOne->value,
                'twoWay' => false,
                'twoWayKey' => 'posts',
                'onDelete' => ForeignKeyAction::Restrict->value,
                'side' => RelationSide::Child->value,
            ],
        ]);

        $this->assertSame('posts', $relationship->collection);
        $this->assertSame('users', $relationship->relatedCollection);
        $this->assertSame(RelationType::ManyToOne, $relationship->type);
        $this->assertFalse($relationship->twoWay);
        $this->assertSame('author', $relationship->key);
        $this->assertSame(RelationSide::Child, $relationship->side);
    }

    public function testFromArrayRoundtrip(): void
    {
        $original = Relationship::manyToMany(
            collection: 'posts',
            relatedCollection: 'tags',
            twoWay: true,
            key: 'tags',
            twoWayKey: 'posts',
            onDelete: ForeignKeyAction::SetNull,
            side: RelationSide::Parent,
        );

        $restored = Relationship::fromArray($original->getArrayCopy());

        $this->assertSame($original->collection, $restored->collection);
        $this->assertSame($original->relatedCollection, $restored->relatedCollection);
        $this->assertSame($original->type, $restored->type);
        $this->assertSame($original->twoWay, $restored->twoWay);
        $this->assertSame($original->key, $restored->key);
        $this->assertSame($original->twoWayKey, $restored->twoWayKey);
        $this->assertSame($original->onDelete, $restored->onDelete);
        $this->assertSame($original->side, $restored->side);
    }

    public function testSetDocumentTypeAcceptsRelationship(): void
    {
        $database = $this->database();
        $database->setDocumentType('rels', Relationship::class);

        $this->assertSame(Relationship::class, $database->getDocumentType('rels'));
    }

    public function testCreateDocumentInstanceHydratesRelationship(): void
    {
        $database = $this->database();
        $database->setDocumentType('rels', Relationship::class);

        $document = $this->instantiate($database, 'rels', [
            '$id' => 'comments',
            '$collection' => 'rels',
            'key' => 'comments',
            'collection' => 'posts',
            'relatedCollection' => 'comments',
            'relationType' => RelationType::OneToMany->value,
            'twoWay' => true,
            'twoWayKey' => 'post',
            'onDelete' => ForeignKeyAction::Cascade->value,
            'side' => RelationSide::Parent->value,
        ]);

        $this->assertInstanceOf(Relationship::class, $document);
        $this->assertSame('posts', $document->collection);
        $this->assertSame('comments', $document->relatedCollection);
        $this->assertSame(RelationType::OneToMany, $document->type);
        $this->assertTrue($document->twoWay);
        $this->assertSame('comments', $document->key);
        $this->assertSame(ForeignKeyAction::Cascade, $document->onDelete);
    }

    private function database(): Database
    {
        return new Database(
            $this->createStub(Adapter::class),
            new Cache(new NoneAdapter()),
        );
    }

    /**
     * @param array<string, mixed> $data
     */
    private function instantiate(Database $database, string $collection, array $data): Document
    {
        $method = new ReflectionMethod(Database::class, 'createDocumentInstance');

        /** @var Document $document */
        $document = $method->invoke($database, $collection, $data);

        return $document;
    }
}

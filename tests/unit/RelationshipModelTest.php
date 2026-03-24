<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ForeignKeyAction;

class RelationshipModelTest extends TestCase
{
    public function testConstructor(): void
    {
        $rel = new Relationship(
            collection: 'posts',
            relatedCollection: 'comments',
            type: RelationType::OneToMany,
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
        $rel = new Relationship(
            collection: 'a',
            relatedCollection: 'b',
            type: RelationType::OneToOne,
        );

        $this->assertFalse($rel->twoWay);
        $this->assertSame('', $rel->key);
        $this->assertSame('', $rel->twoWayKey);
        $this->assertSame(ForeignKeyAction::Restrict, $rel->onDelete);
        $this->assertSame(RelationSide::Parent, $rel->side);
    }

    public function testToDocumentProducesCorrectStructure(): void
    {
        $rel = new Relationship(
            collection: 'users',
            relatedCollection: 'profiles',
            type: RelationType::OneToOne,
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
        $rel = new Relationship(
            collection: 'posts',
            relatedCollection: 'tags',
            type: RelationType::ManyToMany,
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
}

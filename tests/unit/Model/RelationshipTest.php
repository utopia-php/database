<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Query\Schema\ForeignKeyAction;

final class RelationshipTest extends TestCase
{
    public function testConstructorIsNotCallableFromOutside(): void
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Call to private Utopia\Database\Relationship::__construct()');

        $relationship = new Relationship('users', RelationshipType::OneToOne, false, null, null, RelationshipDeleteAction::Restrict); // @phpstan-ignore new.privateConstructor
    }

    public function testInverseSwapsKeysAndPointsAtTheGivenCollection(): void
    {
        $relationship = Relationship::oneToMany('comments', key: 'comments', twoWay: true, twoWayKey: 'post', onDelete: RelationshipDeleteAction::Cascade);

        $inverse = $relationship->inverse('posts');

        $this->assertSame('posts', $inverse->relatedCollection);
        $this->assertSame('post', $inverse->key);
        $this->assertSame('comments', $inverse->twoWayKey);
        $this->assertSame(RelationshipType::OneToMany, $inverse->type);
        $this->assertTrue($inverse->twoWay);
        $this->assertSame(RelationshipDeleteAction::Cascade, $inverse->onDelete);
    }

    public function testInverseLeavesTheOriginalUntouched(): void
    {
        $relationship = Relationship::manyToMany('tags', key: 'tags', twoWay: true, twoWayKey: 'posts');

        $relationship->inverse('posts');

        $this->assertSame('tags', $relationship->relatedCollection);
        $this->assertSame('tags', $relationship->key);
        $this->assertSame('posts', $relationship->twoWayKey);
    }

    public function testInverseOfInverseRestoresTheKeys(): void
    {
        $relationship = Relationship::manyToOne('users', key: 'author', twoWay: true, twoWayKey: 'posts', onDelete: RelationshipDeleteAction::SetNull);

        $this->assertSameRelationship($relationship, $relationship->inverse('posts')->inverse('users'));
    }

    public function testInverseWithUnresolvedKeysKeepsThemUnresolved(): void
    {
        $inverse = Relationship::oneToOne('profiles', key: 'profile')->inverse('users');

        $this->assertNull($inverse->key);
        $this->assertSame('profile', $inverse->twoWayKey);
    }

    public function testToDocumentWritesTheStoredOptionsShape(): void
    {
        $stored = Relationship::oneToMany('comments', key: 'comments', twoWay: true, twoWayKey: 'post', onDelete: RelationshipDeleteAction::SetNull)->toDocument();

        $this->assertSame([
            'relatedCollection' => 'comments',
            'relationType' => 'oneToMany',
            'twoWay' => true,
            'key' => 'comments',
            'twoWayKey' => 'post',
            'onDelete' => 'setNull',
        ], $stored->getArrayCopy());
    }

    /**
     * @return array<string, array{Relationship}>
     */
    public static function relationships(): array
    {
        return [
            'oneToOne defaults' => [Relationship::oneToOne('profiles')],
            'oneToMany two-way cascade' => [Relationship::oneToMany('comments', key: 'comments', twoWay: true, twoWayKey: 'post', onDelete: RelationshipDeleteAction::Cascade)],
            'manyToOne set null' => [Relationship::manyToOne('users', key: 'author', twoWayKey: 'posts', onDelete: RelationshipDeleteAction::SetNull)],
            'manyToMany two-way' => [Relationship::manyToMany('tags', key: 'tags', twoWay: true, twoWayKey: 'posts')],
        ];
    }

    #[DataProvider('relationships')]
    public function testDocumentRoundTrip(Relationship $relationship): void
    {
        $this->assertSameRelationship($relationship, Relationship::fromDocument($relationship->toDocument()));
    }

    #[DataProvider('relationships')]
    public function testArrayRoundTrip(Relationship $relationship): void
    {
        $this->assertSameRelationship($relationship, Relationship::fromArray($relationship->toDocument()->getArrayCopy()));
    }

    public function testSevenXStoredOptionsHydrate(): void
    {
        $relationship = Relationship::fromDocument(new Document([
            'relatedCollection' => 'comments',
            'relationType' => 'oneToMany',
            'twoWay' => true,
            'twoWayKey' => 'post',
            'onDelete' => 'cascade',
            'side' => 'parent',
        ]));

        $this->assertSame('comments', $relationship->relatedCollection);
        $this->assertSame(RelationshipType::OneToMany, $relationship->type);
        $this->assertTrue($relationship->twoWay);
        $this->assertNull($relationship->key);
        $this->assertSame('post', $relationship->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Cascade, $relationship->onDelete);
    }

    public function testSideIsIgnored(): void
    {
        $data = ['relatedCollection' => 'users', 'relationType' => 'manyToOne', 'key' => 'author', 'twoWayKey' => 'posts'];

        $this->assertSameRelationship(
            Relationship::fromArray($data),
            Relationship::fromArray($data + ['side' => RelationshipSide::Child->value]),
        );
        $this->assertArrayNotHasKey('side', Relationship::fromArray($data + ['side' => 'child'])->toDocument()->getArrayCopy());
    }

    public function testMissingOptionalKeysFallBackToDefaults(): void
    {
        $relationship = Relationship::fromArray(['relatedCollection' => 'users', 'relationType' => 'oneToOne']);

        $this->assertFalse($relationship->twoWay);
        $this->assertNull($relationship->key);
        $this->assertNull($relationship->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Restrict, $relationship->onDelete);
    }

    public function testEmptyStoredKeysHydrateAsUnresolved(): void
    {
        $relationship = Relationship::fromArray(['relatedCollection' => 'users', 'relationType' => 'oneToOne', 'key' => '', 'twoWayKey' => '']);

        $this->assertNull($relationship->key);
        $this->assertNull($relationship->twoWayKey);
    }

    public function testEnumValuesAreAcceptedInPlaceOfStrings(): void
    {
        $relationship = Relationship::fromArray([
            'relatedCollection' => 'users',
            'relationType' => RelationshipType::ManyToMany,
            'onDelete' => RelationshipDeleteAction::SetNull,
        ]);

        $this->assertSame(RelationshipType::ManyToMany, $relationship->type);
        $this->assertSame(RelationshipDeleteAction::SetNull, $relationship->onDelete);
    }

    public function testToOptionsIsTheStoredShapeWithoutTheKeyPlusTheSide(): void
    {
        $relationship = Relationship::manyToMany('tags', key: 'tags', twoWay: true, twoWayKey: 'posts', onDelete: RelationshipDeleteAction::SetNull);

        $this->assertSame([
            'relatedCollection' => 'tags',
            'relationType' => 'manyToMany',
            'twoWay' => true,
            'twoWayKey' => 'posts',
            'onDelete' => 'setNull',
            'side' => 'child',
        ], $relationship->toOptions(RelationshipSide::Child));
    }

    public function testToOptionsHydratesBackWithTheKey(): void
    {
        $relationship = Relationship::oneToMany('comments', key: 'comments', twoWayKey: 'post');

        $options = $relationship->toOptions(RelationshipSide::Parent);
        unset($options[Relationship::SIDE]);

        $this->assertSameRelationship($relationship, Relationship::fromArray([...$options, 'key' => 'comments']));
    }

    public function testSupportedForeignKeyActionIsAccepted(): void
    {
        $relationship = Relationship::fromArray(['relatedCollection' => 'users', 'relationType' => 'oneToOne', 'onDelete' => ForeignKeyAction::Cascade]);

        $this->assertSame(RelationshipDeleteAction::Cascade, $relationship->onDelete);
    }

    /**
     * @return array<string, array{array<string, mixed>}>
     */
    public static function invalidRows(): array
    {
        return [
            'missing related collection' => [['relationType' => 'oneToOne']],
            'empty related collection' => [['relatedCollection' => '', 'relationType' => 'oneToOne']],
            'non-string related collection' => [['relatedCollection' => 7, 'relationType' => 'oneToOne']],
            'missing type' => [['relatedCollection' => 'users']],
            'unknown type' => [['relatedCollection' => 'users', 'relationType' => 'oneToFew']],
            'non-string type' => [['relatedCollection' => 'users', 'relationType' => 1]],
            'non-string key' => [['relatedCollection' => 'users', 'relationType' => 'oneToOne', 'key' => 5]],
            'non-string two-way key' => [['relatedCollection' => 'users', 'relationType' => 'oneToOne', 'twoWayKey' => ['posts']]],
            'unknown delete action' => [['relatedCollection' => 'users', 'relationType' => 'oneToOne', 'onDelete' => 'explode']],
            'unrelated enum as delete action' => [['relatedCollection' => 'users', 'relationType' => 'oneToOne', 'onDelete' => RelationshipSide::Parent]],
        ];
    }

    /**
     * @param  array<string, mixed>  $data
     */
    #[DataProvider('invalidRows')]
    public function testInvalidArrayIsRejected(array $data): void
    {
        $this->expectException(RelationshipException::class);

        Relationship::fromArray($data);
    }

    /**
     * @param  array<string, mixed>  $data
     */
    #[DataProvider('invalidRows')]
    public function testInvalidDocumentIsRejected(array $data): void
    {
        $this->expectException(RelationshipException::class);

        Relationship::fromDocument(new Document($data));
    }

    private function assertSameRelationship(Relationship $expected, Relationship $actual): void
    {
        $this->assertSame($expected->relatedCollection, $actual->relatedCollection);
        $this->assertSame($expected->type, $actual->type);
        $this->assertSame($expected->twoWay, $actual->twoWay);
        $this->assertSame($expected->key, $actual->key);
        $this->assertSame($expected->twoWayKey, $actual->twoWayKey);
        $this->assertSame($expected->onDelete, $actual->onDelete);
    }
}

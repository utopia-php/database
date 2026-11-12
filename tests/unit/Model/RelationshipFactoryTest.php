<?php

namespace Tests\Unit\Model;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;

final class RelationshipFactoryTest extends TestCase
{
    /**
     * @return array<string, array{Closure(string, ?string=, bool=, ?string=, RelationshipDeleteAction=): Relationship, RelationshipType}>
     */
    public static function factories(): array
    {
        return [
            'oneToOne' => [Relationship::oneToOne(...), RelationshipType::OneToOne],
            'oneToMany' => [Relationship::oneToMany(...), RelationshipType::OneToMany],
            'manyToOne' => [Relationship::manyToOne(...), RelationshipType::ManyToOne],
            'manyToMany' => [Relationship::manyToMany(...), RelationshipType::ManyToMany],
        ];
    }

    /**
     * @param  Closure(string, ?string=, bool=, ?string=, RelationshipDeleteAction=): Relationship  $factory
     */
    #[DataProvider('factories')]
    public function testFactoryCarriesEveryArgument(Closure $factory, RelationshipType $type): void
    {
        $relationship = $factory('comments', 'comments', true, 'post', RelationshipDeleteAction::Cascade);

        $this->assertSame('comments', $relationship->relatedCollection);
        $this->assertSame($type, $relationship->type);
        $this->assertTrue($relationship->twoWay);
        $this->assertSame('comments', $relationship->key);
        $this->assertSame('post', $relationship->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Cascade, $relationship->onDelete);
    }

    /**
     * @param  Closure(string, ?string=, bool=, ?string=, RelationshipDeleteAction=): Relationship  $factory
     */
    #[DataProvider('factories')]
    public function testFactoryDefaultsToOneWayRestrictWithDerivedKeys(Closure $factory, RelationshipType $type): void
    {
        $relationship = $factory('users');

        $this->assertSame('users', $relationship->relatedCollection);
        $this->assertSame($type, $relationship->type);
        $this->assertFalse($relationship->twoWay);
        $this->assertNull($relationship->key);
        $this->assertNull($relationship->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Restrict, $relationship->onDelete);
    }

    public function testFactoriesAcceptNamedArguments(): void
    {
        $relationship = Relationship::manyToMany(
            relatedCollection: 'tags',
            key: 'tags',
            twoWay: true,
            twoWayKey: 'posts',
            onDelete: RelationshipDeleteAction::SetNull,
        );

        $this->assertSame('tags', $relationship->relatedCollection);
        $this->assertSame(RelationshipType::ManyToMany, $relationship->type);
        $this->assertTrue($relationship->twoWay);
        $this->assertSame('tags', $relationship->key);
        $this->assertSame('posts', $relationship->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::SetNull, $relationship->onDelete);
    }

    public function testOneWayRelationshipMayNameItsTwoWayKey(): void
    {
        $relationship = Relationship::manyToOne('users', key: 'author', twoWayKey: 'posts');

        $this->assertFalse($relationship->twoWay);
        $this->assertSame('posts', $relationship->twoWayKey);
    }
}

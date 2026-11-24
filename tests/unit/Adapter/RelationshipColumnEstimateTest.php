<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;

final class RelationshipColumnEstimateTest extends TestCase
{
    private const int RELATIONSHIP_WIDTH = 1021;

    public function testSidesWithoutColumnDoNotWidenTheRow(): void
    {
        $adapter = new MySQL($this->createStub(\PDO::class));

        $strings = [];
        for ($i = 0; $i < 7; $i++) {
            $strings[] = Attribute::string(key: 'string'.$i, size: 2200);
        }

        $relationships = [
            Attribute::relationship('authors', Relationship::manyToOne(relatedCollection: 'authors', twoWay: true, twoWayKey: 'book'), RelationshipSide::Child),
            Attribute::relationship('tags', Relationship::manyToMany(relatedCollection: 'tags', twoWay: true, twoWayKey: 'books'), RelationshipSide::Child),
            Attribute::relationship('reviews', Relationship::manyToOne(relatedCollection: 'reviews', twoWay: true, twoWayKey: 'book'), RelationshipSide::Child),
        ];

        $withoutRelationships = Collection::create(id: 'books', attributes: $strings);
        $collection = Collection::create(id: 'books', attributes: [...$strings, ...$relationships]);

        $width = $adapter->getAttributeWidth($collection);

        $this->assertSame(62681, $width, 'Seven VARCHAR(2200) columns on top of the internal columns, and nothing for the relationships');
        $this->assertLessThan($adapter->limits()->documentSize, $width);
        $this->assertSame($adapter->getAttributeWidth($withoutRelationships), $width);
        $this->assertSame($adapter->getCountOfAttributes($withoutRelationships), $adapter->getCountOfAttributes($collection));
    }

    /**
     * @return array<string, array{Relationship, RelationshipSide, bool}>
     */
    public static function sides(): array
    {
        return [
            'oneToOne parent' => [Relationship::oneToOne(relatedCollection: 'other'), RelationshipSide::Parent, true],
            'oneToOne two-way child' => [Relationship::oneToOne(relatedCollection: 'other', twoWay: true), RelationshipSide::Child, true],
            'oneToOne one-way child' => [Relationship::oneToOne(relatedCollection: 'other'), RelationshipSide::Child, false],
            'oneToMany parent' => [Relationship::oneToMany(relatedCollection: 'other', twoWay: true), RelationshipSide::Parent, false],
            'oneToMany child' => [Relationship::oneToMany(relatedCollection: 'other'), RelationshipSide::Child, true],
            'manyToOne parent' => [Relationship::manyToOne(relatedCollection: 'other'), RelationshipSide::Parent, true],
            'manyToOne child' => [Relationship::manyToOne(relatedCollection: 'other', twoWay: true), RelationshipSide::Child, false],
            'manyToMany parent' => [Relationship::manyToMany(relatedCollection: 'other', twoWay: true), RelationshipSide::Parent, false],
            'manyToMany child' => [Relationship::manyToMany(relatedCollection: 'other', twoWay: true), RelationshipSide::Child, false],
        ];
    }

    #[DataProvider('sides')]
    public function testOnlyASideWithAColumnIsCounted(Relationship $relationship, RelationshipSide $side, bool $hasColumn): void
    {
        $adapter = new MySQL($this->createStub(\PDO::class));

        $empty = Collection::create(id: 'books');
        $collection = Collection::create(id: 'books', attributes: [Attribute::relationship('related', $relationship, $side)]);

        $this->assertSame($hasColumn, $relationship->hasColumn($side));
        $this->assertSame(
            $hasColumn ? self::RELATIONSHIP_WIDTH : 0,
            $adapter->getAttributeWidth($collection) - $adapter->getAttributeWidth($empty),
        );
        $this->assertSame(
            $hasColumn ? 1 : 0,
            $adapter->getCountOfAttributes($collection) - $adapter->getCountOfAttributes($empty),
        );
    }
}

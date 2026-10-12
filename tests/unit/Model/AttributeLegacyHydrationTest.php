<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Document;
use Utopia\Database\Format;
use Utopia\Database\IntegerWidth;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipSide;

final class AttributeLegacyHydrationTest extends TestCase
{
    /**
     * @param  array<string, mixed>  $stored
     * @return array<string, mixed>
     */
    private static function sevenFour(array $stored): array
    {
        return [
            '$id' => 'field',
            'key' => 'field',
            'size' => 0,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'format' => '',
            'formatOptions' => [],
            'filters' => [],
            ...$stored,
        ];
    }

    /**
     * @return array<string, array{array<string, mixed>, Attribute}>
     */
    public static function legacyShapes(): array
    {
        return [
            'signed datetime' => [self::sevenFour(['type' => 'datetime', 'filters' => ['datetime']]), Attribute::datetime('field')],
            'signed datetime array' => [self::sevenFour(['type' => 'datetime', 'array' => true, 'filters' => ['datetime']]), Attribute::datetime('field', array: true)],
            'datetime without its filter' => [self::sevenFour(['type' => 'datetime']), Attribute::datetime('field')],
            'sizeless signed integer' => [self::sevenFour(['type' => 'integer']), Attribute::integer('field')],
            '32-bit signed integer' => [self::sevenFour(['type' => 'integer', 'size' => 4]), Attribute::integer('field')],
            '32-bit unsigned integer' => [self::sevenFour(['type' => 'integer', 'size' => 4, 'signed' => false]), Attribute::integer('field', signed: false)],
            '64-bit signed integer' => [self::sevenFour(['type' => 'integer', 'size' => 8]), Attribute::integer('field', width: IntegerWidth::Bits64)],
            '64-bit unsigned integer' => [self::sevenFour(['type' => 'integer', 'size' => 8, 'signed' => false]), Attribute::integer('field', signed: false, width: IntegerWidth::Bits64)],
            'oversized integer' => [self::sevenFour(['type' => 'integer', 'size' => 16]), Attribute::integer('field', width: IntegerWidth::Bits64)],
            'unsigned bigint' => [self::sevenFour(['type' => 'bigint', 'signed' => false]), Attribute::bigInteger('field', signed: false)],
            'string' => [self::sevenFour(['type' => 'string', 'size' => 255]), Attribute::string('field', 255)],
            'unsigned string' => [self::sevenFour(['type' => 'string', 'size' => 255, 'signed' => false]), Attribute::string('field', 255)],
            'string array' => [self::sevenFour(['type' => 'string', 'size' => 64, 'array' => true]), Attribute::string('field', 64, array: true)],
            'sizeless text' => [self::sevenFour(['type' => 'text']), Attribute::text('field')],
            'varchar' => [self::sevenFour(['type' => 'varchar', 'size' => 128]), Attribute::varchar('field', 128)],
            'signed double' => [self::sevenFour(['type' => 'double']), Attribute::double('field')],
            'unsigned double' => [self::sevenFour(['type' => 'double', 'signed' => false]), Attribute::double('field', signed: false)],
            'unsigned float' => [self::sevenFour(['type' => 'float', 'signed' => false]), Attribute::float('field', signed: false)],
            'boolean' => [self::sevenFour(['type' => 'boolean']), Attribute::boolean('field')],
            'unsigned boolean' => [self::sevenFour(['type' => 'boolean', 'signed' => false]), Attribute::boolean('field')],
            'id' => [self::sevenFour(['type' => 'id', 'signed' => false]), Attribute::id('field')],
            'point' => [self::sevenFour(['type' => 'point', 'filters' => ['point']]), Attribute::point('field')],
            'unsigned point' => [self::sevenFour(['type' => 'point', 'signed' => false, 'filters' => ['point']]), Attribute::point('field')],
            'linestring' => [self::sevenFour(['type' => 'linestring', 'filters' => ['linestring']]), Attribute::lineString('field')],
            'polygon' => [self::sevenFour(['type' => 'polygon', 'filters' => ['polygon']]), Attribute::polygon('field')],
            'vector' => [self::sevenFour(['type' => 'vector', 'size' => 3, 'filters' => ['vector']]), Attribute::vector('field', 3)],
            'object' => [self::sevenFour(['type' => 'object', 'filters' => ['object']]), Attribute::object('field')],
            'format' => [
                self::sevenFour(['type' => 'string', 'size' => 64, 'format' => 'enum', 'formatOptions' => ['elements' => ['a', 'b']]]),
                Attribute::string('field', 64, format: new Format('enum', ['elements' => ['a', 'b']])),
            ],
            'relationship' => [
                [
                    '$id' => 'field',
                    'key' => 'field',
                    'type' => 'relationship',
                    'required' => false,
                    'default' => null,
                    'options' => [
                        'relatedCollection' => 'comments',
                        'relationType' => 'oneToMany',
                        'twoWay' => true,
                        'twoWayKey' => 'post',
                        'onDelete' => 'cascade',
                        'side' => 'parent',
                    ],
                ],
                Attribute::relationship(
                    'field',
                    Relationship::oneToMany('comments', twoWay: true, twoWayKey: 'post', onDelete: RelationshipDeleteAction::Cascade),
                    RelationshipSide::Parent,
                ),
            ],
            'unsigned relationship' => [
                [
                    '$id' => 'field',
                    'key' => 'field',
                    'type' => 'relationship',
                    'size' => 0,
                    'signed' => false,
                    'array' => false,
                    'filters' => [],
                    'options' => [
                        'relatedCollection' => 'users',
                        'relationType' => 'manyToOne',
                        'twoWay' => false,
                        'twoWayKey' => 'posts',
                        'onDelete' => 'restrict',
                        'side' => 'child',
                    ],
                ],
                Attribute::relationship('field', Relationship::manyToOne('users', twoWayKey: 'posts'), RelationshipSide::Child),
            ],
        ];
    }

    /**
     * @param  array<string, mixed>  $stored
     */
    #[DataProvider('legacyShapes')]
    public function testLegacyStoredShapeHydratesAsTheEightZeroAttribute(array $stored, Attribute $created): void
    {
        $hydrated = Attribute::fromDocument(new Document($stored));

        $this->assertEquals($created, $hydrated);
        $this->assertSame($created->toDocument()->getArrayCopy(), $hydrated->toDocument()->getArrayCopy());
    }

    /**
     * @param  array<string, mixed>  $stored
     */
    #[DataProvider('legacyShapes')]
    public function testLegacyStoredShapeHydratesAlreadyNormalised(array $stored, Attribute $created): void
    {
        $hydrated = Attribute::fromArray($stored);

        $this->assertEquals($hydrated->apply(new AttributeUpdate()), $hydrated);
        $this->assertEquals($created, $hydrated);
    }

    public function testHydrationKeepsATypeFilterInItsStoredPosition(): void
    {
        $attribute = Attribute::fromDocument(new Document(self::sevenFour([
            'type' => 'datetime',
            'filters' => ['custom', 'datetime'],
        ])));

        $this->assertSame(['custom', 'datetime'], $attribute->filters);
    }

    public function testHydrationPrependsAMissingTypeFilterAheadOfCustomFilters(): void
    {
        $attribute = Attribute::fromDocument(new Document(self::sevenFour([
            'type' => 'point',
            'filters' => ['custom'],
        ])));

        $this->assertSame(['point', 'custom'], $attribute->filters);
    }
}

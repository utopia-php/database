<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\Validator\AttributeDefinition;
use Utopia\Query\Schema\ColumnType;

final class AttributeDefaultTypeTest extends TestCase
{
    /**
     * @return array<string, array{ColumnType, mixed}>
     */
    public static function scalarDefaultsOfTypesWithoutADefaultRule(): array
    {
        return [
            'object with a string' => [ColumnType::Object, 'x'],
            'point with a string' => [ColumnType::Point, 'POINT(1 2)'],
            'linestring with an integer' => [ColumnType::Linestring, 5],
            'polygon with a boolean' => [ColumnType::Polygon, true],
            'id with a string' => [ColumnType::Id, 'x'],
            'relationship with a string' => [ColumnType::Relationship, 'x'],
        ];
    }

    #[DataProvider('scalarDefaultsOfTypesWithoutADefaultRule')]
    public function testAScalarDefaultOfATypeWithoutADefaultRuleIsAnUnknownType(ColumnType $type, mixed $default): void
    {
        $validator = $this->validator(vectors: false, spatial: false);

        $message = $this->refusal($validator, Attribute::fromArray([
            'key' => 'value',
            'type' => $type,
            'default' => $default,
            'options' => ['relatedCollection' => 'others', 'relationType' => RelationshipType::OneToOne->value, 'side' => RelationshipSide::Parent->value],
        ]));

        $this->assertStringStartsWith("Unknown attribute type: {$type->value}. Must be one of ", $message);
        $this->assertStringContainsString(ColumnType::String->value, $message);
        $this->assertStringContainsString(ColumnType::Relationship->value, $message);
        $this->assertStringNotContainsString(ColumnType::Vector->value, $message);
        $this->assertStringNotContainsString(ColumnType::Point->value.',', $message);
    }

    public function testTheListedTypesFollowTheVectorAndSpatialSupport(): void
    {
        $message = $this->refusal(
            $this->validator(vectors: true, spatial: true),
            Attribute::fromArray(['key' => 'value', 'type' => ColumnType::Object, 'default' => 'x']),
        );

        $this->assertStringContainsString(ColumnType::Vector->value, $message);
        foreach ([ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon] as $spatial) {
            $this->assertStringContainsString($spatial->value, $message);
        }

        $vectorsOnly = $this->refusal(
            $this->validator(vectors: true, spatial: false),
            Attribute::id(key: 'value', default: 'x'),
        );
        $this->assertStringContainsString(ColumnType::Vector->value, $vectorsOnly);
        $this->assertStringNotContainsString(ColumnType::Polygon->value, $vectorsOnly);
    }

    public function testAnArrayDefaultOfASpatialOrObjectTypeIsNotCheckedItemByItem(): void
    {
        $validator = $this->validator(vectors: true, spatial: true);

        $this->assertTrue($validator->checkDefaultValue(Attribute::object(key: 'value', default: ['nested' => 'x'])));
    }

    private function refusal(AttributeDefinition $validator, Attribute $attribute): string
    {
        try {
            $validator->checkDefaultValue($attribute);
        } catch (DatabaseException $error) {
            $this->assertSame($error->getMessage(), $validator->getDescription());

            return $error->getMessage();
        }

        $this->fail("A scalar default on {$attribute->type->value} must be refused");
    }

    private function validator(bool $vectors, bool $spatial): AttributeDefinition
    {
        return new AttributeDefinition(
            attributes: [],
            supportForVectors: $vectors,
            supportForSpatialAttributes: $spatial,
            supportForObject: true,
        );
    }
}

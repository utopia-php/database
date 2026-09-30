<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Operator as OperatorValidator;
use Utopia\Query\Schema\ColumnType;

final class OperatorValidatorTest extends TestCase
{
    private const string RELATION = 'related';

    private const string SINGLE_VALUE = 'single-value relationship';

    private const string DOCUMENT_IDS = 'relationship values must be document IDs (strings) or Document objects';

    /**
     * @return array<string, array{RelationType, RelationSide}>
     */
    public static function singleValueSides(): array
    {
        return [
            'one-to-one parent' => [RelationType::OneToOne, RelationSide::Parent],
            'one-to-one child' => [RelationType::OneToOne, RelationSide::Child],
            'one-to-many child' => [RelationType::OneToMany, RelationSide::Child],
            'many-to-one parent' => [RelationType::ManyToOne, RelationSide::Parent],
        ];
    }

    /**
     * @return array<string, array{OperatorType, array<mixed>}>
     */
    public static function arrayOperators(): array
    {
        return [
            'arrayUnique' => [OperatorType::ArrayUnique, []],
            'arrayInsert' => [OperatorType::ArrayInsert, [0, 'b']],
            'arrayRemove' => [OperatorType::ArrayRemove, ['b']],
            'arrayIntersect' => [OperatorType::ArrayIntersect, ['b']],
            'arrayDiff' => [OperatorType::ArrayDiff, ['b']],
            'arrayFilter' => [OperatorType::ArrayFilter, ['isNotNull']],
        ];
    }

    /**
     * @return array<string, array{RelationType, RelationSide, OperatorType, array<mixed>}>
     */
    public static function arrayOperatorsOnSingleValueRelationships(): array
    {
        $cases = [];
        foreach (self::singleValueSides() as $sideName => [$type, $side]) {
            foreach (self::arrayOperators() as $operatorName => [$method, $values]) {
                $cases["{$operatorName} on {$sideName}"] = [$type, $side, $method, $values];
            }
        }

        return $cases;
    }

    /**
     * @param  array<mixed>  $values
     */
    #[DataProvider('arrayOperatorsOnSingleValueRelationships')]
    public function testArrayOperatorOnASingleValueRelationshipIsRejected(RelationType $type, RelationSide $side, OperatorType $method, array $values): void
    {
        $validator = $this->relationshipValidator($type, $side->value);

        $this->assertFalse($validator->isValid(new Operator($method, self::RELATION, $values)));
        $this->assertStringContainsString(self::SINGLE_VALUE, $validator->getDescription());
    }

    /**
     * @return array<string, array{OperatorType, array<mixed>}>
     */
    public static function nonIdentifierValues(): array
    {
        return [
            'arrayAppend of an integer' => [OperatorType::ArrayAppend, [5]],
            'arrayAppend of a list' => [OperatorType::ArrayAppend, [['x']]],
            'arrayPrepend of a boolean' => [OperatorType::ArrayPrepend, [true]],
            'arrayInsert of an integer' => [OperatorType::ArrayInsert, [0, 5]],
            'arrayInsert of a list' => [OperatorType::ArrayInsert, [0, ['x']]],
            'arrayRemove of an integer' => [OperatorType::ArrayRemove, [5]],
            'arrayRemove of a float in a list' => [OperatorType::ArrayRemove, [[1.5]]],
            'arrayIntersect of an integer' => [OperatorType::ArrayIntersect, [5]],
            'arrayDiff of an integer' => [OperatorType::ArrayDiff, [5]],
        ];
    }

    /**
     * @param  array<mixed>  $values
     */
    #[DataProvider('nonIdentifierValues')]
    public function testNonIdentifierRelationshipValuesAreRejected(OperatorType $method, array $values): void
    {
        $validator = $this->relationshipValidator(RelationType::ManyToMany, RelationSide::Parent->value);

        $this->assertFalse($validator->isValid(new Operator($method, self::RELATION, $values)));
        $this->assertStringContainsString(self::DOCUMENT_IDS, $validator->getDescription());
    }

    /**
     * @return array<string, array{OperatorType, array<mixed>}>
     */
    public static function identifierValues(): array
    {
        return [
            'arrayAppend' => [OperatorType::ArrayAppend, ['b', new Document([Document::ID => 'c'])]],
            'arrayInsert' => [OperatorType::ArrayInsert, [0, new Document([Document::ID => 'c'])]],
            'arrayRemove' => [OperatorType::ArrayRemove, [['b', 'c']]],
            'arrayIntersect' => [OperatorType::ArrayIntersect, ['b']],
            'arrayDiff' => [OperatorType::ArrayDiff, [new Document([Document::ID => 'b'])]],
            'arrayUnique' => [OperatorType::ArrayUnique, []],
            'arrayFilter' => [OperatorType::ArrayFilter, ['isNotNull']],
        ];
    }

    /**
     * @param  array<mixed>  $values
     */
    #[DataProvider('identifierValues')]
    public function testIdentifierValuesOnAManyToManyRelationshipAreAccepted(OperatorType $method, array $values): void
    {
        $validator = $this->relationshipValidator(RelationType::ManyToMany, RelationSide::Child->value);

        $this->assertTrue($validator->isValid(new Operator($method, self::RELATION, $values)), $validator->getDescription());
    }

    /**
     * @return array<string, array{mixed}>
     */
    public static function storedValuesOutsideTheRange(): array
    {
        return [
            'non-numeric' => ['abc'],
            'above the integer maximum' => [Database::MAX_INT + 1],
            'below the integer minimum' => [Database::MIN_INT - 1],
            'fractional' => [1.5],
        ];
    }

    #[DataProvider('storedValuesOutsideTheRange')]
    public function testAStoredValueOutsideTheAttributeRangeIsRejected(mixed $stored): void
    {
        $validator = $this->numericValidator(new Document(['count' => $stored]));

        $this->assertFalse($validator->isValid(new Operator(OperatorType::Increment, 'count', [1])));
        $this->assertSame('Cannot apply increment operator: current value is outside the attribute range', $validator->getDescription());
    }

    public function testAStoredValueInsideTheAttributeRangeIsAccepted(): void
    {
        $validator = $this->numericValidator(new Document(['count' => Database::MAX_INT - 1]));

        $this->assertTrue($validator->isValid(new Operator(OperatorType::Increment, 'count', [1])), $validator->getDescription());
    }

    public function testAResultThatCannotBePredictedIsRejected(): void
    {
        $validator = $this->numericValidator(new Document(['count' => 2]));

        $this->assertFalse($validator->isValid(new Operator(OperatorType::Power, 'count', [-1])));
        $this->assertSame('Cannot apply power operator: result is outside the attribute range', $validator->getDescription());

        $this->assertTrue($validator->isValid(new Operator(OperatorType::Power, 'count', [3])), $validator->getDescription());
    }

    private function numericValidator(?Document $current = null): OperatorValidator
    {
        return new OperatorValidator($this->collection([Attribute::integer(key: 'count')->toDocument()]), $current);
    }

    private function relationshipValidator(RelationType $type, RelationSide|string $side): OperatorValidator
    {
        return new OperatorValidator($this->collection([new Document([
            Document::ID => self::RELATION,
            'key' => self::RELATION,
            'type' => ColumnType::Relationship->value,
            'array' => false,
            'options' => [
                'relatedCollection' => 'others',
                'relationType' => $type->value,
                'twoWay' => true,
                'twoWayKey' => 'back',
                'side' => $side,
            ],
        ])]));
    }

    /**
     * @param  array<Document>  $attributes
     */
    private function collection(array $attributes): Document
    {
        return new Document([
            Document::ID => 'operands',
            Document::COLLECTION => Database::METADATA,
            'name' => 'operands',
            'attributes' => $attributes,
            'indexes' => [],
        ]);
    }
}

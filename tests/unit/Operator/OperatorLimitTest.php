<?php

namespace Tests\Unit\Operator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\OperatorType;
use Utopia\Database\Validator\Operator as OperatorValidator;

final class OperatorLimitTest extends TestCase
{
    /**
     * @return array<string, array{OperatorType, string, int|float|string}>
     */
    public static function fractionalLimits(): array
    {
        return [
            'increment max' => [OperatorType::Increment, 'count', 102.4],
            'decrement min' => [OperatorType::Decrement, 'count', -0.5],
            'multiply max' => [OperatorType::Multiply, 'count', 99.9],
            'divide min' => [OperatorType::Divide, 'count', 1.5],
            'power max' => [OperatorType::Power, 'count', 1000.01],
            'numeric string' => [OperatorType::Increment, 'count', '102.4'],
            'big integer' => [OperatorType::Increment, 'big', 4.0e15 + 0.5],
        ];
    }

    #[DataProvider('fractionalLimits')]
    public function testAFractionalLimitOnAnIntegerAttributeIsRefused(OperatorType $method, string $attribute, int|float|string $limit): void
    {
        $validator = $this->validator();

        $this->assertFalse($validator->isValid(new Operator($method, $attribute, [2, $limit])));
        $this->assertSame(
            "Cannot apply {$method->value} operator: max/min limit must be a whole number for integer attribute '{$attribute}', got {$limit}",
            $validator->getDescription(),
        );
    }

    /**
     * @return array<string, array{string, int|float|string}>
     */
    public static function wholeLimits(): array
    {
        return [
            'integer' => ['count', 100],
            'whole float' => ['count', 100.0],
            'integer string' => ['count', '100'],
            'whole float string' => ['count', '100.0'],
            'big whole float' => ['big', 9.0e18],
            'unsigned whole float beyond the signed range' => ['unsigned', 1.0e19],
            'fractional limit on a double' => ['score', 102.4],
        ];
    }

    #[DataProvider('wholeLimits')]
    public function testAWholeLimitIsAccepted(string $attribute, int|float|string $limit): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid(new Operator(OperatorType::Increment, $attribute, [1, $limit])), $validator->getDescription());
    }

    public function testALimitOutsideTheAttributeRangeIsRefused(): void
    {
        $validator = $this->validator();

        $this->assertFalse($validator->isValid(new Operator(OperatorType::Increment, 'big', [1, 1.0e19])));
        $this->assertSame(
            'Cannot apply increment operator: max/min limit must be between -9223372036854775808 and 9223372036854775807',
            $validator->getDescription(),
        );
    }

    public function testANonNumericLimitIsRefused(): void
    {
        $validator = $this->validator();

        $this->assertFalse($validator->isValid(new Operator(OperatorType::Increment, 'count', [1, 'many'])));
        $this->assertSame('Cannot apply increment operator: max/min limit must be numeric, got string', $validator->getDescription());
    }

    private function validator(): OperatorValidator
    {
        return new OperatorValidator(new Document([
            '$id' => 'counters',
            '$collection' => Database::METADATA,
            'attributes' => \array_map(static fn (Attribute $attribute): Document => $attribute->toDocument(), [
                Attribute::integer(key: 'count'),
                Attribute::bigInteger(key: 'big'),
                Attribute::bigInteger(key: 'unsigned', signed: false),
                Attribute::double(key: 'score'),
            ]),
            'indexes' => [],
        ]));
    }
}

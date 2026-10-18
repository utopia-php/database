<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Finite;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Query\Schema\ColumnType;

final class FiniteTest extends TestCase
{
    /**
     * @return array<string, array{int|float}>
     */
    public static function finite(): array
    {
        return [
            'zero' => [0.0],
            'integer' => [5],
            'fraction' => [0.1 + 0.2],
            'denormal' => [5e-324],
            'negative denormal' => [-5e-324],
            'maximum' => [PHP_FLOAT_MAX],
            'minimum' => [-PHP_FLOAT_MAX],
        ];
    }

    /**
     * @return array<string, array{float}>
     */
    public static function nonFinite(): array
    {
        return [
            'infinity' => [INF],
            'negative infinity' => [-INF],
            'not a number' => [NAN],
            'overflowing literal' => [\floatval(\json_decode('1e400'))],
            'negative overflowing literal' => [\floatval(\json_decode('-1e400'))],
        ];
    }

    #[DataProvider('finite')]
    public function testAcceptsAFiniteNumber(int|float $value): void
    {
        $this->assertTrue((new Finite())->isValid($value));
        $this->assertTrue($this->filter()->isValid(Query::equal('f', [$value])));
        $this->assertTrue($this->filter()->isValid(Query::between('f', $value, $value)));
    }

    #[DataProvider('nonFinite')]
    public function testRejectsANonFiniteNumber(float $value): void
    {
        $this->assertFalse((new Finite())->isValid($value));

        foreach ([
            Query::equal('f', [$value]),
            Query::notEqual('f', $value),
            Query::lessThan('f', $value),
            Query::greaterThanEqual('f', $value),
            Query::between('f', 0, $value),
            Query::equal('fa', [$value]),
        ] as $query) {
            $filter = $this->filter();
            $this->assertFalse($filter->isValid($query), $query->getMethod()->value);
            $this->assertStringContainsString('Query value is invalid for attribute', $filter->getDescription());
        }
    }

    public function testLooseRejectsAnOverflowingString(): void
    {
        $this->assertTrue((new Finite(loose: true))->isValid('1e300'));
        $this->assertFalse((new Finite(loose: true))->isValid('1e400'));
        $this->assertFalse((new Finite(loose: true))->isValid('-1e400'));
    }

    private function filter(): Filter
    {
        return new Filter(
            attributes: [
                new Document(['$id' => 'f', 'key' => 'f', 'type' => ColumnType::Double->value, 'array' => false]),
                new Document(['$id' => 'fa', 'key' => 'fa', 'type' => ColumnType::Double->value, 'array' => true]),
            ],
            idAttributeType: ColumnType::Integer->value,
        );
    }
}

<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;

class QueryShapeAggregateTest extends TestCase
{
    use QueryShapeAttributes;

    /**
     * @return array<string, array{0: string}>
     */
    public static function numericMethodProvider(): array
    {
        return [
            'sum' => ['sum'],
            'avg' => ['avg'],
            'stddev' => ['stddev'],
            'stddevPop' => ['stddevPop'],
            'stddevSamp' => ['stddevSamp'],
            'variance' => ['variance'],
            'varPop' => ['varPop'],
            'varSamp' => ['varSamp'],
            'bitAnd' => ['bitAnd'],
            'bitOr' => ['bitOr'],
            'bitXor' => ['bitXor'],
        ];
    }

    private function aggregate(string $method, string $attribute): Query
    {
        return match ($method) {
            'sum' => Query::sum($attribute, 'result'),
            'avg' => Query::avg($attribute, 'result'),
            'stddev' => Query::stddev($attribute, 'result'),
            'stddevPop' => Query::stddevPop($attribute, 'result'),
            'stddevSamp' => Query::stddevSamp($attribute, 'result'),
            'variance' => Query::variance($attribute, 'result'),
            'varPop' => Query::varPop($attribute, 'result'),
            'varSamp' => Query::varSamp($attribute, 'result'),
            'bitAnd' => Query::bitAnd($attribute, 'result'),
            'bitOr' => Query::bitOr($attribute, 'result'),
            'bitXor' => Query::bitXor($attribute, 'result'),
            'count' => Query::count($attribute, 'result'),
            'countDistinct' => Query::countDistinct($attribute, 'result'),
            'min' => Query::min($attribute, 'result'),
            'max' => Query::max($attribute, 'result'),
            default => throw new \InvalidArgumentException('Unknown aggregate: '.$method),
        };
    }

    #[DataProvider('numericMethodProvider')]
    public function testNumericAggregatesRequireANumericAttributeThatIsNotAnArray(string $method): void
    {
        $validator = $this->validator();

        foreach (['name', 'active', 'created', 'tags', 'scores', '$id', '$sequence', '$createdAt'] as $attribute) {
            $this->assertFalse($validator->isValid([$this->aggregate($method, $attribute)]), $method.' on '.$attribute.' must be rejected');
            $this->assertStringStartsWith('Invalid query: Aggregate '.$method.' requires ', $validator->getDescription());
            $this->assertStringEndsWith(' attribute that is not an array: '.$attribute, $validator->getDescription());
        }

        foreach (['price', 'stock'] as $attribute) {
            $this->assertTrue($validator->isValid([$this->aggregate($method, $attribute)]), $validator->getDescription());
        }
    }

    public function testBitwiseAggregatesRequireAnIntegerAttribute(): void
    {
        $validator = $this->validator();

        foreach (['bitAnd', 'bitOr', 'bitXor'] as $method) {
            $this->assertFalse($validator->isValid([$this->aggregate($method, 'rating')]));
            $this->assertSame('Invalid query: Aggregate '.$method.' requires an integer attribute that is not an array: rating', $validator->getDescription());
        }

        foreach (['sum', 'avg', 'stddev', 'varSamp'] as $method) {
            $this->assertTrue($validator->isValid([$this->aggregate($method, 'rating')]), $validator->getDescription());
        }
    }

    public function testJoinedAttributesAreTypedByTheirOwnCollection(): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid([
            Query::leftJoin('reviews', '$id', 'product', '=', 'review'),
            Query::sum('review.score', 'total'),
            Query::bitAnd('review.flags', 'bits'),
        ]), $validator->getDescription());
    }

    public function testCountAndExtremaKeepAcceptingEveryAttribute(): void
    {
        $validator = $this->validator();

        foreach (['count', 'countDistinct', 'min', 'max'] as $method) {
            foreach (['name', 'active', 'created', 'rating', '$id'] as $attribute) {
                $this->assertTrue($validator->isValid([$this->aggregate($method, $attribute)]), $validator->getDescription());
            }
        }
    }

    public function testOnlyCountAggregatesEveryRow(): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid([Query::count('*', 'rows')]), $validator->getDescription());

        foreach (['countDistinct', 'min', 'max', 'sum', 'avg', 'bitOr'] as $method) {
            $this->assertFalse($validator->isValid([$this->aggregate($method, '*')]), $method.' of "*" must be rejected');
            $this->assertSame('Invalid query: Only count can aggregate "*"', $validator->getDescription());
        }
    }
}

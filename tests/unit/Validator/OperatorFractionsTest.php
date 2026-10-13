<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Operator as OperatorValidator;

final class OperatorFractionsTest extends TestCase
{
    /**
     * @return iterable<string, array{Operator}>
     */
    public static function fractionalOperators(): iterable
    {
        yield 'fractional increment' => [Operator::increment(1.5)];
        yield 'fractional text decrement' => [Operator::decrement('0.5')];
        yield 'whole increment with a fractional limit' => [Operator::increment(2, 13.9)];
        yield 'fractional increment with a fractional limit' => [Operator::increment(1.5, 13.5)];
        yield 'fractional multiply' => [Operator::multiply(2.5)];
        yield 'fractional divide with a fractional limit' => [Operator::divide(1.5, 5.5)];
        yield 'fractional modulo' => [Operator::modulo(2.75)];
        yield 'fractional power' => [Operator::power(0.5)];
    }

    #[DataProvider('fractionalOperators')]
    public function testAFractionalAmountOrLimitOnAnIntegerIsAcceptedWithTheFlag(Operator $operator): void
    {
        $validator = $this->validator(fractions: true);

        $this->assertTrue($validator->isValid($operator->setAttribute('count')), $validator->getDescription());
    }

    #[DataProvider('fractionalOperators')]
    public function testAFractionalAmountOrLimitOnAnIntegerIsRefusedWithoutTheFlag(Operator $operator): void
    {
        $this->assertFalse($this->validator()->isValid($operator->setAttribute('count')));
    }

    public function testAFractionOutsideTheIntegerRangeIsRefused(): void
    {
        $validator = $this->validator(fractions: true);

        $this->assertFalse($validator->isValid(Operator::increment(2147483647.5)->setAttribute('count')));
        $this->assertSame('Cannot apply increment operator: value must be numeric, got double', $validator->getDescription());

        $this->assertFalse($validator->isValid(Operator::increment(1, -2147483648.5)->setAttribute('count')));
        $this->assertSame('Cannot apply increment operator: max/min limit must be between -2147483648 and 2147483647', $validator->getDescription());
    }

    public function testAFractionIsLeftToTheCallerToResolveAgainstTheStoredValue(): void
    {
        $validator = $this->validator(new Document(['count' => 2147483647]), fractions: true);

        $this->assertTrue($validator->isValid(Operator::increment(1.5)->setAttribute('count')));
        $this->assertFalse($validator->isValid(Operator::increment(1)->setAttribute('count')), 'A whole amount is still checked against the stored value');
    }

    public function testTheFlagLeavesOtherOperandsAsTheyAre(): void
    {
        $validator = $this->validator(fractions: true);

        $this->assertFalse($validator->isValid(Operator::increment(\INF)->setAttribute('count')));
        $this->assertFalse($validator->isValid(Operator::increment('ten')->setAttribute('count')));
        $this->assertFalse($validator->isValid(Operator::arrayAppend([1.5])->setAttribute('scores')));
        $this->assertTrue($validator->isValid(Operator::increment(1.5, 10.25)->setAttribute('ratio')));
    }

    private function validator(?Document $current = null, bool $fractions = false): OperatorValidator
    {
        return new OperatorValidator(new Document([
            Document::ID => 'operands',
            Document::COLLECTION => Database::METADATA,
            'name' => 'operands',
            'attributes' => [
                Attribute::integer(key: 'count')->toDocument(),
                Attribute::integer(key: 'scores', array: true)->toDocument(),
                Attribute::double(key: 'ratio')->toDocument(),
            ],
            'indexes' => [],
        ]), $current, fractions: $fractions);
    }
}

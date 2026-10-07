<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Join;

final class JoinAliasTest extends TestCase
{
    #[DataProvider('acceptedJoins')]
    public function testAcceptsAlias(Query $join): void
    {
        $validator = new Join();

        $this->assertTrue($validator->isValid($join), $validator->getDescription());
    }

    /**
     * @return iterable<string, array{Query}>
     */
    public static function acceptedJoins(): iterable
    {
        yield 'no alias' => [Query::join('orders', 'j0', [Query::on('$id', 'customerId')])];
        yield 'an identifier' => [Query::join('orders', 'ord', [Query::on('$id', 'customerId')])];
        yield 'an identifier with digits and underscores' => [Query::leftJoin('orders', '_order_2', [Query::on('$id', 'customerId')])];
        yield 'a cross join alias' => [Query::crossJoin('orders', 'ord')];
        yield 'a nested join alias' => [Query::rightJoin('orders', 'ord', [Query::on('$id', 'customerId')])];
    }

    #[DataProvider('rejectedJoins')]
    public function testRejectsAlias(Query $join, string $message): void
    {
        $validator = new Join();

        $this->assertFalse($validator->isValid($join));
        $this->assertSame($message, $validator->getDescription());
    }

    /**
     * @return iterable<string, array{Query, string}>
     */
    public static function rejectedJoins(): iterable
    {
        $main = Query::DEFAULT_ALIAS;
        $upper = \strtoupper(Query::DEFAULT_ALIAS);
        $invalid = 'Join alias must start with a letter or an underscore and contain only letters, digits and underscores';

        yield 'the main collection alias' => [Query::join('orders', $main, [Query::on('$id', 'customerId')]), "Join alias \"{$main}\" is reserved for the main collection"];
        yield 'the main collection alias in upper case' => [Query::join('orders', $upper, [Query::on('$id', 'customerId')]), "Join alias \"{$upper}\" is reserved for the main collection"];
        yield 'the main collection alias on a cross join' => [Query::crossJoin('orders', $main), "Join alias \"{$main}\" is reserved for the main collection"];
        yield 'the main collection alias on a nested join' => [Query::fullOuterJoin('orders', $main, [Query::on('$id', 'customerId')]), "Join alias \"{$main}\" is reserved for the main collection"];
        yield 'a hyphen' => [Query::join('orders', 'my-alias', [Query::on('$id', 'customerId')]), $invalid];
        yield 'a leading digit' => [Query::leftJoin('orders', '1st', [Query::on('$id', 'customerId')]), $invalid];
        yield 'a dot' => [Query::join('orders', 'a.b', [Query::on('$id', 'customerId')]), $invalid];
        yield 'a quote' => [Query::crossJoin('orders', 'x`y'), $invalid];
    }
}

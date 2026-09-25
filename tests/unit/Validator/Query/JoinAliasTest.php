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
        yield 'no alias' => [Query::join('orders', '$id', 'customerId')];
        yield 'an identifier' => [Query::join('orders', '$id', 'customerId', '=', 'ord')];
        yield 'an identifier with digits and underscores' => [Query::leftJoin('orders', '$id', 'customerId', '=', '_order_2')];
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

        yield 'the main collection alias' => [Query::join('orders', '$id', 'customerId', '=', $main), "Join alias \"{$main}\" is reserved for the main collection"];
        yield 'the main collection alias in upper case' => [Query::join('orders', '$id', 'customerId', '=', $upper), "Join alias \"{$upper}\" is reserved for the main collection"];
        yield 'the main collection alias on a cross join' => [Query::crossJoin('orders', $main), "Join alias \"{$main}\" is reserved for the main collection"];
        yield 'the main collection alias on a nested join' => [Query::fullOuterJoin('orders', $main, [Query::on('$id', 'customerId')]), "Join alias \"{$main}\" is reserved for the main collection"];
        yield 'a hyphen' => [Query::join('orders', '$id', 'customerId', '=', 'my-alias'), $invalid];
        yield 'a leading digit' => [Query::leftJoin('orders', '$id', 'customerId', '=', '1st'), $invalid];
        yield 'a dot' => [Query::join('orders', '$id', 'customerId', '=', 'a.b'), $invalid];
        yield 'a quote' => [Query::crossJoin('orders', 'x`y'), $invalid];
    }
}

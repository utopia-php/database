<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\RelationshipType;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Joined\Collection as JoinedCollection;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

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
        yield 'a short identifier with a digit' => [Query::join('orders', 'j0', [Query::on('$id', 'customerId')])];
        yield 'an identifier' => [Query::join('orders', 'ord', [Query::on('$id', 'customerId')])];
        yield 'an identifier with digits and underscores' => [Query::leftJoin('orders', '_order_2', [Query::on('$id', 'customerId')])];
        yield 'a cross join alias' => [Query::crossJoin('orders', 'ord')];
        yield 'a nested join alias' => [Query::rightJoin('orders', 'ord', [Query::on('$id', 'customerId')])];
        yield 'no alias' => [Query::join('orders', '', [Query::on('$id', 'customerId')])];
        yield 'no alias on a left join' => [Query::leftJoin('orders', '', [Query::on('$id', 'orders.customerId')])];
        yield 'no alias on a cross join' => [Query::crossJoin('orders', '')];
        yield 'no alias on a join built without one' => [new Query(Method::Join, 'orders', [Query::on('$id', 'customerId')])];
        yield 'no alias on a join of a table named like the main collection' => [Query::join('main', '', [Query::on('$id', 'main.customerId')])];
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

    #[DataProvider('rejectedTableIds')]
    public function testRejectsATableIdThatCannotBeTheAlias(Query $join, string $message): void
    {
        $validator = new Join();

        $this->assertTrue($join->isAliasDefaulted());
        $this->assertFalse($validator->isValid($join));
        $this->assertSame($message, $validator->getDescription());
    }

    /**
     * @return iterable<string, array{Query, string}>
     */
    public static function rejectedTableIds(): iterable
    {
        $main = Query::DEFAULT_ALIAS;
        $upper = \strtoupper(Query::DEFAULT_ALIAS);
        $pattern = static fn (string $table): string => "Table ID \"{$table}\" cannot be used as the join alias: an alias must start with a letter or an underscore and contain only letters, digits and underscores. Pass an alias for the join";
        $reserved = static fn (string $table): string => "Table ID \"{$table}\" cannot be used as the join alias: it is reserved for the main collection. Pass an alias for the join";

        yield 'a hyphen' => [Query::join('my-orders', '', [Query::on('$id', 'customerId')]), $pattern('my-orders')];
        yield 'a dot' => [Query::leftJoin('orders.v2', '', [Query::on('$id', 'customerId')]), $pattern('orders.v2')];
        yield 'a leading digit' => [Query::rightJoin('2024orders', '', [Query::on('$id', 'customerId')]), $pattern('2024orders')];
        yield 'a hyphen on a cross join' => [Query::crossJoin('my-orders', ''), $pattern('my-orders')];
        yield 'a hyphen on a join built without an alias' => [new Query(Method::FullOuterJoin, 'my-orders', [Query::on('$id', 'customerId')]), $pattern('my-orders')];
        yield 'a hyphen on a parsed join' => [Query::parseQuery(['method' => 'join', 'attribute' => 'my-orders', 'values' => ['$id', '=', 'customerId']]), $pattern('my-orders')];
        yield 'the main collection alias' => [Query::join($main, '', [Query::on('$id', 'customerId')]), $reserved($main)];
        yield 'the main collection alias in upper case' => [Query::crossJoin($upper, ''), $reserved($upper)];
    }

    public function testRejectsATableIdThatIsARelationshipKey(): void
    {
        $validator = new Join([
            new Document(['$id' => 'customerId', 'key' => 'customerId', 'type' => ColumnType::String->value]),
            new Document(['$id' => 'customers', 'key' => 'customers', 'type' => ColumnType::Relationship->value, 'options' => ['relationType' => RelationshipType::ManyToOne->value, 'side' => 'parent', 'relatedCollection' => 'customers']]),
        ]);

        $this->assertFalse($validator->isValid(Query::join('customers', '', [Query::on('customerId', '$id')])));
        $this->assertSame('Table ID "customers" cannot be used as the join alias: it is the key of the relationship attribute "customers". Pass an alias for the join', $validator->getDescription());

        $validator->resetJoinAliases();
        $this->assertFalse($validator->isValid(Query::join('people', 'customers', [Query::on('customerId', '$id')])));
        $this->assertSame('Join alias "customers" is the key of the relationship attribute "customers": give the join another alias', $validator->getDescription(), 'an explicit alias keeps its message');

        $validator->resetJoinAliases();
        $this->assertTrue($validator->isValid(Query::join('customers', 'buyer', [Query::on('customerId', '$id')])), $validator->getDescription());
    }

    /**
     * The main collection is read under the reserved alias, never under its own ID, so a self-join's
     * defaulted alias names only the joined copy: its columns are qualified by it on the right, and a
     * left column qualified by it is not the main collection's.
     */
    public function testASelfJoinWithoutAnAliasIsReadUnderTheTableId(): void
    {
        $columns = [new Document(['$id' => 'managerId', 'key' => 'managerId', 'type' => ColumnType::String->value])];
        $validator = new Join($columns);
        $validator->allowJoins([JoinedCollection::of('staff', new Document(['$id' => 'staff', 'attributes' => $columns]))]);

        $this->assertTrue($validator->isValid(Query::join('staff', '', [Query::on('managerId', '$id')])), $validator->getDescription());

        $validator->resetJoinAliases();
        $validator->allowJoins([JoinedCollection::of('staff', new Document(['$id' => 'staff', 'attributes' => $columns]))]);
        $this->assertTrue($validator->isValid(Query::join('staff', '', [Query::on('$id', 'staff.managerId')])), $validator->getDescription());

        $validator->resetJoinAliases();
        $this->assertFalse($validator->isValid(Query::join('staff', '', [Query::on('staff.managerId', '$id')])));
        $this->assertSame('The left column of a join condition must belong to the main collection or to a join declared before it: staff.managerId', $validator->getDescription());
    }

    public function testAnEmptyAliasIsStillDescribedAsRequired(): void
    {
        $this->assertSame('Join alias is required', Join::describeInvalidAlias(''));
        $this->assertSame('Join alias is required', Join::describeInvalidAlias('', true));
    }

    public function testDescribesADuplicateAliasByHowItWasGiven(): void
    {
        $this->assertSame('Join alias "c" is declared more than once', Join::describeDuplicateAlias('c'));
        $this->assertSame('Table ID "customers" cannot be used as the join alias: another join of the query already uses it as its alias. Pass an alias for the join', Join::describeDuplicateAlias('customers', true));
    }
}

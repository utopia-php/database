<?php

namespace Tests\Unit\Validator;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Queries\Document as DocumentQueries;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Order;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Schema\ColumnType;

/**
 * An `alias.column` reference is valid exactly when the column would be valid unaliased on the
 * collection the alias joins, for the same query type.
 */
final class JoinedColumnTest extends TestCase
{
    /**
     * @return array<string, array{0: string, 1: Closure(string): list<Query>}>
     */
    public static function internalAttributeProvider(): array
    {
        $types = [
            'filter' => static fn (string $attribute): array => [Query::isNotNull($attribute)],
            'select' => static fn (string $attribute): array => [Query::select([$attribute])],
            'order' => static fn (string $attribute): array => [Query::orderAsc($attribute)],
            'aggregate' => static fn (string $attribute): array => [Query::count($attribute, 'result')],
            'groupBy' => static fn (string $attribute): array => [Query::count('*', 'rows'), Query::groupBy([$attribute])],
        ];

        $cases = [];
        foreach (['$id', '$sequence', '$createdAt', '$updatedAt', '$permissions', '$tenant'] as $attribute) {
            foreach ($types as $type => $queries) {
                $cases[$attribute.' in '.$type] = [$attribute, $queries];
            }
        }

        return $cases;
    }

    /**
     * @param  Closure(string): list<Query>  $queries
     */
    #[DataProvider('internalAttributeProvider')]
    public function testInternalAttributeUnderAnAliasFollowsTheRuleOfTheMainCollection(string $attribute, Closure $queries): void
    {
        $validator = $this->validator([$this->notes()]);

        $unaliased = $validator->isValid($queries($attribute));
        $aliased = $validator->isValid([self::join(), ...$queries('note.'.$attribute)]);

        $this->assertSame($unaliased, $aliased, $validator->getDescription());
    }

    public function testCollectionOfAJoinedRowCannotBeReferenced(): void
    {
        $validator = $this->validator([$this->notes()]);

        $this->assertTrue($validator->isValid([Query::select(['$collection'])]), $validator->getDescription());

        foreach ([
            [Query::select(['note.$collection'])],
            [Query::count('note.$collection', 'result')],
            [Query::count('*', 'rows'), Query::groupBy(['note.$collection'])],
        ] as $queries) {
            $this->assertFalse($validator->isValid([self::join(), ...$queries]));
            $this->assertSame('Invalid query: Attribute not found in schema: note.$collection', $validator->getDescription());
        }
    }

    /**
     * @return array<string, array{0: Closure(string): Query}>
     */
    public static function everyQueryTypeProvider(): array
    {
        return [
            'filter' => [static fn (string $attribute): Query => Query::equal($attribute, ['x'])],
            'filter in a logical group' => [static fn (string $attribute): Query => Query::or([Query::equal('name', ['x']), Query::equal($attribute, ['x'])])],
            'select' => [static fn (string $attribute): Query => Query::select(['name', $attribute])],
            'order' => [static fn (string $attribute): Query => Query::orderDesc($attribute)],
            'aggregate' => [static fn (string $attribute): Query => Query::countDistinct($attribute, 'result')],
            'groupBy' => [static fn (string $attribute): Query => Query::groupBy(['name', $attribute])],
        ];
    }

    /**
     * @param  Closure(string): Query  $query
     */
    #[DataProvider('everyQueryTypeProvider')]
    public function testColumnTheJoinedCollectionDoesNotDeclareIsRejected(Closure $query): void
    {
        $validator = $this->validator([$this->notes()]);

        $this->assertTrue($validator->isValid([self::join(), $query('note.body')]), $validator->getDescription());

        $this->assertFalse($validator->isValid([self::join(), $query('note.visits')]), 'an attribute of the main collection is not a column of the join');
        $this->assertSame('Invalid query: Attribute not found in schema: note.visits', $validator->getDescription());

        $this->assertFalse($validator->isValid([self::join(), $query('note.customer')]), 'a relationship has no column a join reads');
        $this->assertSame('Invalid query: Attribute not found in schema: note.customer', $validator->getDescription());
    }

    /**
     * @param  Closure(string): Query  $query
     */
    #[DataProvider('everyQueryTypeProvider')]
    public function testAnyPlainColumnIsAcceptedUnderAnAliasWhoseCollectionIsUnknown(Closure $query): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid([self::join(), $query('note.anything')]), $validator->getDescription());
    }

    /**
     * @param  Closure(string): Query  $query
     */
    #[DataProvider('everyQueryTypeProvider')]
    public function testSchemalessValidatorsAcceptAnyJoinedColumn(Closure $query): void
    {
        $validator = $this->validator([$this->notes()], supportForAttributes: false);

        $this->assertTrue($validator->isValid([self::join(), $query('note.anything')]), $validator->getDescription());
    }

    public function testEachAliasResolvesToItsOwnCollection(): void
    {
        $validator = $this->validator([$this->notes(), $this->orders()]);
        $joins = [self::join(), Query::join('orders', '$id', 'customerId', '=', 'purchase')];

        $this->assertTrue($validator->isValid([...$joins, Query::equal('purchase.amount', [1]), Query::equal('note.body', ['x'])]), $validator->getDescription());

        $this->assertFalse($validator->isValid([...$joins, Query::equal('purchase.body', ['x'])]));
        $this->assertSame('Invalid query: Attribute not found in schema: purchase.body', $validator->getDescription());
    }

    public function testCollectionJoinedTwiceIsCheckedUnderBothAliases(): void
    {
        $validator = $this->validator([$this->notes()]);
        $joins = [self::join(), Query::leftJoin('notes', '$id', 'customerId', '=', 'again')];

        $this->assertTrue($validator->isValid([...$joins, Query::select(['note.body', 'again.body'])]), $validator->getDescription());

        $this->assertFalse($validator->isValid([...$joins, Query::select(['again.nothing'])]));
        $this->assertSame('Invalid query: Attribute not found in schema: again.nothing', $validator->getDescription());
    }

    public function testJoinConditionIsCheckedAgainstTheJoinedCollection(): void
    {
        $validator = $this->validator([$this->notes()]);

        $this->assertTrue($validator->isValid([
            Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId'), Query::equal('note.body', ['x'])]),
        ]), $validator->getDescription());

        $this->assertFalse($validator->isValid([
            Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId'), Query::equal('note.nothing', ['x'])]),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: note.nothing', $validator->getDescription());
    }

    public function testDocumentQueriesCheckJoinConditionsAgainstTheJoinedCollection(): void
    {
        $validator = new DocumentQueries($this->attributes());
        $validator->setJoinedCollections([$this->notes()]);

        $this->assertTrue($validator->isValid([
            Query::leftJoin('notes', 'note', [
                Query::on('$id', 'customerId'),
                Query::or([Query::equal('note.body', ['x']), Query::equal('note.$id', ['y'])]),
                Query::greaterThan('$sequence', 1),
            ]),
            Query::select(['name', 'note.body', 'note.$permissions']),
        ]), $validator->getDescription());

        foreach ([
            'an unknown joined column' => [Query::equal('note.nothing', ['x']), 'Invalid query: Attribute not found in schema: note.nothing'],
            'the joined permissions' => [Query::equal('note.$permissions', ['x']), 'Invalid query: Attribute not found in schema: note.$permissions'],
            'a value the main attribute cannot hold' => [Query::equal('visits', ['many']), 'Invalid query: Query value is invalid for attribute "visits"'],
            'an unknown column in a logical group' => [Query::or([Query::equal('note.body', ['x']), Query::equal('note.nothing', ['y'])]), 'Invalid query: Attribute not found in schema: note.nothing'],
        ] as $label => [$condition, $message]) {
            $this->assertFalse($validator->isValid([
                Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId'), $condition]),
            ]), $label);
            $this->assertSame($message, $validator->getDescription(), $label);
        }

        $this->assertFalse($validator->isValid([
            Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId')]),
            Query::select(['note.nothing']),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: note.nothing', $validator->getDescription());
    }

    public function testDocumentQueriesKeepRejectingTopLevelFilters(): void
    {
        $validator = new DocumentQueries($this->attributes());
        $validator->setJoinedCollections([$this->notes()]);

        $this->assertFalse($validator->isValid([
            Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId')]),
            Query::equal('note.body', ['x']),
        ]));
        $this->assertSame('Invalid query method: equal', $validator->getDescription());
    }

    private static function join(): Query
    {
        return Query::join('notes', '$id', 'customerId', '=', 'note');
    }

    /**
     * @param  array<Document>  $joinedCollections
     */
    private function validator(array $joinedCollections = [], bool $supportForAttributes = true): Queries
    {
        $attributes = [
            ...$this->attributes(),
            $this->attribute('$id', ColumnType::String),
            $this->attribute('$sequence', ColumnType::Id),
            $this->attribute('$createdAt', ColumnType::Datetime),
            $this->attribute('$updatedAt', ColumnType::Datetime),
        ];

        $validator = new Queries([
            new Filter($attributes, ColumnType::Integer->value, supportForAttributes: $supportForAttributes),
            new Select($attributes, $supportForAttributes),
            new Order($attributes, $supportForAttributes),
            new Join(),
            new Aggregate($attributes, $supportForAttributes),
            new GroupBy($attributes, $supportForAttributes),
        ]);
        $validator->setJoinedCollections($joinedCollections);

        return $validator;
    }

    /**
     * @return array<Document>
     */
    private function attributes(): array
    {
        return [
            $this->attribute('name', ColumnType::String),
            $this->attribute('visits', ColumnType::Integer),
        ];
    }

    private function notes(): Document
    {
        return $this->collection('notes', [
            $this->attribute('customerId', ColumnType::String),
            $this->attribute('body', ColumnType::String),
            $this->attribute('customer', ColumnType::Relationship),
        ]);
    }

    private function orders(): Document
    {
        return $this->collection('orders', [
            $this->attribute('customerId', ColumnType::String),
            $this->attribute('amount', ColumnType::Integer),
        ]);
    }

    /**
     * @param  array<Document>  $attributes
     */
    private function collection(string $id, array $attributes): Document
    {
        return new Document([
            '$id' => $id,
            'attributes' => $attributes,
            'indexes' => [],
        ]);
    }

    private function attribute(string $key, ColumnType $type): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => $type->value,
            'size' => $type === ColumnType::String ? 256 : 0,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);
    }
}

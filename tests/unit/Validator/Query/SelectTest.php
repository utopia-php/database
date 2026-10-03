<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

class SelectTest extends TestCase
{
    protected Select $validator;

    /**
     * @throws Exception
     */
    protected function setUp(): void
    {
        $this->validator = new Select(
            attributes: [
                new Document([
                    '$id' => 'attr',
                    'key' => 'attr',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'artist',
                    'key' => 'artist',
                    'type' => ColumnType::Relationship->value,
                    'array' => false,
                ]),
            ],
        );
    }

    public function test_value_success(): void
    {
        $this->assertTrue($this->validator->isValid(Query::select(['*', 'attr'])));
        $this->assertTrue($this->validator->isValid(Query::select(['artist.name'])));
    }

    public function test_value_failure(): void
    {
        $this->assertFalse($this->validator->isValid(Query::limit(1)));
        $this->assertEquals('Invalid query', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid(Query::select(['name.artist'])));
    }

    /**
     * A non-string selection used to reach str_contains() and raise a TypeError,
     * which is an Error rather than an Exception, so it escaped every catch on the
     * way out and surfaced as a 500. Select was the only query method that answered
     * a malformed value that way.
     *
     * @param array<mixed> $values
     */
    #[DataProvider('nonStringSelections')]
    public function testANonStringSelectionIsRefusedByType(array $values, string $expected): void
    {
        $this->assertFalse($this->validator->isValid(new Query(Method::Select, values: $values)));
        $this->assertSame($expected, $this->validator->getDescription());
    }

    /**
     * @return array<string, array{array<mixed>, string}>
     */
    public static function nonStringSelections(): array
    {
        return [
            'nested array' => [[['attr']], 'Attribute selection must be a string, got array'],
            'nested wildcard' => [[['*']], 'Attribute selection must be a string, got array'],
            'mixed flat and nested' => [['attr', ['x']], 'Attribute selection must be a string, got array'],
            'assoc array' => [[['a' => 1]], 'Attribute selection must be a string, got array'],
            'integer' => [[1], 'Attribute selection must be a string, got int'],
            'null' => [[null], 'Attribute selection must be a string, got null'],
        ];
    }

    /**
     * Two nested values used to collapse to one under array_unique(), which casts
     * every array to the string "Array", so the duplicate check tripped first and
     * reported a duplicate that was not there. The type check has to run before it.
     *
     * Parsed from JSON rather than built with Query::select(), because that is the
     * path a hand-written HTTP client takes and the only one that can carry a value
     * the constructor's array<string> type would reject.
     */
    public function testTwoNestedSelectionsReportTheTypeNotAFalseDuplicate(): void
    {
        $query = Query::parse('{"method":"select","values":[["a"],["b"]]}');

        $this->assertFalse($this->validator->isValid($query));
        $this->assertSame('Attribute selection must be a string, got array', $this->validator->getDescription());
    }

    public function testTheLegitimateFlatFormStillPasses(): void
    {
        $this->assertTrue($this->validator->isValid(Query::select(['attr'])));
        $this->assertTrue($this->validator->isValid(Query::select(['*'])));
        $this->assertTrue($this->validator->isValid(Query::select(['$id', '$createdAt'])));
        $this->assertTrue($this->validator->isValid(Query::select(['artist.name'])));
    }

    public function testDottedJoinAliasIsAcceptedAfterAllowJoinAliases(): void
    {
        $this->validator->allowJoinAliases(['ord']);

        $this->assertTrue($this->validator->isValid(Query::select(['ord.amount'])));
        $this->assertTrue($this->validator->isValid(Query::select(['ord.$id'])));
    }

    public function testUnqualifiedAmountIsStillRejected(): void
    {
        $this->validator->allowJoinAliases(['ord']);

        $this->assertFalse($this->validator->isValid(Query::select(['amount'])));
        $this->assertSame('Attribute not found in schema: amount', $this->validator->getDescription());
    }
}

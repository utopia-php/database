<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

/**
 * A query parsed from JSON takes a JSON object for its values as readily as a list. The validator read such values
 * by position and raised an undefined key warning; it refuses them as invalid instead.
 */
final class FilterMalformedValuesTest extends TestCase
{
    private function validator(bool $supportForAttributes = true): Filter
    {
        return new Filter(
            attributes: [
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value, 'array' => false]),
                new Document(['$id' => 'tags', 'key' => 'tags', 'type' => ColumnType::String->value, 'array' => true]),
                new Document(['$id' => 'location', 'key' => 'location', 'type' => ColumnType::Point->value, 'array' => false]),
                new Document(['$id' => 'embedding', 'key' => 'embedding', 'type' => ColumnType::Vector->value, 'array' => false, 'size' => 2]),
            ],
            idAttributeType: ColumnType::Integer->value,
            supportForAttributes: $supportForAttributes,
        );
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function objectValues(): iterable
    {
        yield 'equal' => ['{"method":"equal","attribute":"name","values":{"value":"a"}}'];
        yield 'contains' => ['{"method":"contains","attribute":"tags","values":{"tag":"a"}}'];
        yield 'notEqual' => ['{"method":"notEqual","attribute":"name","values":{"value":"a"}}'];
        yield 'regex' => ['{"method":"regex","attribute":"name","values":{"pattern":"^a"}}'];
        yield 'between' => ['{"method":"between","attribute":"name","values":{"from":"a","to":"b"}}'];
        yield 'exists' => ['{"method":"exists","attribute":"","values":{"attribute":"name"}}'];
        yield 'intersects' => ['{"method":"intersects","attribute":"location","values":{"type":"Point","coordinates":[1,2]}}'];
        yield 'distanceLessThan' => ['{"method":"distanceLessThan","attribute":"location","values":{"geometry":[[1,2],10,false]}}'];
        yield 'vectorDot' => ['{"method":"vectorDot","attribute":"embedding","values":{"vector":[1,2]}}'];
    }

    #[DataProvider('objectValues')]
    public function testValuesGivenAsAnObjectAreRefused(string $json): void
    {
        $validator = $this->validator();
        $query = Query::parse($json);

        $this->assertFalse($validator->isValid($query));
        $this->assertSame(\ucfirst($query->getMethod()->value).' query values must be a list', $validator->getDescription());
    }

    public function testADistanceTupleGivenAsAnObjectIsRefused(): void
    {
        $validator = $this->validator();
        $query = Query::parse('{"method":"distanceLessThan","attribute":"location","values":[{"geometry":[1,2],"distance":10,"meters":false}]}');

        $this->assertFalse($validator->isValid($query));
        $this->assertSame('Distance query requires [[geometry, distance]] parameters', $validator->getDescription());
    }

    public function testADistanceWithAMetersFlagThatIsNotABooleanIsRefused(): void
    {
        $validator = $this->validator();

        $this->assertFalse($validator->isValid(new Query(Method::DistanceLessThan, 'location', [[[1, 2], 10, ['yes']]])));
        $this->assertSame('Distance query requires a boolean meters flag', $validator->getDescription());
        $this->assertTrue($validator->isValid(Query::distanceLessThan('location', [1, 2], 10, true)));
    }

    public function testAVectorGivenAsAnObjectIsRefused(): void
    {
        $validator = $this->validator();

        $this->assertFalse($validator->isValid(Query::vectorDot('embedding', ['x' => 1.0, 'y' => 2.0])));
        $this->assertSame('Vector query value must be an array', $validator->getDescription());
        $this->assertTrue($validator->isValid(Query::vectorDot('embedding', [1.0, 2.0])));
    }

    public function testAVectorQueryOnAnUndeclaredAttributeIsRefusedWithoutAWarning(): void
    {
        $validator = $this->validator(supportForAttributes: false);

        $this->assertFalse($validator->isValid(Query::vectorDot('undeclared', [1.0, 2.0])));
        $this->assertSame('Vector queries can only be used on vector attributes', $validator->getDescription());
    }

    public function testListValuesAreStillValid(): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid(Query::equal('name', ['a', 'b'])));
        $this->assertTrue($validator->isValid(Query::contains('tags', ['a'])));
        $this->assertTrue($validator->isValid(Query::intersects('location', [1, 2])));
    }
}

<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\Profiles;
use Utopia\Database\Capability;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Base;
use Utopia\Database\Validator\Queries\Documents;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\UID;
use Utopia\Query\Schema\ColumnType;

/**
 * The query validation messages 7.x returned, which API clients see in their 400 bodies.
 */
class QueriesCompatibilityTest extends TestCase
{
    private function documents(bool $sharedTables = false): Documents
    {
        return new Documents(
            [
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value, 'size' => 128, 'array' => false]),
                new Document(['$id' => 'age', 'key' => 'age', 'type' => ColumnType::Integer->value, 'size' => 4, 'array' => false]),
                new Document(['$id' => 'tags', 'key' => 'tags', 'type' => ColumnType::String->value, 'size' => 64, 'array' => true]),
            ],
            [],
            Profiles::of(capabilities: [Capability::DefinedAttributes, Capability::OrderRandom], sharedTables: $sharedTables),
        );
    }

    public function test_nested_child_is_validated_before_its_parent(): void
    {
        $validator = $this->documents();

        $this->assertFalse($validator->isValid(['{"method":"and","values":[{"method":"equal","attribute":"nope","values":["a"]}]}']));
        $this->assertSame('Invalid query: Attribute not found in schema: nope', $validator->getDescription());

        $this->assertFalse($validator->isValid(['{"method":"or","values":[{"method":"equal","attribute":"nope","values":["a"]},{"method":"limit","values":[1]}]}']));
        $this->assertSame('Invalid query: Attribute not found in schema: nope', $validator->getDescription());

        $this->assertFalse($validator->isValid(['{"method":"elemMatch","attribute":"tags","values":[{"method":"equal","attribute":"x","values":["a"]}]}']));
        $this->assertSame('Invalid query: Attribute not found in schema: x', $validator->getDescription());
    }

    public function test_first_invalid_query_wins_over_a_later_parse_failure(): void
    {
        $validator = $this->documents();

        $this->assertFalse($validator->isValid(['{"method":"equal","attribute":"nope","values":["a"]}', 'not json']));
        $this->assertSame('Invalid query: Attribute not found in schema: nope', $validator->getDescription());

        $this->assertFalse($validator->isValid(['not json', '{"method":"equal","attribute":"nope","values":["a"]}']));
        $this->assertSame('Invalid query: Invalid query: Syntax error', $validator->getDescription());
    }

    public function test_method_unknown_to_7x_keeps_the_parse_prefix(): void
    {
        $validator = new Base([new Limit(), new Offset(), new Cursor()]);

        $this->assertFalse($validator->isValid(['{"method":"count","attribute":"name","values":[]}']));
        $this->assertSame('Invalid query: Invalid query method: count', $validator->getDescription());

        $this->assertFalse($validator->isValid(['{"method":"jsonContains","attribute":"name","values":["a"]}']));
        $this->assertSame('Invalid query: Invalid query method: jsonContains', $validator->getDescription());

        $this->assertFalse($validator->isValid(['{"method":"and","values":[{"method":"equal","attribute":"name","values":["a"]},{"method":"equal","attribute":"name","values":["b"]}]}']));
        $this->assertSame('Invalid query method: equal', $validator->getDescription());
    }

    public function test_raw_is_an_invalid_method(): void
    {
        $validator = $this->documents();

        $this->assertFalse($validator->isValid(['{"method":"raw","values":["1=1"]}']));
        $this->assertSame('Invalid query: Invalid query method: raw', $validator->getDescription());

        try {
            Query::parse('{"method":"or","values":[{"method":"raw","values":["1=1"]},{"method":"equal","attribute":"name","values":["a"]}]}');
            $this->fail('A nested raw query parsed');
        } catch (QueryException $e) {
            $this->assertSame('Invalid query method: raw', $e->getMessage());
        }
    }

    public function test_tenant_passes_select_validation_without_shared_tables(): void
    {
        $this->assertTrue($this->documents()->isValid(['{"method":"select","values":["$tenant","name"]}']));
        $this->assertTrue($this->documents(sharedTables: true)->isValid(['{"method":"select","values":["$tenant"]}']));
    }

    public function test_uid_description(): void
    {
        $this->assertSame(
            'UID must contain at most 36 chars. Valid chars are a-z, A-Z, 0-9, and underscore. Can\'t start with a leading underscore',
            (new UID())->getDescription(),
        );
    }
}

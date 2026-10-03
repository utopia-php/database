<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Capability;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

class DocumentsValidatorGrammarTest extends TestCase
{
    private Document $orders;

    protected function setUp(): void
    {
        $this->orders = new Document([
            '$id' => 'orders',
            'attributes' => [
                new Document([
                    '$id' => 'amount',
                    'key' => 'amount',
                    'type' => ColumnType::Integer->value,
                    'size' => 0,
                    'required' => false,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ]),
            ],
            'indexes' => [],
        ]);
    }

    public function testAdaptersWithoutJoinsOrAggregationsKeepTheFilterGrammar(): void
    {
        $validator = (new DocumentsValidatorDatabase(new Memory(), new Cache(new None())))->documentsValidator($this->orders);

        $this->assertFalse($validator->isValid([Query::join('customers', '$id', 'customerId')]));
        $this->assertSame('Invalid query method: join', $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::sum('amount', 'total')]));
        $this->assertSame('Invalid query method: sum', $validator->getDescription());
    }

    public function testAdaptersWithJoinsAndAggregationsAcceptThem(): void
    {
        $validator = (new DocumentsValidatorDatabase(new SQLite(new PDO('sqlite::memory:')), new Cache(new None())))->documentsValidator($this->orders);

        $this->assertTrue($validator->isValid([Query::join('customers', '$id', 'customerId')]), $validator->getDescription());
        $this->assertTrue($validator->isValid([Query::sum('amount', 'total')]), $validator->getDescription());
    }

    public function testCapabilitiesArePartOfTheCacheKey(): void
    {
        $adapter = new class () extends Memory {
            /**
             * @var array<string, true>
             */
            public array $enabled = [];

            public function supports(Capability $feature): bool
            {
                return isset($this->enabled[$feature->name]) || parent::supports($feature);
            }
        };
        $database = new DocumentsValidatorDatabase($adapter, new Cache(new None()));
        $queries = [Query::join('customers', '$id', 'customerId'), Query::sum('amount', 'total')];

        $this->assertFalse($database->documentsValidator($this->orders)->isValid($queries));

        $adapter->enabled[Capability::Joins->name] = true;
        $adapter->enabled[Capability::Aggregations->name] = true;

        $validator = $database->documentsValidator($this->orders);
        $this->assertTrue($validator->isValid($queries), $validator->getDescription());
    }
}

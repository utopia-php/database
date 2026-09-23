<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;
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
        $validator = $this->documentsValidator(new Database(new Memory(), new Cache(new None())));

        $this->assertFalse($validator->isValid([Query::join('customers', '$id', 'customerId')]));
        $this->assertSame('Invalid query method: join', $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::sum('amount', 'total')]));
        $this->assertSame('Invalid query method: sum', $validator->getDescription());
    }

    public function testAdaptersWithJoinsAndAggregationsAcceptThem(): void
    {
        $validator = $this->documentsValidator(new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None())));

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
        $database = new Database($adapter, new Cache(new None()));
        $queries = [Query::join('customers', '$id', 'customerId'), Query::sum('amount', 'total')];

        $this->assertFalse($this->documentsValidator($database)->isValid($queries));

        $adapter->enabled[Capability::Joins->name] = true;
        $adapter->enabled[Capability::Aggregations->name] = true;

        $validator = $this->documentsValidator($database);
        $this->assertTrue($validator->isValid($queries), $validator->getDescription());
    }

    private function documentsValidator(Database $database): DocumentsValidator
    {
        $validator = (new ReflectionMethod($database, 'getDocumentsValidator'))->invoke($database, $this->orders);
        $this->assertInstanceOf(DocumentsValidator::class, $validator);

        return $validator;
    }
}

<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

class DocumentsValidatorGrammarTest extends TestCase
{
    private Document $orders;

    #[\Override]
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

        $this->assertFalse($validator->isValid([Query::join('customers', 'j0', [Query::on('$id', 'customerId')])]));
        $this->assertSame('Invalid query method: join', $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::sum('amount', 'total')]));
        $this->assertSame('Invalid query method: sum', $validator->getDescription());
    }

    public function testAdaptersWithJoinsAndAggregationsAcceptThem(): void
    {
        $validator = (new DocumentsValidatorDatabase(new SQLite(new PDO('sqlite::memory:')), new Cache(new None())))->documentsValidator($this->orders);

        $this->assertTrue($validator->isValid([Query::join('customers', 'j0', [Query::on('$id', 'customerId')])]), $validator->getDescription());
        $this->assertTrue($validator->isValid([Query::sum('amount', 'total')]), $validator->getDescription());
    }

    public function testTheCachedValidatorFollowsTheProfile(): void
    {
        $database = new DocumentsValidatorDatabase(new Memory(), new Cache(new None()));
        $queries = [Query::select(['$tenant'])];

        $this->assertFalse($database->documentsValidator($this->orders)->isValid($queries));

        $database->setSharedTables(true);

        $validator = $database->documentsValidator($this->orders);
        $this->assertTrue($validator->isValid($queries), $validator->getDescription());
    }
}

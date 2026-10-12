<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

final class MongoArrayNotContainsTest extends TestCase
{
    private const string COLLECTION = 'tags';

    /**
     * @var list<array<mixed>>
     */
    private array $filters = [];

    public function testNotContainsOnAnArrayExcludesMissingAndNullArrays(): void
    {
        $query = Query::notContains('labels', ['a', 'c']);
        $query->setOnArray(true);

        $this->createAdapter()->find(new Document(['$id' => self::COLLECTION]), [$query]);

        $this->assertSame(
            [['labels' => ['$nin' => ['a', 'c'], '$ne' => null]]],
            $this->filters[0]['$and'] ?? null,
            'A document whose array is missing or null must not match notContains, as on MariaDB, MySQL and PostgreSQL',
        );
    }

    private function createAdapter(): Mongo
    {
        $record = function (array $filters): void {
            $this->filters[] = $filters;
        };

        $client = new class ($record) extends Client {
            /**
             * @param  Closure(array<mixed>): void  $record
             */
            public function __construct(private readonly Closure $record)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                ($this->record)($filters);

                return (object) ['cursor' => (object) ['firstBatch' => [], 'id' => 0]];
            }
        };

        $authorization = new Authorization();
        $authorization->disable();

        $adapter = new Mongo($client);
        $adapter->setAuthorization($authorization);
        $adapter->setNamespace('array_not_contains');

        return $adapter;
    }
}

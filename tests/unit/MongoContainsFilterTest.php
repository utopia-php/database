<?php

namespace Tests\Unit;

use Closure;
use MongoDB\BSON\Regex;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

final class MongoContainsFilterTest extends TestCase
{
    private const string COLLECTION = 'movies';

    /**
     * @var list<array<mixed>>
     */
    private array $filters = [];

    public function testContainsOnAnArrayMatchesAnyValue(): void
    {
        $contains = Query::contains('genres', ['comics', 'kids']);
        $contains->setOnArray(true);
        $containsAny = Query::containsAny('genres', ['comics', 'kids']);
        $containsAny->setOnArray(true);

        $this->assertSame(['genres' => ['$in' => ['comics', 'kids']]], $this->filterOf($contains));
        $this->assertSame(['genres' => ['$in' => ['comics', 'kids']]], $this->filterOf($containsAny));
    }

    public function testNotEqualToSeveralValuesExcludesEachOfThem(): void
    {
        $this->assertSame(['name' => ['$nin' => ['a', 'b']]], $this->filterOf(Query::notEqual('name', ['a', 'b'])));
    }

    public function testDollarPrefixedAttributeIsFilteredByItsStoredName(): void
    {
        $this->assertSame(['_meta__dot__key' => ['$eq' => 'x']], $this->filterOf(Query::equal('$meta.key', ['x'])));
    }

    public function testSchemalessUnparsableDateStaysAString(): void
    {
        $adapter = $this->createAdapter();
        $adapter->setSchemaless(true);

        $this->assertSame(
            ['when' => ['$eq' => '2026-13-45T99:99:99Z']],
            $this->filterOf(Query::equal('when', ['2026-13-45T99:99:99Z']), $adapter),
        );
    }

    #[RequiresPhpExtension('mongodb')]
    public function testContainsOnAStringMatchesAnyOfSeveralSubstrings(): void
    {
        foreach ([Query::contains('name', ['Captain', 'Work']), Query::containsAny('name', ['Captain', 'Work'])] as $query) {
            $this->filters = [];
            $this->createAdapter()->find(new Document(['$id' => self::COLLECTION]), [$query]);

            $alternatives = $this->recordedCondition()['$or'] ?? null;
            $this->assertIsArray($alternatives);
            $this->assertSame(['.*Captain.*/i', '.*Work.*/i'], \array_map(
                static function (mixed $alternative): string {
                    $regex = \is_array($alternative) && \is_array($alternative['name'] ?? null) ? ($alternative['name']['$regex'] ?? null) : null;

                    return $regex instanceof Regex ? $regex->getPattern().'/'.$regex->getFlags() : '';
                },
                $alternatives,
            ));
        }
    }

    #[RequiresPhpExtension('mongodb')]
    public function testNotContainsOnAStringExcludesTheSubstring(): void
    {
        $filter = $this->filterOf(Query::notContains('name', ['Captain']));

        $regex = \is_array($filter['name'] ?? null) ? ($filter['name']['$not'] ?? null) : null;
        $this->assertInstanceOf(Regex::class, $regex);
        $this->assertSame('.*Captain.*', $regex->getPattern());
        $this->assertSame('i', $regex->getFlags());
    }

    /**
     * @return array<mixed>
     */
    private function filterOf(Query $query, ?Mongo $adapter = null): array
    {
        $this->filters = [];
        ($adapter ?? $this->createAdapter())->find(new Document(['$id' => self::COLLECTION]), [$query]);

        $filter = $this->recordedCondition();
        $this->assertIsArray($filter);

        return $filter;
    }

    /**
     * @return array<mixed>|null
     */
    private function recordedCondition(): ?array
    {
        $filter = $this->filters[0] ?? null;
        $conditions = \is_array($filter) ? ($filter['$and'] ?? null) : null;
        $condition = \is_array($conditions) ? ($conditions[0] ?? null) : null;

        return \is_array($condition) ? $condition : null;
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
        $adapter->setNamespace('contains_filter');

        return $adapter;
    }
}

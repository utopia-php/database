<?php

namespace Tests\Unit;

use Closure;
use MongoDB\BSON\Regex;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

#[RequiresPhpExtension('mongodb')]
final class MongoDollarWordRegexTest extends TestCase
{
    private const string COLLECTION = 'prices';

    /**
     * @var list<array<mixed>>
     */
    private array $filters = [];

    /**
     * @return array<string, array{Query, string, string}>
     */
    public static function regexQueries(): array
    {
        return [
            'contains' => [Query::contains('label', ['$USD']), '$regex', '.*\$USD.*'],
            'notContains' => [Query::notContains('label', ['$USD']), '$not', '.*\$USD.*'],
            'notSearch' => [Query::notSearch('label', '$USD'), '$not', '.*\$USD.*'],
            'notStartsWith' => [Query::notStartsWith('label', '$USD'), '$not', '^\$USD'],
            'notEndsWith' => [Query::notEndsWith('label', '$USD'), '$not', '\$USD$'],
            'metacharacters' => [Query::contains('label', ['a.b($x']), '$regex', '.*a\.b\(\$x.*'],
        ];
    }

    #[DataProvider('regexQueries')]
    public function testADollarWordIsMatchedLiterally(Query $query, string $operator, string $pattern): void
    {
        $this->createAdapter()->find(new Document(['$id' => self::COLLECTION]), [$query]);

        $recorded = $this->filters[0] ?? null;
        $conditions = \is_array($recorded) ? ($recorded['$and'] ?? null) : null;
        $condition = \is_array($conditions) ? ($conditions[0] ?? null) : null;
        $filter = \is_array($condition) ? ($condition['label'] ?? null) : null;
        $this->assertIsArray($filter);
        $regex = $filter[$operator] ?? null;
        $this->assertInstanceOf(Regex::class, $regex);
        $this->assertSame($pattern, $regex->getPattern());
        $this->assertSame('i', $regex->getFlags());
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
        $adapter->setNamespace('dollar_word_regex');

        return $adapter;
    }
}

<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\TestCase;
use stdClass;
use Stringable;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Storage;
use Utopia\Mongo\Client;

final class MongoSequencesTest extends TestCase
{
    private const string COLLECTION = 'orders';

    /**
     * @var list<stdClass>
     */
    private array $rows = [];

    /**
     * @var list<array<mixed>>
     */
    private array $filters = [];

    public function testSameIdUnderTwoTenantsKeepsEachTenantsSequence(): void
    {
        $adapter = $this->createAdapter(sharedTables: true, tenant: null);
        $this->rows = [
            $this->row('shared', 'sequence-one', tenant: 1),
            $this->row('shared', 'sequence-two', tenant: 2),
        ];

        [$one, $two] = $adapter->getSequences(self::COLLECTION, [
            new Document(['$id' => 'shared', '$tenant' => 1]),
            new Document(['$id' => 'shared', '$tenant' => 2]),
        ]);

        $this->assertSame('sequence-one', $one->getSequence());
        $this->assertSame('sequence-two', $two->getSequence());
    }

    public function testDocumentWithoutTenantResolvesUnderTheAdapterTenant(): void
    {
        $adapter = $this->createAdapter(sharedTables: true, tenant: 3);
        $this->rows = [
            $this->row('first', 'sequence-first', tenant: 3),
            $this->row('second', 'sequence-second', tenant: 5),
        ];

        [$first, $second] = $adapter->getSequences(self::COLLECTION, [
            new Document(['$id' => 'first']),
            new Document(['$id' => 'second', '$tenant' => 5]),
        ]);

        $this->assertSame('sequence-first', $first->getSequence());
        $this->assertSame('sequence-second', $second->getSequence());
    }

    public function testRowOfAnotherTenantIsNotMatched(): void
    {
        $adapter = $this->createAdapter(sharedTables: true, tenant: 1);
        $this->rows = [
            $this->row('shared', 'sequence-other', tenant: 2),
        ];

        [$document] = $adapter->getSequences(self::COLLECTION, [
            new Document(['$id' => 'shared', '$tenant' => 1]),
        ]);

        $this->assertNull($document->getSequence());
    }

    public function testGeneratedObjectIdIsReadAsItsString(): void
    {
        $adapter = $this->createAdapter(sharedTables: false, tenant: null);
        $objectId = new class () implements Stringable {
            public function __toString(): string
            {
                return '6553f1c2a4b8e3d2f0c1a9b7';
            }
        };
        $this->rows = [
            $this->row('generated', $objectId, tenant: null),
        ];

        [$document] = $adapter->getSequences(self::COLLECTION, [
            new Document(['$id' => 'generated']),
        ]);

        $this->assertSame('6553f1c2a4b8e3d2f0c1a9b7', $document->getSequence());
    }

    public function testDocumentsWithSequencesAreNotLookedUp(): void
    {
        $adapter = $this->createAdapter(sharedTables: false, tenant: null);

        [$document] = $adapter->getSequences(self::COLLECTION, [
            new Document(['$id' => 'known', '$sequence' => 'sequence-known']),
        ]);

        $this->assertSame('sequence-known', $document->getSequence());
        $this->assertSame([], $this->filters);
    }

    private function row(string $id, string|Stringable $sequence, ?int $tenant): stdClass
    {
        $row = new stdClass();
        $row->{Storage::UID} = $id;
        $row->{Storage::SEQUENCE} = $sequence;
        $row->{Storage::TENANT} = $tenant;
        $row->name = 'unprojected';

        return $row;
    }

    private function createAdapter(bool $sharedTables, ?int $tenant): Mongo
    {
        $rows = fn (): array => $this->rows;
        $record = function (array $filters): void {
            $this->filters[] = $filters;
        };

        $client = new class ($rows, $record) extends Client {
            /**
             * @param  Closure(): list<stdClass>  $rows
             * @param  Closure(array<mixed>): void  $record
             */
            public function __construct(
                private readonly Closure $rows,
                private readonly Closure $record,
            ) {
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

                $projection = \is_array($options['projection'] ?? null) ? $options['projection'] : null;
                $batch = [];
                foreach (($this->rows)() as $row) {
                    if (! $this->matches($row, $filters)) {
                        continue;
                    }

                    $fields = (array) $row;
                    if ($projection !== null) {
                        $fields = \array_intersect_key($fields, $projection + [Storage::SEQUENCE => 1]);
                    }
                    $batch[] = (object) $fields;
                }

                return (object) ['cursor' => (object) ['firstBatch' => $batch, 'id' => 0]];
            }

            /**
             * @param  array<mixed>  $filters
             */
            private function matches(stdClass $row, array $filters): bool
            {
                foreach ($filters as $field => $condition) {
                    $candidates = \is_array($condition) && \is_array($condition['$in'] ?? null)
                        ? $condition['$in']
                        : [$condition];

                    if (! \in_array($row->{$field} ?? null, $candidates, true)) {
                        return false;
                    }
                }

                return true;
            }
        };

        $adapter = new Mongo($client);
        $adapter->setNamespace('sequences');
        $adapter->setSharedTables($sharedTables);
        $adapter->setTenant($tenant);

        return $adapter;
    }
}

<?php

namespace Tests\Unit;

use ArrayObject;
use LogicException;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Storage;
use Utopia\Mongo\Client;

/**
 * Under skipDuplicates() MongoDB returns only the documents it inserted, so the database counts
 * and emits the same documents on every adapter.
 */
final class MongoSkipDuplicatesTest extends TestCase
{
    private const string COLLECTION = 'orders';

    public function testOnlyDocumentsWithANewIdAreWrittenAndReturned(): void
    {
        /** @var ArrayObject<int, list<string>> $upserted */
        $upserted = new ArrayObject();
        $adapter = $this->createAdapter([$this->row('stored', 'sequence-stored', tenant: null)], $upserted, sharedTables: false);

        $created = $adapter->skipDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'stored', 'name' => 'replayed']),
            new Document(['$id' => 'fresh', 'name' => 'first']),
            new Document(['$id' => 'fresh', 'name' => 'second']),
        ]));

        $this->assertSame(['fresh'], \array_map(static fn (Document $document): string => $document->getId(), $created));
        $this->assertSame('first', $created[0]->getAttribute('name'));
        $this->assertSame([['fresh']], $upserted->getArrayCopy());
    }

    public function testAnIdStoredUnderAnotherTenantIsNew(): void
    {
        /** @var ArrayObject<int, list<string>> $upserted */
        $upserted = new ArrayObject();
        $adapter = $this->createAdapter([$this->row('shared', 'sequence-one', tenant: 1)], $upserted, sharedTables: true);

        $created = $adapter->skipDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'shared', '$tenant' => 1, 'name' => 'replayed']),
            new Document(['$id' => 'shared', '$tenant' => 2, 'name' => 'new']),
        ]));

        $this->assertSame([2], \array_map(static fn (Document $document): int|string|null => $document->getTenant(), $created));
        $this->assertSame([['shared']], $upserted->getArrayCopy());
    }

    public function testAnIdDifferingOnlyInCaseIsStored(): void
    {
        /** @var ArrayObject<int, list<string>> $upserted */
        $upserted = new ArrayObject();
        $adapter = $this->createAdapter([$this->row('Stored', 'sequence-stored', tenant: null)], $upserted, sharedTables: false);

        $created = $adapter->skipDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'stored', 'name' => 'replayed']),
            new Document(['$id' => 'Fresh', 'name' => 'first']),
            new Document(['$id' => 'fresh', 'name' => 'second']),
        ]));

        $this->assertSame(['Fresh'], \array_map(static fn (Document $document): string => $document->getId(), $created));
        $this->assertSame([['Fresh']], $upserted->getArrayCopy());
    }

    public function testABatchOfStoredIdsWritesNothing(): void
    {
        /** @var ArrayObject<int, list<string>> $upserted */
        $upserted = new ArrayObject();
        $adapter = $this->createAdapter([$this->row('stored', 'sequence-stored', tenant: null)], $upserted, sharedTables: false);

        $created = $adapter->skipDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'stored', 'name' => 'replayed']),
        ]));

        $this->assertSame([], $created);
        $this->assertSame([], $upserted->getArrayCopy());
    }

    private function row(string $id, string $sequence, ?int $tenant): stdClass
    {
        $row = new stdClass();
        $row->{Storage::UID} = $id;
        $row->{Storage::SEQUENCE} = $sequence;
        $row->{Storage::TENANT} = $tenant;

        return $row;
    }

    /**
     * @param  list<stdClass>  $rows
     * @param  ArrayObject<int, list<string>>  $upserted
     */
    private function createAdapter(array $rows, ArrayObject $upserted, bool $sharedTables): Mongo
    {
        $client = new class ($rows, $upserted) extends Client {
            /**
             * @param  list<stdClass>  $rows
             * @param  ArrayObject<int, list<string>>  $upserted
             */
            public function __construct(
                private readonly array $rows,
                private readonly ArrayObject $upserted,
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
                $collation = $options['collation'] ?? null;
                $caseInsensitive = \is_array($collation) && ($collation['strength'] ?? null) === 1;
                $batch = [];
                foreach ($this->rows as $row) {
                    if ($this->matches($row, $filters, $caseInsensitive)) {
                        $batch[] = $row;
                    }
                }

                return (object) ['cursor' => (object) ['firstBatch' => $batch, 'id' => 0]];
            }

            /**
             * @param  array<string, mixed>  $command
             */
            #[\Override]
            public function query(array $command, ?string $db = null): int
            {
                $updates = $command['updates'] ?? null;
                if (! \is_array($updates)) {
                    throw new LogicException('An upsert command must list its updates');
                }

                $ids = [];
                foreach ($updates as $update) {
                    if (! \is_array($update)) {
                        throw new LogicException('An upsert command must hold update documents');
                    }
                    $collation = $update['collation'] ?? null;
                    if (! \is_array($collation) || ($collation['strength'] ?? null) !== 1) {
                        throw new LogicException('An upsert by id must use the _uid index collation');
                    }
                    $filter = $update['q'] ?? null;
                    $id = \is_array($filter) ? ($filter[Storage::UID] ?? null) : null;
                    if (! \is_string($id)) {
                        throw new LogicException('An upsert by id must filter on _uid');
                    }
                    $ids[] = $id;
                }
                $this->upserted->append($ids);

                return \count($ids);
            }

            /**
             * @param  array<mixed>  $filters
             */
            private function matches(stdClass $row, array $filters, bool $caseInsensitive): bool
            {
                $normalize = static fn (mixed $value): mixed => $caseInsensitive && \is_string($value) ? \strtolower($value) : $value;
                foreach ($filters as $field => $condition) {
                    $candidates = \is_array($condition) && \is_array($condition['$in'] ?? null)
                        ? $condition['$in']
                        : [$condition];

                    if (! \in_array($normalize($row->{$field} ?? null), \array_map($normalize, $candidates), true)) {
                        return false;
                    }
                }

                return true;
            }
        };

        $adapter = new Mongo($client);
        $adapter->setNamespace('skip_duplicates');
        $adapter->setSharedTables($sharedTables);

        return $adapter;
    }
}

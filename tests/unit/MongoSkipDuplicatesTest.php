<?php

namespace Tests\Unit;

use ArrayObject;
use Closure;
use LogicException;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Storage;
use Utopia\Mongo\Client;

/**
 * Under ignoreDuplicates() MongoDB returns only the documents it inserted, so the database counts
 * and emits the same documents on every adapter.
 */
final class MongoSkipDuplicatesTest extends TestCase
{
    private const string COLLECTION = 'orders';

    public function testOnlyDocumentsWithANewIdAreWrittenAndReturned(): void
    {
        $rows = new ArrayObject([$this->row('stored', 'sequence-stored', tenant: null)]);
        $adapter = $this->createAdapter($rows, sharedTables: false);

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'stored', 'name' => 'replayed']),
            new Document(['$id' => 'fresh', 'name' => 'first']),
            new Document(['$id' => 'fresh', 'name' => 'second']),
        ]));

        $this->assertSame(['fresh'], $this->ids($created));
        $this->assertSame('first', $created[0]->getAttribute('name'));
        $this->assertSame(['stored', 'fresh'], $this->storedIds($rows));
        $this->assertSame('first', $rows->getArrayCopy()[1]->name ?? null);
    }

    public function testAnIdStoredUnderAnotherTenantIsNew(): void
    {
        $rows = new ArrayObject([$this->row('shared', 'sequence-one', tenant: 1)]);
        $adapter = $this->createAdapter($rows, sharedTables: true);

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'shared', '$tenant' => 1, 'name' => 'replayed']),
            new Document(['$id' => 'shared', '$tenant' => 2, 'name' => 'new']),
        ]));

        $this->assertSame([2], \array_map(static fn (Document $document): int|string|null => $document->getTenant(), $created));
        $this->assertSame(['shared', 'shared'], $this->storedIds($rows));
    }

    public function testAnIdDifferingOnlyInCaseIsStored(): void
    {
        $rows = new ArrayObject([$this->row('Stored', 'sequence-stored', tenant: null)]);
        $adapter = $this->createAdapter($rows, sharedTables: false);

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'stored', 'name' => 'replayed']),
            new Document(['$id' => 'Fresh', 'name' => 'first']),
            new Document(['$id' => 'fresh', 'name' => 'second']),
        ]));

        $this->assertSame(['Fresh'], $this->ids($created));
        $this->assertSame(['Stored', 'Fresh'], $this->storedIds($rows));
    }

    public function testAnIdTheIdCollationMatchesIsNotReportedAsCreated(): void
    {
        $rows = new ArrayObject([$this->row('resume', 'sequence-stored', tenant: null)]);
        $adapter = $this->createAdapter($rows, sharedTables: false);

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'résumé', 'name' => 'replayed']),
        ]));

        $this->assertSame([], $this->ids($created), 'The _uid index collation folds accents, so the upsert matched the stored document and inserted nothing');
        $this->assertSame(['resume'], $this->storedIds($rows));
    }

    public function testADocumentAnotherWriterStoresFirstIsNotReportedAsCreated(): void
    {
        $rows = new ArrayObject();
        $adapter = $this->createAdapter($rows, sharedTables: false, beforeUpdate: function () use ($rows): void {
            $rows->append($this->row('raced', 'sequence-other-writer', tenant: null));
        });

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'raced', 'name' => 'late']),
            new Document(['$id' => 'fresh', 'name' => 'new']),
        ]));

        $this->assertSame(['fresh'], $this->ids($created), 'A document another writer stored before the upsert ran was matched, not inserted');
        $this->assertSame(['raced', 'fresh'], $this->storedIds($rows));
    }

    public function testAReplayedSequenceIsNotReportedAsCreated(): void
    {
        $rows = new ArrayObject([$this->row('stored', 'sequence-stored', tenant: null)]);
        $adapter = $this->createAdapter($rows, sharedTables: false);

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'stored', '$sequence' => 'sequence-stored', 'name' => 'replayed']),
            new Document(['$id' => 'moved', '$sequence' => 'sequence-new', 'name' => 'new']),
        ]));

        $this->assertSame(['moved'], $this->ids($created));
        $this->assertSame(['stored', 'moved'], $this->storedIds($rows));
    }

    public function testABatchOfStoredIdsWritesNothing(): void
    {
        $rows = new ArrayObject([$this->row('stored', 'sequence-stored', tenant: null)]);
        $adapter = $this->createAdapter($rows, sharedTables: false);

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document(['$id' => self::COLLECTION]), [
            new Document(['$id' => 'stored', 'name' => 'replayed']),
        ]));

        $this->assertSame([], $created);
        $this->assertSame(['stored'], $this->storedIds($rows));
        $this->assertFalse(isset($rows[0]->name), 'A skipped document leaves the stored one untouched');
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
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }

    /**
     * @param  ArrayObject<int, stdClass>  $rows
     * @return list<string>
     */
    private function storedIds(ArrayObject $rows): array
    {
        return \array_values(\array_map(static function (stdClass $row): string {
            $id = $row->{Storage::UID};
            self::assertIsString($id);

            return $id;
        }, $rows->getArrayCopy()));
    }

    /**
     * A client that applies an upsert as MongoDB does: a statement whose filter matches a stored document under the
     * `_uid` collation (case and accents folded) changes nothing, any other inserts its `$setOnInsert` document.
     *
     * @param  ArrayObject<int, stdClass>  $rows
     */
    private function createAdapter(ArrayObject $rows, bool $sharedTables, ?Closure $beforeUpdate = null): Mongo
    {
        $client = new class ($rows, $beforeUpdate) extends Client {
            /**
             * @param  ArrayObject<int, stdClass>  $rows
             */
            public function __construct(
                private readonly ArrayObject $rows,
                private readonly ?Closure $beforeUpdate,
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
                $folded = \is_array($collation) && ($collation['strength'] ?? null) === 1;
                $batch = [];
                foreach ($this->rows as $row) {
                    if ($this->matches($row, $filters, $folded)) {
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

                if ($this->beforeUpdate !== null) {
                    ($this->beforeUpdate)();
                }

                foreach ($updates as $update) {
                    if (! \is_array($update) || ! \is_array($update['q'] ?? null) || ! ($update['u'] ?? null) instanceof stdClass) {
                        throw new LogicException('An upsert command must hold update documents');
                    }
                    $collation = $update['collation'] ?? null;
                    if (! \is_array($collation) || ($collation['strength'] ?? null) !== 1) {
                        throw new LogicException('An upsert by id must use the _uid index collation');
                    }
                    if (! \is_string($update['q'][Storage::UID] ?? null)) {
                        throw new LogicException('An upsert by id must filter on _uid');
                    }

                    foreach ($this->rows as $row) {
                        if ($this->matches($row, $update['q'], true)) {
                            continue 2;
                        }
                    }

                    $row = new stdClass();
                    foreach ($update['q'] as $field => $value) {
                        $row->{$field} = $value;
                    }
                    foreach ((array) $update['u']->{'$setOnInsert'} as $field => $value) {
                        $row->{$field} = $value;
                    }
                    $this->rows->append($row);
                }

                return \count($updates);
            }

            /**
             * @param  array<mixed>  $filters
             */
            private function matches(stdClass $row, array $filters, bool $folded): bool
            {
                $normalize = static fn (mixed $value): mixed => $folded && \is_string($value)
                    ? \strtolower(\strtr($value, ['é' => 'e', 'É' => 'E']))
                    : $value;
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

<?php

namespace Tests\Unit;

use ArrayObject;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

/**
 * How the Mongo adapter pages through a cursor: it follows getMore while the server returns a cursor id, and treats
 * a reply without one as the last batch.
 */
final class MongoCursorTest extends TestCase
{
    private const string COLLECTION = 'orders';

    public function testSequencesStopPagingAtABatchWithoutACursorId(): void
    {
        /** @var ArrayObject<int, string> $calls */
        $calls = new ArrayObject();
        $adapter = new Mongo($this->client(
            $calls,
            firstBatch: [self::row('first', 'sequence-first')],
            nextBatches: [[self::row('second', 'sequence-second')], [self::row('third', 'sequence-third')]],
        ));
        $adapter->setNamespace('cursor');

        [$first, $second, $third] = $adapter->getSequences(self::COLLECTION, [
            new Document(['$id' => 'first']),
            new Document(['$id' => 'second']),
            new Document(['$id' => 'third']),
        ]);

        $this->assertSame('sequence-first', $first->getSequence());
        $this->assertSame('sequence-second', $second->getSequence());
        $this->assertNull($third->getSequence(), 'a batch without a cursor id is the last one');
        $this->assertSame(['getMore'], $calls->getArrayCopy());
    }

    public function testAFindKeepsItsResultsWhenTheOpenCursorCannotBeKilled(): void
    {
        /** @var ArrayObject<int, string> $calls */
        $calls = new ArrayObject();
        $adapter = new Mongo($this->client($calls, firstBatch: [self::row('first', 'sequence-first')], nextBatches: [[]]));
        $adapter->setNamespace('cursor');
        $adapter->setAuthorization(new Authorization());

        $documents = $adapter->find(new Document([Document::ID => self::COLLECTION]));

        $this->assertSame(['first'], \array_map(static fn (Document $document): string => $document->getId(), $documents));
        $this->assertSame(['getMore', 'killCursors'], $calls->getArrayCopy(), 'the cursor left open by an empty batch is killed, and the failure to kill it is ignored');
    }

    private static function row(string $id, string $sequence): stdClass
    {
        return (object) [Storage::UID => $id, Storage::SEQUENCE => $sequence];
    }

    /**
     * A client whose find opens a cursor over $firstBatch and whose getMore hands out $nextBatches in turn, each in a
     * reply without a cursor id. Every getMore and killCursors is recorded in $calls; killCursors fails.
     *
     * @param  ArrayObject<int, string>  $calls
     * @param  list<stdClass>  $firstBatch
     * @param  list<list<stdClass>>  $nextBatches
     */
    private function client(ArrayObject $calls, array $firstBatch, array $nextBatches): Client
    {
        return new class ($calls, $firstBatch, $nextBatches) extends Client {
            /**
             * @param  ArrayObject<int, string>  $calls
             * @param  list<stdClass>  $firstBatch
             * @param  list<list<stdClass>>  $nextBatches
             */
            public function __construct(private readonly ArrayObject $calls, private readonly array $firstBatch, private readonly array $nextBatches)
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
                return (object) ['cursor' => (object) ['firstBatch' => $this->firstBatch, 'id' => 7]];
            }

            #[\Override]
            public function getMore(int $cursorId, string $collection, int $batchSize = 25): stdClass
            {
                $batch = $this->nextBatches[\count($this->calls)] ?? [];
                $this->calls[] = 'getMore';

                return (object) ['cursor' => (object) ['nextBatch' => $batch]];
            }

            /**
             * @param  array<mixed>  $command
             */
            #[\Override]
            public function query(array $command, ?string $db = null): stdClass
            {
                $this->calls[] = \array_key_first($command) === 'killCursors' ? 'killCursors' : 'query';

                throw new RuntimeException('the cursor could not be killed');
            }
        };
    }
}

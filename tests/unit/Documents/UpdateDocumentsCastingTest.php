<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator;

final class UpdateDocumentsCastingTest extends TestCase
{
    public function testCastsUpdatesBeforeEveryBatchWrite(): void
    {
        $adapter = new class () extends Memory implements Feature\InternalCasting {
            /** @var array<int, true> */
            private array $casted = [];

            /** @var array<bool> */
            public array $receivedCastedUpdates = [];

            /** @var array<int, string> */
            public array $receivedUpdatedAtTypes = [];

            /** @var array<int, string|null> */
            public array $receivedUpdatedAtValues = [];

            /** @var array<int, int> */
            public array $receivedUpdateIds = [];

            public function castingBefore(Document $collection, Document $document): Document
            {
                $this->casted[\spl_object_id($document)] = true;

                $value = $document->getAttribute('value');
                if ($value instanceof Operator) {
                    $value->setValues([2]);
                    if ($document->getId() === '') {
                        $document->setAttribute('$updatedAt', '2000-01-01 00:00:00.000');
                    }
                }

                return $document;
            }

            public function castingAfter(Document $collection, Document $document): Document
            {
                return $document;
            }

            #[\Override]
            public function updateDocuments(Document $collection, Document $updates, array $documents): int
            {
                $this->receivedCastedUpdates[] = isset($this->casted[\spl_object_id($updates)]);
                $this->receivedUpdatedAtTypes[] = \get_debug_type($updates->getUpdatedAt());
                $this->receivedUpdatedAtValues[] = $updates->getUpdatedAt();
                $this->receivedUpdateIds[] = \spl_object_id($updates);

                return parent::updateDocuments($collection, $updates, $documents);
            }
        };
        $database = new Database($adapter, new Cache(new None()));
        $database->getAuthorization()->disable();
        $database->setNamespace('casting');
        $this->assertTrue($database->create());
        $database->createCollection('counters', [
            Attribute::integer(key: 'value'),
        ]);
        $database->createDocuments('counters', [
            new Document(['$id' => 'first', 'value' => 1]),
            new Document(['$id' => 'second', 'value' => 2]),
        ]);

        $operator = Operator::increment('1');
        $modified = $database->updateDocuments(
            'counters',
            new Document(['value' => $operator]),
            batchSize: 1,
        );

        $this->assertSame(2, $modified);
        $this->assertSame([true, true], $adapter->receivedCastedUpdates);
        $this->assertSame(['1'], $operator->getValues());
        $this->assertSame(['string', 'string'], $adapter->receivedUpdatedAtTypes);
        $this->assertSame(
            ['2000-01-01 00:00:00.000', '2000-01-01 00:00:00.000'],
            $adapter->receivedUpdatedAtValues,
        );
        $this->assertCount(1, \array_unique($adapter->receivedUpdateIds));
        $this->assertSame(3, $database->getDocument('counters', 'first')->getAttribute('value'));
        $this->assertSame(4, $database->getDocument('counters', 'second')->getAttribute('value'));
    }
}

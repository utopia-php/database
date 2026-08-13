<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Query\Schema\ColumnType;

final class UpdateDocumentsCastingTest extends TestCase
{
    public function testCastsUpdatesBeforeEveryBatchWrite(): void
    {
        $adapter = new class () extends Memory {
            /** @var array<int, true> */
            private array $casted = [];

            /** @var array<bool> */
            public array $receivedCastedUpdates = [];

            #[\Override]
            public function castingBefore(Document $collection, Document $document): Document
            {
                $this->casted[\spl_object_id($document)] = true;

                return parent::castingBefore($collection, $document);
            }

            #[\Override]
            public function updateDocuments(Document $collection, Document $updates, array $documents): int
            {
                $this->receivedCastedUpdates[] = isset($this->casted[\spl_object_id($updates)]);

                return parent::updateDocuments($collection, $updates, $documents);
            }
        };
        $database = new Database($adapter, new Cache(new None()));
        $database->getAuthorization()->disable();
        $database->setNamespace('casting');
        $this->assertTrue($database->create());
        $database->createCollection('counters', [
            new Attribute(key: 'value', type: ColumnType::Integer),
        ]);
        $database->createDocuments('counters', [
            new Document(['$id' => 'first', 'value' => 1]),
            new Document(['$id' => 'second', 'value' => 2]),
        ]);

        $modified = $database->updateDocuments(
            'counters',
            new Document(['value' => Operator::increment(1)]),
            batchSize: 1,
        );

        $this->assertSame(2, $modified);
        $this->assertSame([true, true], $adapter->receivedCastedUpdates);
        $this->assertSame(2, $database->getDocument('counters', 'first')->getAttribute('value'));
        $this->assertSame(3, $database->getDocument('counters', 'second')->getAttribute('value'));
    }
}

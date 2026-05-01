<?php

namespace Utopia\Database\Migration\Strategy;

use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

class ExpandContract
{
    public function expand(Database $db, string $collection, Attribute $newAttribute): void
    {
        $db->createAttribute($collection, $newAttribute);
    }

    public function migrate(Database $db, string $collection, string $oldKey, string $newKey, callable $transform, int $batchSize = 100): int
    {
        $count = 0;
        $lastDocument = null;

        while (true) {
            $queries = [Query::limit($batchSize)];

            if ($lastDocument !== null) {
                $queries[] = Query::cursorAfter($lastDocument);
            }

            $documents = $db->find($collection, $queries);

            if ($documents === []) {
                break;
            }

            foreach ($documents as $doc) {
                $oldValue = $doc->getAttribute($oldKey);
                $newValue = $transform($oldValue);

                $db->updateDocument($collection, $doc->getId(), new Document([
                    '$id' => $doc->getId(),
                    $newKey => $newValue,
                ]));

                $count++;
            }

            $lastDocument = \end($documents);

            if (\count($documents) < $batchSize) {
                break;
            }
        }

        return $count;
    }

    public function contract(Database $db, string $collection, string $oldKey): void
    {
        $db->deleteAttribute($collection, $oldKey);
    }
}

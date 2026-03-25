<?php

namespace Utopia\Database\Cache;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;

class CacheInvalidator implements Lifecycle
{
    public function __construct(
        private QueryCache $queryCache,
    ) {
    }

    public function handle(Event $event, mixed $data): void
    {
        $collection = $this->extractCollection($event, $data);

        if ($collection === null) {
            return;
        }

        $writeEvents = [
            Event::DocumentCreate,
            Event::DocumentsCreate,
            Event::DocumentUpdate,
            Event::DocumentsUpdate,
            Event::DocumentsUpsert,
            Event::DocumentDelete,
            Event::DocumentsDelete,
            Event::DocumentIncrease,
            Event::DocumentDecrease,
        ];

        if (\in_array($event, $writeEvents, true)) {
            $this->queryCache->invalidateCollection($collection);
        }
    }

    private function extractCollection(Event $event, mixed $data): ?string
    {
        if ($data instanceof Document) {
            $collection = $data->getCollection();

            return $collection !== '' ? $collection : null;
        }

        if (\is_string($data) && $data !== '') {
            return $data;
        }

        return null;
    }
}

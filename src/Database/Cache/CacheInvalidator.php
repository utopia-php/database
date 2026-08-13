<?php

namespace Utopia\Database\Cache;

use Utopia\Database\Database;
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
        if (! $this->isMutation($event)) {
            return;
        }

        foreach (\array_keys($this->extractCollections($event, $data)) as $collection) {
            $this->queryCache->invalidateCollection($collection);
        }
    }

    private function isMutation(Event $event): bool
    {
        return \in_array($event, [
            Event::CollectionCreate,
            Event::CollectionUpdate,
            Event::CollectionDelete,
            Event::AttributeCreate,
            Event::AttributesCreate,
            Event::AttributeUpdate,
            Event::AttributeDelete,
            Event::IndexCreate,
            Event::IndexRename,
            Event::IndexDelete,
            Event::DocumentPurge,
            Event::DocumentCreate,
            Event::DocumentsCreate,
            Event::DocumentUpdate,
            Event::DocumentsUpdate,
            Event::DocumentsUpsert,
            Event::DocumentDelete,
            Event::DocumentsDelete,
            Event::DocumentIncrease,
            Event::DocumentDecrease,
            Event::PermissionsCreate,
            Event::PermissionsDelete,
        ], true);
    }

    /**
     * @return array<string, true>
     */
    private function extractCollections(Event $event, mixed $data): array
    {
        $collections = [];

        if (\is_array($data)) {
            foreach ($data as $item) {
                foreach ($this->extractCollections($event, $item) as $collection => $present) {
                    $collections[$collection] = $present;
                }
            }

            return $collections;
        }

        if ($data instanceof Document) {
            if (\in_array($event, [
                Event::CollectionCreate,
                Event::CollectionUpdate,
                Event::CollectionDelete,
            ], true)) {
                $collection = $data->getId();
            } else {
                $collection = $data->getCollection();
                if ($collection === Database::METADATA) {
                    $collection = $data->getId();
                }
            }

            if ($collection !== '') {
                $collections[$collection] = true;
            }

            $options = $data->getAttribute('options', []);
            if ($options instanceof Document) {
                $options = $options->getArrayCopy();
            }
            $related = \is_array($options) ? ($options['relatedCollection'] ?? null) : null;
            if (\is_string($related) && $related !== '') {
                $collections[$related] = true;
            }

            return $collections;
        }

        if (\is_string($data) && $data !== '') {
            $collections[$data] = true;
        }

        return $collections;
    }
}

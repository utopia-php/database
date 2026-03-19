<?php

namespace Utopia\Database\Event;

use Utopia\Database\Event;

class DocumentDeleted extends DomainEvent
{
    public function __construct(
        string $collection,
        public readonly string $documentId,
    ) {
        parent::__construct($collection, Event::DocumentDelete);
    }
}

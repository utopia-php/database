<?php

namespace Utopia\Database\Event;

use Utopia\Database\Document;
use Utopia\Database\Event;

class DocumentCreated extends DomainEvent
{
    public function __construct(
        string $collection,
        public readonly Document $document,
    ) {
        parent::__construct($collection, Event::DocumentCreate);
    }
}

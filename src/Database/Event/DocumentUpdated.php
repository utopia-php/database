<?php

namespace Utopia\Database\Event;

use Utopia\Database\Document;
use Utopia\Database\Event;

class DocumentUpdated extends DomainEvent
{
    public function __construct(
        string $collection,
        public readonly Document $document,
        public readonly ?Document $previous = null,
    ) {
        parent::__construct($collection, Event::DocumentUpdate);
    }
}

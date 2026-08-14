<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\DomainEvent;

class Created extends DomainEvent
{
    public function __construct(
        string $collection,
        public readonly Document $document,
    ) {
        parent::__construct($collection, Event::DocumentCreate);
    }
}

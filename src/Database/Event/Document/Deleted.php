<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Event;
use Utopia\Database\Event\DomainEvent;

class Deleted extends DomainEvent
{
    public function __construct(
        string $collection,
        public readonly string $documentId,
    ) {
        parent::__construct($collection, Event::DocumentDelete);
    }
}

<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

class Updated extends Domain
{
    public function __construct(
        string $collection,
        public readonly Document $document,
        public readonly ?Document $previous = null,
    ) {
        parent::__construct($collection, Event::DocumentUpdate);
    }
}

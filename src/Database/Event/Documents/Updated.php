<?php

namespace Utopia\Database\Event\Documents;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

class Updated extends Domain
{
    public function __construct(
        string $collection,
        public readonly int $count,
    ) {
        parent::__construct($collection, Event::DocumentsUpdate);
    }
}

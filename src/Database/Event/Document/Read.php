<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Read extends Domain
{
    public function __construct(
        public string $collection,
        public Document $document,
    ) {
        parent::__construct(Event::DocumentRead);
    }
}

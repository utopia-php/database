<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Increased extends Domain
{
    public function __construct(
        public string $collection,
        public Document $document,
        public string $attribute,
    ) {
        parent::__construct(Event::DocumentIncrease);
    }
}

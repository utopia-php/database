<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class BatchCreated extends Domain
{
    public function __construct(
        public string $collection,
        public int $count,
    ) {
        parent::__construct(Event::DocumentsCreate);
    }
}

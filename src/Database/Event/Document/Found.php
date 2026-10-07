<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Found extends Domain
{
    /**
     * @param  list<Document>  $documents
     */
    public function __construct(
        public string $collection,
        public array $documents,
    ) {
        parent::__construct(Event::DocumentFind);
    }
}

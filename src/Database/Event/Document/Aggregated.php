<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Aggregated extends Domain
{
    /**
     * @param  list<array<string, mixed>>  $rows
     */
    public function __construct(
        public string $collection,
        public array $rows,
    ) {
        parent::__construct(Event::DocumentAggregate);
    }
}

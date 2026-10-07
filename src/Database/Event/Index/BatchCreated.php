<?php

namespace Utopia\Database\Event\Index;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Index;

final readonly class BatchCreated extends Domain
{
    /**
     * @param  list<Index>  $indexes
     */
    public function __construct(
        public string $collection,
        public array $indexes,
    ) {
        parent::__construct(Event::IndexesCreate);
    }
}

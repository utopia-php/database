<?php

namespace Utopia\Database\Event\Collection;

use Utopia\Database\Collection;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Read extends Domain
{
    public function __construct(
        public string $collection,
        public Collection $definition,
    ) {
        parent::__construct(Event::CollectionRead);
    }
}

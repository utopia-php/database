<?php

namespace Utopia\Database\Event\Index;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Index;

final readonly class Created extends Domain
{
    public function __construct(
        public string $collection,
        public Index $index,
    ) {
        parent::__construct(Event::IndexCreate);
    }
}

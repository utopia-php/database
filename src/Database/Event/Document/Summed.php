<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Summed extends Domain
{
    public function __construct(
        public string $collection,
        public string $attribute,
        public int|float $sum,
    ) {
        parent::__construct(Event::DocumentSum);
    }
}

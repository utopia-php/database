<?php

namespace Utopia\Database\Event\Attribute;

use Utopia\Database\Attribute;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Updated extends Domain
{
    public function __construct(
        public string $collection,
        public Attribute $attribute,
    ) {
        parent::__construct(Event::AttributeUpdate);
    }
}

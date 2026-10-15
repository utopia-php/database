<?php

namespace Utopia\Database\Event\Attribute;

use Utopia\Database\Attribute;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Created extends Domain
{
    public function __construct(
        public string $collection,
        public Attribute $attribute,
    ) {
        parent::__construct(Event::AttributeCreate);
    }
}

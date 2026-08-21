<?php

namespace Utopia\Database\Event\Collection;

use Utopia\Database\Event;
use Utopia\Database\Event\DomainEvent;

class Deleted extends DomainEvent
{
    public function __construct(
        string $collection,
    ) {
        parent::__construct($collection, Event::CollectionDelete);
    }
}

<?php

namespace Utopia\Database\Event;

use Utopia\Database\Event;

class CollectionDeleted extends DomainEvent
{
    public function __construct(
        string $collection,
    ) {
        parent::__construct($collection, Event::CollectionDelete);
    }
}

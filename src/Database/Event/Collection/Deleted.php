<?php

namespace Utopia\Database\Event\Collection;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

class Deleted extends Domain
{
    public function __construct(
        string $collection,
    ) {
        parent::__construct($collection, Event::CollectionDelete);
    }
}

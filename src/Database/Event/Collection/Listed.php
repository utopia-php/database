<?php

namespace Utopia\Database\Event\Collection;

use Utopia\Database\Collection;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Listed extends Domain
{
    /**
     * @param  list<Collection>  $collections
     */
    public function __construct(
        public array $collections,
    ) {
        parent::__construct(Event::CollectionList);
    }
}

<?php

namespace Utopia\Database\Event\Database;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Created extends Domain
{
    public function __construct(
        public string $database,
    ) {
        parent::__construct(Event::DatabaseCreate);
    }
}

<?php

namespace Utopia\Database\Event\Database;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Updated extends Domain
{
    public function __construct(
        public string $database,
        public string $new,
    ) {
        parent::__construct(Event::DatabaseUpdate);
    }
}

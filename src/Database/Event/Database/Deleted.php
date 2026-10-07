<?php

namespace Utopia\Database\Event\Database;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Deleted extends Domain
{
    public function __construct(
        public string $database,
        public bool $deleted,
    ) {
        parent::__construct(Event::DatabaseDelete);
    }
}

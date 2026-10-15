<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Purged extends Domain
{
    public function __construct(
        public string $collection,
        public string $id,
    ) {
        parent::__construct(Event::DocumentPurge);
    }
}

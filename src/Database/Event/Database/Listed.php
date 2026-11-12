<?php

namespace Utopia\Database\Event\Database;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Listed extends Domain
{
    /**
     * @param  list<Document>  $databases
     */
    public function __construct(
        public array $databases,
    ) {
        parent::__construct(Event::DatabaseList);
    }
}

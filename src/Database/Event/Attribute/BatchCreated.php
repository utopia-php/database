<?php

namespace Utopia\Database\Event\Attribute;

use Utopia\Database\Attribute;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class BatchCreated extends Domain
{
    /**
     * @param  list<Attribute>  $attributes
     */
    public function __construct(
        public string $collection,
        public array $attributes,
    ) {
        parent::__construct(Event::AttributesCreate);
    }
}

<?php

namespace Utopia\Database\Seeder;

class FactoryDefinition
{
    /** @var callable */
    public $callback;

    public function __construct(callable $callback)
    {
        $this->callback = $callback;
    }
}

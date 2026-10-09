<?php

namespace Utopia\Database\Adapter\SQL;

final readonly class Expression
{
    /**
     * @param  list<mixed>  $bindings
     */
    public function __construct(
        public string $sql,
        public array $bindings = [],
    ) {
    }
}

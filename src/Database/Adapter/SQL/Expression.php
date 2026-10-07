<?php

namespace Utopia\Database\Adapter\SQL;

/**
 * A raw SQL fragment with `?` placeholders and the values bound to them, in placeholder order.
 */
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

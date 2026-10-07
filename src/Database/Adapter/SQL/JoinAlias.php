<?php

namespace Utopia\Database\Adapter\SQL;

/**
 * A joined collection of a read and the alias the read names it by.
 */
final readonly class JoinAlias
{
    public function __construct(
        public string $table,
        public string $alias,
    ) {
    }
}

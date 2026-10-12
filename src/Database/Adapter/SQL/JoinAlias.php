<?php

namespace Utopia\Database\Adapter\SQL;

final readonly class JoinAlias
{
    public function __construct(
        public string $table,
        public string $alias,
    ) {
    }
}

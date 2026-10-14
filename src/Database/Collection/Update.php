<?php

namespace Utopia\Database\Collection;

final readonly class Update
{
    /**
     * @param  list<string>|null  $permissions
     */
    public function __construct(
        public ?array $permissions = null,
        public ?bool $documentSecurity = null,
    ) {
    }
}

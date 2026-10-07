<?php

namespace Utopia\Database;

final readonly class CollectionUpdate
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

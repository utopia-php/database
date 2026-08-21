<?php

namespace Utopia\Database\ORM\Mapping;

#[\Attribute(\Attribute::TARGET_CLASS)]
final class Entity
{
    /**
     * @param  array<string>  $permissions
     */
    public function __construct(
        public string $collection,
        public bool $documentSecurity = true,
        public array $permissions = [],
    ) {
    }
}

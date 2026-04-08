<?php

namespace Utopia\Database\ORM\Mapping;

#[\Attribute(\Attribute::TARGET_PROPERTY)]
final class Embedded
{
    public function __construct(
        public string $type,
        public string $prefix = '',
    ) {
    }
}

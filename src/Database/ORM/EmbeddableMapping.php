<?php

namespace Utopia\Database\ORM;

class EmbeddableMapping
{
    public function __construct(
        public readonly string $propertyName,
        public readonly string $typeName,
        public readonly string $prefix,
    ) {
    }
}

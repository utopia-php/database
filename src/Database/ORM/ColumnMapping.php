<?php

namespace Utopia\Database\ORM;

use Utopia\Database\ORM\Mapping\Column;

class ColumnMapping
{
    public function __construct(
        public readonly string $propertyName,
        public readonly string $documentKey,
        public readonly Column $column,
    ) {
    }
}

<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Attribute;
use Utopia\Database\Index;

class SchemaChange
{
    public function __construct(
        public readonly SchemaChangeType $type,
        public readonly ?Attribute $attribute = null,
        public readonly ?Attribute $previousAttribute = null,
        public readonly ?Index $index = null,
        public readonly ?string $collectionId = null,
    ) {
    }
}

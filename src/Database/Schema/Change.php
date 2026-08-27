<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Attribute;
use Utopia\Database\Index;

class Change
{
    public function __construct(
        public readonly ChangeType $type,
        public readonly ?Attribute $attribute = null,
        public readonly ?Attribute $previousAttribute = null,
        public readonly ?Index $index = null,
        public readonly ?string $collectionId = null,
    ) {
    }
}

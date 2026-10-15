<?php

namespace Utopia\Database\Adapter;

use Utopia\Query\OrderDirection;

/**
 * @internal
 */
final readonly class CursorBoundary
{
    public function __construct(
        public string $field,
        public OrderDirection $direction,
        public mixed $reference,
    ) {
    }
}

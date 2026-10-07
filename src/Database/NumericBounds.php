<?php

namespace Utopia\Database;

final readonly class NumericBounds
{
    public function __construct(
        public int|float|string $min,
        public int|float|string $max,
    ) {
    }
}

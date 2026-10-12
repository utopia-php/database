<?php

namespace Tests\Unit\Attributes;

final readonly class TypeSample
{
    public function __construct(
        public mixed $value,
        public string $readType,
        public mixed $default = null,
        public bool $incrementable = false,
        public int $size = 0,
    ) {
    }
}

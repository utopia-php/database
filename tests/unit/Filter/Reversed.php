<?php

namespace Tests\Unit\Filter;

use Utopia\Database\Filter\Codec;

final readonly class Reversed implements Codec
{
    public function __construct(private string $name = 'reversed')
    {
    }

    public function name(): string
    {
        return $this->name;
    }

    public function encode(mixed $value): mixed
    {
        return \is_string($value) ? \strrev($value) : $value;
    }

    public function decode(mixed $value): mixed
    {
        return \is_string($value) ? \strrev($value) : $value;
    }
}

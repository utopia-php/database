<?php

namespace Tests\Unit\Filter;

use Utopia\Database\Filter\Codec;

final readonly class Rot13 implements Codec
{
    public function __construct(private string $name = 'rot13')
    {
    }

    public function name(): string
    {
        return $this->name;
    }

    public function encode(mixed $value): mixed
    {
        return \is_string($value) ? \str_rot13($value) : $value;
    }

    public function decode(mixed $value): mixed
    {
        return \is_string($value) ? \str_rot13($value) : $value;
    }
}

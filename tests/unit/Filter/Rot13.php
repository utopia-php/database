<?php

namespace Tests\Unit\Filter;

use Utopia\Database\Filter\Codec;

final readonly class Rot13 implements Codec
{
    public function __construct(private string $name = 'rot13')
    {
    }

    #[\Override]
    public function name(): string
    {
        return $this->name;
    }

    #[\Override]
    public function encode(mixed $value): mixed
    {
        return \is_string($value) ? \str_rot13($value) : $value;
    }

    #[\Override]
    public function decode(mixed $value): mixed
    {
        return \is_string($value) ? \str_rot13($value) : $value;
    }
}

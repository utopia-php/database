<?php

namespace Tests\Unit\Filter;

use Utopia\Database\Filter\Signed;

/**
 * A codec for {@see CodecTest} whose instances encode differently, told apart by their prefix.
 */
final readonly class Prefixed implements Signed
{
    public function __construct(private string $prefix)
    {
    }

    public function name(): string
    {
        return 'text';
    }

    public function encode(mixed $value): mixed
    {
        return \is_string($value) ? $this->prefix.$value : $value;
    }

    public function decode(mixed $value): mixed
    {
        return \is_string($value) && \str_starts_with($value, $this->prefix) ? \substr($value, \strlen($this->prefix)) : $value;
    }

    public function signature(): string
    {
        return self::class.':'.$this->prefix;
    }
}

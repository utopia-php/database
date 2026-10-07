<?php

namespace Utopia\Database\Filter;

/**
 * A filter a Database handle applies to the values of the attributes that declare it: encode() before a value is
 * stored, decode() after it is read.
 */
interface Codec
{
    public function name(): string;

    public function encode(mixed $value): mixed;

    public function decode(mixed $value): mixed;
}

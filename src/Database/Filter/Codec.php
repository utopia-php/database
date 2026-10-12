<?php

namespace Utopia\Database\Filter;

/**
 * A filter a Database handle applies to the values of the attributes that declare it: encode() before a value is
 * stored, decode() after it is read.
 *
 * Cached documents and query results are keyed by the class of each codec, so instances of one class must encode
 * alike; a codec whose instances differ implements {@see Signed} to be keyed by its signature instead.
 */
interface Codec
{
    public function name(): string;

    public function encode(mixed $value): mixed;

    public function decode(mixed $value): mixed;
}

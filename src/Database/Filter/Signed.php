<?php

namespace Utopia\Database\Filter;

/**
 * A codec that tells its instances apart in cache keys by its signature rather than its class: implement it when
 * two instances of one class can encode differently.
 */
interface Signed extends Codec
{
    /**
     * The same for every instance that encodes and decodes alike, different for any that does not.
     */
    public function signature(): string;
}

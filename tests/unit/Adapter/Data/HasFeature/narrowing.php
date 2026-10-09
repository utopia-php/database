<?php

namespace Tests\Unit\Adapter\Data\HasFeature;

use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature\Spatial;
use Utopia\Database\Adapter\Pool;

use function PHPStan\Testing\assertType;

function adapter(Adapter $adapter): void
{
    if ($adapter->hasFeature(Spatial::class)) {
        assertType('Utopia\Database\Adapter', $adapter);
    }
}

function pool(Pool $pool): void
{
    if ($pool->hasFeature(Spatial::class)) {
        assertType('Utopia\Database\Adapter\Pool', $pool);
    }
}

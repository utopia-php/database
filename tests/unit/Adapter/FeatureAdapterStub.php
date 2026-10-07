<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;

abstract class FeatureAdapterStub extends Adapter implements
    Feature\ConnectionId,
    Feature\QueryBuilder,
    Feature\RawQuery,
    Feature\Spatial,
    Feature\Timeouts
{
}

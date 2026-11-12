<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;

abstract class FeatureAdapterStub extends Adapter implements
    Feature\Connection,
    Feature\QueryBuilder,
    Feature\RawQuery,
    Feature\Spatial,
    Feature\Timeouts
{
}

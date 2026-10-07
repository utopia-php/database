<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;

abstract class FeatureAdapterStub extends Adapter implements
    Feature\ColumnTypes,
    Feature\Connection,
    Feature\QueryBuilder,
    Feature\RawQuery,
    Feature\SchemaAttributes,
    Feature\SchemaIndexes,
    Feature\Spatial,
    Feature\Timeouts
{
}

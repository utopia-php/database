<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter\Feature;

abstract class CastingAdapterStub extends FeatureAdapterStub implements
    Feature\InternalCasting,
    Feature\UTCCasting
{
}

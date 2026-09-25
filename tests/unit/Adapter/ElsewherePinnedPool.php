<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;

final class ElsewherePinnedPool extends Pool
{
    private ?Adapter $elsewhere = null;

    public function pinElsewhere(Adapter $adapter): void
    {
        $this->elsewhere = $adapter;
    }

    protected function pin(): ?Adapter
    {
        return $this->elsewhere;
    }
}

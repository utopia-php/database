<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter\Redis;
use Utopia\Cache\Exception\Unavailable;

final class FailingLegacyRedis extends Redis
{
    public bool $failCompletion = false;

    #[\Override]
    public function purgeLegacy(string $key, string $hash = ''): bool
    {
        if ($this->failCompletion && \str_ends_with($key, '#owners')) {
            throw new Unavailable('Legacy completion unavailable');
        }

        return parent::purgeLegacy($key, $hash);
    }
}

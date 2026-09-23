<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter\Memory as MemoryCache;

final class FailingMemory extends MemoryCache
{
    private bool $failing = false;

    public function failPurges(): void
    {
        $this->failing = true;
    }

    public function seedEpoch(string $collection): void
    {
        $this->save('default:qcache:'.$collection.'#epoch', 'active:seed');
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        if ($this->failing && \str_ends_with($key, '#epoch')) {
            return false;
        }

        return parent::purge($key, $hash);
    }
}

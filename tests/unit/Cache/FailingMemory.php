<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter\Memory as MemoryCache;

final class FailingMemory extends MemoryCache
{
    private bool $failing = false;

    public function failBlocks(): void
    {
        $this->failing = true;
    }

    /**
     * @param  array<int|string, mixed>|string  $data
     * @return bool|string|array<int|string, mixed>
     */
    #[\Override]
    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if (
            $this->failing
            && \str_ends_with($key, '#epoch')
            && \is_string($data)
            && \str_starts_with($data, 'blocked:')
        ) {
            return false;
        }

        return parent::save($key, $data, $hash);
    }
}

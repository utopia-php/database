<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;

final class InvalidationCache extends Cache
{
    /** @var array<string, string> */
    private array $values = [];

    /** @var array<string, int> */
    private array $generations = [];

    public function __construct()
    {
        parent::__construct(new Memory());
    }

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        return $this->values[$key] ?? false;
    }

    #[\Override]
    public function save(string $key, mixed $data, string $hash = '', int $ttl = 0): bool|string|array
    {
        if (\is_string($data)) {
            $this->values[$key] = $data;
        }

        return $data;
    }

    #[\Override]
    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;
        unset($this->values[$key]);

        return true;
    }
}

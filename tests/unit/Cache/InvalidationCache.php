<?php

namespace Tests\Unit\Cache;

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;

final class InvalidationCache extends Cache
{
    /** @var array<string, string> */
    public array $values = [];

    /** @var array<string, int> */
    public array $generations = [];

    /** @var array<string, int> */
    public array $purges = [];

    /** @var array<string, true> */
    public array $failures = [];

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
    public function save(string $key, mixed $data, string $hash = ''): bool|string|array
    {
        if (isset($this->failures[$key])) {
            return false;
        }

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
        $this->purges[$key] = ($this->purges[$key] ?? 0) + 1;
        if (isset($this->failures[$key])) {
            return false;
        }

        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;
        unset($this->values[$key]);

        return true;
    }

    public function fail(string $key): void
    {
        $this->failures[$key] = true;
    }

    public function getPurges(string $key): int
    {
        return $this->purges[$key] ?? 0;
    }
}

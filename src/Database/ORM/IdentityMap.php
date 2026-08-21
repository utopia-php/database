<?php

namespace Utopia\Database\ORM;

class IdentityMap
{
    /** @var array<string, array<string, object>> */
    private array $map = [];

    public function put(string $collection, string $id, object $entity): void
    {
        $this->map[$collection][$id] = $entity;
    }

    public function get(string $collection, string $id): ?object
    {
        return $this->map[$collection][$id] ?? null;
    }

    public function has(string $collection, string $id): bool
    {
        return isset($this->map[$collection][$id]);
    }

    public function remove(string $collection, string $id): void
    {
        unset($this->map[$collection][$id]);
    }

    public function clear(): void
    {
        $this->map = [];
    }

    /**
     * @return \Generator<int, object>
     */
    public function all(): \Generator
    {
        foreach ($this->map as $collection) {
            foreach ($collection as $entity) {
                yield $entity;
            }
        }
    }

    /**
     * @return array<string, array<string, object>>
     */
    public function snapshot(): array
    {
        return $this->map;
    }

    /**
     * @param  array<string, array<string, object>>  $map
     */
    public function restore(array $map): void
    {
        $this->map = $map;
    }
}

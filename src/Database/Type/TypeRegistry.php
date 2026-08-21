<?php

namespace Utopia\Database\Type;

use Utopia\Database\Database;

class TypeRegistry
{
    /** @var array<string, Custom> */
    private array $types = [];

    /** @var array<string, Embeddable> */
    private array $embeddables = [];

    public function register(Custom $type): void
    {
        $this->types[$type->name()] = $type;

        Database::addFilter(
            $type->name(),
            fn (mixed $value) => $type->encode($value),
            fn (mixed $value) => $type->decode($value),
        );
    }

    public function registerEmbeddable(Embeddable $type): void
    {
        $this->embeddables[$type->name()] = $type;
    }

    public function get(string $name): ?Custom
    {
        return $this->types[$name] ?? null;
    }

    public function getEmbeddable(string $name): ?Embeddable
    {
        return $this->embeddables[$name] ?? null;
    }

    /**
     * @return array<string, Custom>
     */
    public function all(): array
    {
        return $this->types;
    }

    /**
     * @return array<string, Embeddable>
     */
    public function allEmbeddables(): array
    {
        return $this->embeddables;
    }
}

<?php

namespace Utopia\Database\Type;

use Utopia\Database\Database;

class TypeRegistry
{
    /** @var array<string, CustomType> */
    private array $types = [];

    /** @var array<string, EmbeddableType> */
    private array $embeddables = [];

    public function register(CustomType $type): void
    {
        $this->types[$type->name()] = $type;

        Database::addFilter(
            $type->name(),
            fn (mixed $value) => $type->encode($value),
            fn (mixed $value) => $type->decode($value),
        );
    }

    public function registerEmbeddable(EmbeddableType $type): void
    {
        $this->embeddables[$type->name()] = $type;
    }

    public function get(string $name): ?CustomType
    {
        return $this->types[$name] ?? null;
    }

    public function getEmbeddable(string $name): ?EmbeddableType
    {
        return $this->embeddables[$name] ?? null;
    }

    /**
     * @return array<string, CustomType>
     */
    public function all(): array
    {
        return $this->types;
    }

    /**
     * @return array<string, EmbeddableType>
     */
    public function allEmbeddables(): array
    {
        return $this->embeddables;
    }
}

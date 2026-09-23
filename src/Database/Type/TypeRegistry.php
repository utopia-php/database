<?php

namespace Utopia\Database\Type;

use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate as DuplicateException;

class TypeRegistry
{
    /** @var array<string, Custom> */
    private array $types = [];

    /** @var array<string, Embeddable> */
    private array $embeddables = [];

    /**
     * @throws DuplicateException
     */
    public function register(Custom $type): void
    {
        $name = $type->name();

        if (\in_array($name, Database::DEFAULT_FILTERS, true)) {
            throw new DuplicateException("Custom type \"{$name}\" collides with the built-in filter of the same name");
        }

        $this->types[$name] = $type;
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

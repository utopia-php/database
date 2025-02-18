<?php

namespace Utopia\Database;

class QueryContext
{
    public const TYPE_EQUAL = 'equal';

    protected array $collections;

    protected array $aliases;

    protected array $queries;

    /**
     * @param  array<Document>  $collections
     *
     * @throws \Exception
     */
    public function __construct(array $queries)
    {
        foreach ($queries as $query) {
            $this->queries[] = clone $query;
        }
    }

    public function __clone(): void
    {
        foreach ($this->values as $index => $value) {
            if ($value instanceof self) {
                $this->values[$index] = clone $value;
            }
        }
    }

    public function getCollections(): array
    {
        return $this->collections;
    }

    public function getQueries(): array
    {
        return $this->queries;
    }

    public function getCollectionByAlias(string $alias): array
    {
        return $this->collections[''];
    }

    public function add(Document $collection, string $alias): void
    {
        $this->collections[] = $collection;
        $this->aliases[$alias] = $collection->getId();
    }
}

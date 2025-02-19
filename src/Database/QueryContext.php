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

        var_dump('__clone __clone __clone __clone __clone __clone __clone __clone __clone __clone __clone __clone __clone __clone');

//        foreach ($this->values as $index => $value) {
//            if ($value instanceof self) {
//                $this->values[$index] = clone $value;
//            }
//        }
    }

    public function getCollections(): array
    {
        return $this->collections;
    }

    public function getQueries(): array
    {
        return $this->queries;
    }

    public function getCollectionByAlias(string $alias): Document
    {
        /**
         * $alias can be an empty string
         */
        $collectionId = $this->aliases[$alias] ?? null;

        if (is_null($collectionId)) {
            return new Document;
        }

        foreach ($this->collections as $collection) {
            if ($collection->getId() === $collectionId) {
                return $collection;
            }
        }

        return new Document;
    }

    public function add(Document $collection, string $alias = Query::DEFAULT_ALIAS): void
    {
        $this->collections[] = $collection;
        $this->aliases[$alias] = $collection->getId();
    }
}

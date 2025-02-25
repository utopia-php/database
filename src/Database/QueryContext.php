<?php

namespace Utopia\Database;

class QueryContext
{
    protected array $collections = [];

    protected array $aliases = [];

    protected array $queries = [];

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
            return new Document();
        }

        foreach ($this->collections as $collection) {
            if ($collection->getId() === $collectionId) {
                return $collection;
            }
        }

        return new Document();
    }

    public function add(Document $collection, string $alias = Query::DEFAULT_ALIAS): void
    {
        $this->collections[] = $collection;
        $this->aliases[$alias] = $collection->getId();
    }

    public function setLimit($limit): void
    {

        $this->aliases

//        $collection->getId(),
//            $queries,
//            $limit ?? 25,
//            $offset ?? 0,
//            $orderAttributes,
//            $orderTypes,
//            $cursor,
//            $cursorDirection,
//            $forPermission
    }
}

<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Query as QueryException;

class QueryContext
{
    protected array $collections = [];

    protected array $aliases = [];

    //protected array $queries = [];

    protected array $orders = [];

    protected array $selects = [];

    protected array $filters = [];

    protected array $joins = [];

    protected ?int $limit = null;

    protected ?int $offset = null;

    protected ?Query $cursor = null;

    public function __construct()
    {

    }

    /**
     * @return array<Document>
     */
    public function getCollections(): array
    {
        return $this->collections;
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

    /**
     * @throws QueryException
     */
    public function add(Document $collection, string $alias = Query::DEFAULT_ALIAS): void
    {
        if (! empty($this->aliases[$alias])) {
            throw new QueryException('Ambiguous alias for collection "'.$collection->getId().'".');
        }

        $this->collections[] = $collection;
        $this->aliases[$alias] = $collection->getId();
    }
}

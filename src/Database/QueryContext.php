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

    public function __construct__2(array $queries):void
    {
        foreach ($queries as $query) {
            //$this->queries[] = clone $query;
            $query = clone $query;

            switch ($query->getMethod()) {
                case Query::TYPE_ORDER_ASC:
                case Query::TYPE_ORDER_DESC:
                    $this->orders[] = $query;

                    break;
                case Query::TYPE_LIMIT:
                    if (! is_null($this->limit)) {
                        break;
                    }

                    $this->limit = $query->getValue();

                    break;
                case Query::TYPE_OFFSET:
                    if (! is_null($this->offset)) {
                        break;
                    }

                    $this->offset = $query->getValue();

                    break;
                case Query::TYPE_CURSOR_AFTER:
                case Query::TYPE_CURSOR_BEFORE:
                    if (! is_null($this->cursor)) {
                        continue 2;
                    }

                    $this->cursor = $query;
                    break;

                case Query::TYPE_SELECT:
                    $this->selects[] = $query;

                    break;

                case Query::TYPE_INNER_JOIN:
                case Query::TYPE_LEFT_JOIN:
                case Query::TYPE_RIGHT_JOIN:
                    $this->joins[] = $query;

                    break;

                default:
                    $this->filters[] = $query;

                    break;
            }
        }
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

    public function setLimit(int $limit): void
    {
        $this->limit = $limit;
    }

    public function setOffset(int $offset): void
    {
        $this->offset = $offset;
    }

    /**
     * @return array<Query>
     */
    public function getJoinQueries(): array
    {
        return $this->joins;
    }

    /**
     * @return Query|null
     */
    public function getCursorQuery(): ?Query
    {
        return $this->cursor;
    }

    /**
     * @return Query|null
     */
    public function getLimit(): ?int
    {
        return $this->limit;
    }

    /**
     * @return Query|null
     */
    public function getOffset(): ?int
    {
        return $this->offset;
    }
}

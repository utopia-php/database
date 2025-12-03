<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Validator\Authorization;

class QueryContext
{
    /**
     * @var array<Document>
     */
    protected array $collections = [];

    /**
     * @var array<string>
     */
    protected array $aliases = [];

    /**
     * @var array<mixed>
     */
    protected array $skipAuthCollections = [];

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

    /**
     * @return Document
     */
    public function getMainCollection(): Document
    {
        return $this->getCollections()[0];
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

    public function addSkipAuth(string $collection, string $permission, bool $skipAuth): void
    {
        $this->skipAuthCollections[$collection][$permission] = $skipAuth;
    }

    public function skipAuth(string $collection, string $permission, Authorization $authorization): bool
    {
        if (!$authorization->getStatus()) { // for Authorization::disable();
            return true;
        }

        if (empty($this->skipAuthCollections[$collection][$permission])) {
            return false;
        }

        return true;
    }

    /**
     * @param array<Query> $queries
     * @param Query $query
     * @return array{array<Query>, bool}
     * @throws \Exception
     */
    public static function addSelect(array $queries, Query $query): array
    {
        $merge = true;
        $found = false;

        foreach ($queries as $q) {
            if ($q->getMethod() === Query::TYPE_SELECT) {
                $found = true;

                if ($q->getAlias() === $query->getAlias()) {
                    if ($q->getAttribute() === '*') {
                        $merge = false;
                    }

                    if ($q->getAttribute() === $query->getAttribute()) {
                        if ($q->getAs() === $query->getAs()) {
                            $merge = false;
                        }
                    }
                }
            }
        }

        if ($found && $merge) {
            $queries = [
                ...$queries,
                $query
            ];

            return [$queries, true];
        }

        return [$queries, false];
    }
}

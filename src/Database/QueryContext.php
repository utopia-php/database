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

    public function skipAuth(string $collection, string $permission): bool
    {
        if (!Authorization::$status) { // for Authorization::disable();
            return true;
        }

        if (empty($this->skipAuthCollections[$collection][$permission])) {
            return false;
        }

        return true;
    }


}

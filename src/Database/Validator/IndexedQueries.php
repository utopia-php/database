<?php

namespace Utopia\Database\Validator;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;

class IndexedQueries extends Queries
{
    /**
     * @var array<Document>
     */
    protected array $attributes = [];

    /**
     * @var array<Document>
     */
    protected array $indexes = [];

    /**
     * Expression constructor
     *
     * This Queries Validator filters indexes for only available indexes
     *
     * @param array<Document> $attributes
     * @param array<Document> $indexes
     * @param array<Base> $validators
     * @throws Exception
     */
    public function __construct(array $attributes = [], array $indexes = [], array $validators = [])
    {
        $this->attributes = $attributes;

        $this->indexes[] = new Document([
            'type' => Database::INDEX_UNIQUE,
            'attributes' => ['$id']
        ]);

        $this->indexes[] = new Document([
            'type' => Database::INDEX_KEY,
            'attributes' => ['$createdAt']
        ]);

        $this->indexes[] = new Document([
            'type' => Database::INDEX_KEY,
            'attributes' => ['$updatedAt']
        ]);

        foreach ($indexes as $index) {
            $this->indexes[] = $index;
        }

        parent::__construct($validators);
    }

    /**
     * @param mixed $value
     * @return bool
     * @throws Exception
     */
    public function isValid($value): bool
    {
        if (!parent::isValid($value)) {
            return false;
        }
        $queries = [];
        foreach ($value as $query) {
            if (!$query instanceof Query) {
                $query = Query::parse($query);
            }

            $queries[] = $query;
        }

        $grouped = Query::groupByType($queries);
        $filters = $grouped['filters'];

        foreach ($filters as $filter) {
            if ($filter->getMethod() === Query::TYPE_SEARCH) {
                $matched = false;

                foreach ($this->indexes as $index) {
                    if (
                        $index->getAttribute('type') === Database::INDEX_FULLTEXT
                        && $index->getAttribute('attributes') === [$filter->getAttribute()]
                    ) {
                        $matched = true;
                    }
                }

                if (!$matched) {
                    $this->message = "Searching by attribute \"{$filter->getAttribute()}\" requires a fulltext index.";
                    return false;
                }
            }
        }

        return true;
    }
}

<?php

namespace Utopia\Database\Validator;

use Exception;
use Throwable;
use Utopia\Database\Attribute as AttributeVO;
use Utopia\Database\Document;
use Utopia\Database\Index as IndexVO;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Query\Method;
use Utopia\Query\Schema\IndexType;

/**
 * Validates queries against available indexes, ensuring search queries have matching fulltext indexes.
 */
class IndexedQueries extends Queries
{
    /**
     * @var array<AttributeVO>
     */
    protected array $attributes = [];

    /**
     * @var array<IndexVO>
     */
    protected array $indexes = [];

    /**
     * Expression constructor
     *
     * This Queries Validator filters indexes for only available indexes
     *
     * @param  array<AttributeVO|Document>  $attributes
     * @param  array<IndexVO|Document>  $indexes
     * @param  array<Base>  $validators
     *
     * @throws Exception
     */
    public function __construct(array $attributes = [], array $indexes = [], array $validators = [])
    {
        foreach ($attributes as $attribute) {
            $this->attributes[] = $attribute instanceof AttributeVO ? $attribute : AttributeVO::fromDocument($attribute);
        }

        $this->indexes[] = new IndexVO(key: '_uid_', type: IndexType::Unique, attributes: ['$id']);
        $this->indexes[] = new IndexVO(key: '_created_at_', type: IndexType::Key, attributes: ['$createdAt']);
        $this->indexes[] = new IndexVO(key: '_updated_at_', type: IndexType::Key, attributes: ['$updatedAt']);

        foreach ($indexes as $index) {
            $this->indexes[] = $index instanceof IndexVO ? $index : IndexVO::fromDocument($index);
        }

        parent::__construct($validators);
    }

    /**
     * Count vector queries across entire query tree
     *
     * @param  array<Query>  $queries
     */
    private function countVectorQueries(array $queries): int
    {
        $count = 0;

        foreach ($queries as $query) {
            if (in_array($query->getMethod(), [Method::VectorDot, Method::VectorCosine, Method::VectorEuclidean])) {
                $count++;
            }

            if ($query->isNested()) {
                /** @var array<Query> $nestedValues */
                $nestedValues = $query->getValues();
                $count += $this->countVectorQueries($nestedValues);
            }
        }

        return $count;
    }

    /**
     * @param  mixed  $value
     *
     * @throws Exception
     */
    public function isValid($value): bool
    {
        /** @var array<Query|string> $value */
        if (! parent::isValid($value)) {
            return false;
        }
        $queries = [];
        foreach ($value as $query) {
            if (! $query instanceof Query) {
                try {
                    $query = Query::parse((string) $query);
                } catch (Throwable $e) {
                    $this->message = 'Invalid query: '.$e->getMessage();

                    return false;
                }
            }

            if ($query->isNested() && $query->getMethod() !== Method::Having) {
                if (! self::isValid($query->getValues())) {
                    return false;
                }
            }

            $queries[] = $query;
        }

        $vectorQueryCount = $this->countVectorQueries($queries);
        if ($vectorQueryCount > 1) {
            $this->message = 'Cannot use multiple vector queries in a single request';

            return false;
        }

        $grouped = Query::groupForDatabase($queries);
        $filters = $grouped['filters'];

        foreach ($filters as $filter) {
            if (
                $filter->getMethod() === Method::Search ||
                $filter->getMethod() === Method::NotSearch
            ) {
                $matched = false;

                foreach ($this->indexes as $index) {
                    if (
                        $index->type === IndexType::Fulltext
                        && $index->attributes === [$filter->getAttribute()]
                    ) {
                        $matched = true;
                    }
                }

                if (! $matched) {
                    $this->message = "Searching by attribute \"{$filter->getAttribute()}\" requires a fulltext index.";

                    return false;
                }
            }
        }

        return true;
    }
}

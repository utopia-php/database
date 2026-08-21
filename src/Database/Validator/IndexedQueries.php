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
    private const string UID_INDEX = '_uid_';

    private const string CREATED_AT_INDEX = '_created_at_';

    private const string UPDATED_AT_INDEX = '_updated_at_';

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

        $this->indexes[] = new IndexVO(key: self::UID_INDEX, type: IndexType::Unique, attributes: [Document::ID]);
        $this->indexes[] = new IndexVO(key: self::CREATED_AT_INDEX, type: IndexType::Key, attributes: [Document::CREATED_AT]);
        $this->indexes[] = new IndexVO(key: self::UPDATED_AT_INDEX, type: IndexType::Key, attributes: [Document::UPDATED_AT]);

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
     * @param  array<Query>  $queries
     * @return array<string, true>
     */
    private function joinAliases(array $queries): array
    {
        $aliases = [];

        foreach ($queries as $query) {
            if (! $query->getMethod()->isJoin()) {
                continue;
            }

            $alias = $query->getJoinAlias();
            if ($alias !== '') {
                $aliases[$alias] = true;
            }
        }

        return $aliases;
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

            $queries[] = $query;
        }

        $vectorQueryCount = $this->countVectorQueries($queries);
        if ($vectorQueryCount > 1) {
            $this->message = 'Cannot use multiple vector queries in a single request';

            return false;
        }

        return $this->validateSearchIndexes($queries, $this->joinAliases($queries));
    }

    /**
     * @param  array<Query>  $queries
     * @param  array<string, true>  $joinAliases
     */
    private function validateSearchIndexes(array $queries, array $joinAliases): bool
    {
        foreach ($queries as $query) {
            if (
                $query->getMethod() === Method::Search ||
                $query->getMethod() === Method::NotSearch
            ) {
                $attribute = $query->getAttribute();
                $dot = \strpos($attribute, '.');
                if ($dot !== false && isset($joinAliases[\substr($attribute, 0, $dot)])) {
                    continue;
                }

                $matched = false;

                foreach ($this->indexes as $index) {
                    if (
                        $index->type === IndexType::Fulltext
                        && $index->attributes === [$attribute]
                    ) {
                        $matched = true;
                    }
                }

                if (! $matched) {
                    $this->message = "Searching by attribute \"{$attribute}\" requires a fulltext index.";

                    return false;
                }
            }

            if ($query->isNested() && $query->getMethod() !== Method::Having) {
                /** @var array<Query> $nested */
                $nested = $query->getValues();
                if (! $this->validateSearchIndexes($nested, $joinAliases)) {
                    return false;
                }
            }
        }

        return true;
    }
}

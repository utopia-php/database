<?php

namespace Utopia\Database\Validator\Queries;

use Exception;
use Throwable;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base as QueryBase;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\IndexType;

/**
 * Validates queries against available indexes, ensuring search queries have matching fulltext indexes.
 */
class Indexed extends Base
{
    private const string UID_INDEX = '_uid_';

    private const string CREATED_AT_INDEX = '_created_at_';

    private const string UPDATED_AT_INDEX = '_updated_at_';

    /**
     * @var list<Index>|null
     */
    private static ?array $internalIndexes = null;

    /**
     * @var list<Attribute>
     */
    protected array $attributes = [];

    /**
     * @var list<Index>
     */
    protected array $indexes = [];

    /**
     * @param  array<Attribute|Document>  $attributes
     * @param  array<Index|Document>  $indexes
     * @param  array<QueryBase>  $validators
     *
     * @throws Exception
     */
    public function __construct(array $attributes = [], array $indexes = [], array $validators = [])
    {
        foreach ($attributes as $attribute) {
            $this->attributes[] = $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute);
        }

        $this->indexes = self::$internalIndexes ??= [
            Index::unique(self::UID_INDEX, [Document::ID]),
            Index::key(self::CREATED_AT_INDEX, [Document::CREATED_AT]),
            Index::key(self::UPDATED_AT_INDEX, [Document::UPDATED_AT]),
        ];

        foreach ($indexes as $index) {
            $this->indexes[] = $index instanceof Index ? $index : Index::fromDocument($index);
        }

        parent::__construct($validators);
    }

    /**
     * Count vector queries across entire query tree
     *
     * @param  array<BaseQuery>  $queries
     */
    private function countVectorQueries(array $queries): int
    {
        $count = 0;

        foreach ($queries as $query) {
            if (in_array($query->getMethod(), [Method::VectorDot, Method::VectorCosine, Method::VectorEuclidean])) {
                $count++;
            }

            if ($query->isNestedJoin()) {
                $count += $this->countVectorQueries($query->getJoinOnQueries());
            } elseif ($query->isNested()) {
                /** @var array<BaseQuery> $nestedValues */
                $nestedValues = $query->getValues();
                $count += $this->countVectorQueries($nestedValues);
            }
        }

        return $count;
    }

    /**
     * @param  array<BaseQuery>  $queries
     * @return array<string, list<Index>> The indexes of the collection each join alias names
     */
    private function joinIndexes(array $queries): array
    {
        $indexes = [];

        foreach ($queries as $query) {
            if (! $query->getMethod()->isJoin()) {
                continue;
            }

            $collection = $this->getJoinedCollection($query->getAttribute());

            $indexes[$query->getAlias()] = $collection === null ? [] : Collection::fromDocument($collection)->indexes();
        }

        return $indexes;
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

        return $this->validateSearchIndexes($queries, $this->joinIndexes($queries));
    }

    /**
     * @param  array<BaseQuery>  $queries
     * @param  array<string, list<Index>>  $joinIndexes
     */
    private function validateSearchIndexes(array $queries, array $joinIndexes): bool
    {
        foreach ($queries as $query) {
            if (
                $query->getMethod() === Method::Search ||
                $query->getMethod() === Method::NotSearch
            ) {
                $attribute = $query->getAttribute();
                $column = $attribute;
                $indexes = $this->indexes;

                $dot = \strpos($attribute, '.');
                if ($dot !== false && isset($joinIndexes[\substr($attribute, 0, $dot)])) {
                    $column = \substr($attribute, $dot + 1);
                    $indexes = $joinIndexes[\substr($attribute, 0, $dot)];
                }

                $matched = false;

                foreach ($indexes as $index) {
                    if (
                        $index->type === IndexType::Fulltext
                        && $index->attributes === [$column]
                    ) {
                        $matched = true;
                    }
                }

                if (! $matched) {
                    $this->message = "Searching by attribute \"{$attribute}\" requires a fulltext index.";

                    return false;
                }
            }

            if ($query->isNestedJoin()) {
                if (! $this->validateSearchIndexes($query->getJoinOnQueries(), $joinIndexes)) {
                    return false;
                }
            } elseif ($query->isNested()) {
                /** @var array<BaseQuery> $nested */
                $nested = $query->getValues();
                if (! $this->validateSearchIndexes($nested, $joinIndexes)) {
                    return false;
                }
            }
        }

        return true;
    }
}

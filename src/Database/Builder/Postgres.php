<?php

namespace Utopia\Database\Builder;

use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Validator\ObjectPath;
use Utopia\Query\Builder\PostgreSQL as Base;
use Utopia\Query\Query;
use Utopia\Query\Schema\ColumnType;

/**
 * The PostgreSQL builder, which also compiles filters on their own, prepares search terms as 7.x did, and writes a
 * filter on a path into an object attribute only when every key of the path is a plain key.
 */
class Postgres extends Base implements Filtering
{
    use CompilesFilters;
    use PreparesSearchTerms;

    /**
     * @throws QueryException
     */
    #[\Override]
    public function compileFilter(Query $query): string
    {
        $attribute = $query->getAttribute();

        if ($query->getAttributeType() === ColumnType::Object->value && \str_contains($attribute, '.')) {
            $path = new ObjectPath();
            if (! $path->isValid($attribute)) {
                throw new QueryException('Invalid object path "'.$attribute.'": '.$path->getDescription());
            }
        }

        return parent::compileFilter($query);
    }
}

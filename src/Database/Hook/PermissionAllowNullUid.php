<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

final readonly class PermissionAllowNullUid implements Filter
{
    private AllowNullColumn $inner;

    public function __construct(
        Filter $filter,
        string $documentColumn,
        string $quoteChar = '`',
    ) {
        $this->inner = new AllowNullColumn($filter, $documentColumn, $quoteChar);
    }

    public function filter(string $table): Condition
    {
        return $this->inner->filter($table);
    }
}

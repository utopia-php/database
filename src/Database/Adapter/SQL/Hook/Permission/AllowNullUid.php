<?php

namespace Utopia\Database\Adapter\SQL\Hook\Permission;

use Utopia\Database\Adapter\SQL\Hook\Column\AllowNull;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

final readonly class AllowNullUid implements Filter
{
    private AllowNull $inner;

    public function __construct(
        Filter $filter,
        string $documentColumn,
        string $quoteCharacter = '`',
    ) {
        $this->inner = new AllowNull($filter, $documentColumn, $quoteCharacter);
    }

    #[\Override]
    public function filter(string $table): Condition
    {
        return $this->inner->filter($table);
    }
}

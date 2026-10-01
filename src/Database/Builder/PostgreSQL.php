<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\PostgreSQL as Base;

/**
 * The PostgreSQL builder, which also compiles filters on their own.
 */
class PostgreSQL extends Base implements Filtering
{
    use CompilesFilters;
}

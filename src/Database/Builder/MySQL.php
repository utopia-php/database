<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\MySQL as Base;

/**
 * The MySQL builder, which also compiles filters on their own.
 */
class MySQL extends Base implements Filtering
{
    use CompilesFilters;
}

<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\MariaDB as Base;

/**
 * The MariaDB builder, which also compiles filters on their own.
 */
class MariaDB extends Base implements Filtering
{
    use CompilesFilters;
}

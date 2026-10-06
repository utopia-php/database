<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\MariaDB as Base;

/**
 * The MariaDB builder, which also compiles filters on their own and prepares search terms as 7.x did.
 */
class MariaDB extends Base implements Filtering
{
    use CompilesFilters;
    use PreparesSearchTerms;
}

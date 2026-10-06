<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\MySQL as Base;

/**
 * The MySQL builder, which also compiles filters on their own and prepares search terms as 7.x did.
 */
class MySQL extends Base implements Filtering
{
    use CompilesFilters;
    use PreparesSearchTerms;
}

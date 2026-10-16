<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\MySQL as Base;

/**
 * The MySQL builder, which also compiles filters on their own and prepares search terms, matches containsAll()
 * as 7.x did, raises a floating aggregate that overflowed and signs a bitwise one.
 */
class MySQL extends Base implements Filtering, Scoping
{
    use CompilesFilters;
    use MatchesContainsAllPatterns;
    use PreparesSearchTerms;
    use CompilesAggregates;
    use RefusesJoinedWrites;
    use ScopesCollections;
}

<?php

namespace Utopia\Database\Builder;

use Utopia\Query\Builder\MySQL as Base;

/**
 * The MySQL builder, which also compiles filters on their own and prepares search terms, matches containsAll()
 * as 7.x did and raises a floating aggregate that overflowed.
 */
class MySQL extends Base implements Filtering, Scoping
{
    use CompilesFilters;
    use MatchesContainsAllPatterns;
    use PreparesSearchTerms;
    use RaisesFloatOverflow;
    use RefusesJoinedWrites;
    use ScopesCollections;
}

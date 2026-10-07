<?php

namespace Utopia\Database\Schema;

use Utopia\Query\Schema\IndexType;

/**
 * An index as the engine reports it, read back from its catalog. Its type is Unique, Fulltext, Spatial or Key.
 */
final readonly class Index
{
    /**
     * @param  list<string>  $columns  The indexed columns, in index order
     * @param  list<int|null>  $lengths  The prefix length of each column; null where the whole value is indexed or the
     *                                    engine reports no prefix lengths, so null is never compared
     */
    public function __construct(
        public string $name,
        public IndexType $type,
        public array $columns,
        public array $lengths,
    ) {
    }
}

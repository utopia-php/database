<?php

namespace Utopia\Database\Adapter;

use DateTime;
use Utopia\Query\Schema\ColumnType;

/**
 * The fixed limits of an adapter: the largest string, varchar and integer sizes, the attribute, index and
 * document size caps (0 when there is none), the attributes and indexes every collection holds, the longest
 * index key and document id, the datetime range, the type of the sequence, and the names no attribute or
 * index may take.
 */
final readonly class Limits
{
    /**
     * @param  list<string>  $keywords
     * @param  list<string>  $internalIndexKeys
     */
    public function __construct(
        public int $string,
        public int $varchar,
        public int $integer,
        public int $bigInteger,
        public int $attributes,
        public int $indexes,
        public int $defaultAttributes,
        public int $defaultIndexes,
        public int $indexLength,
        public int $uidLength,
        public int $documentSize,
        public DateTime $minDateTime,
        public DateTime $maxDateTime,
        public ColumnType $idType,
        public array $keywords,
        public array $internalIndexKeys,
    ) {
    }
}

<?php

namespace Utopia\Database\Adapter;

use DateTime;
use Utopia\Database\Database;
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
        public bool $escalatedStringByteCap = true,
    ) {
    }

    /**
     * Byte capacity for a {@see ColumnType::String} attribute stored outside VARCHAR, matching {@see SQL::getSqlType()}.
     * Null when the declared size fits in VARCHAR or when escalated TEXT types have no byte cap (PostgreSQL).
     */
    public function stringEscalatedStorageByteLimit(int $size): ?int
    {
        if ($size <= $this->varchar) {
            return null;
        }

        if (! $this->escalatedStringByteCap) {
            return null;
        }

        if ($size > 16777215) {
            return Database::MAX_LONGTEXT_BYTES;
        }

        if ($size > 65535) {
            return Database::MAX_MEDIUMTEXT_BYTES;
        }

        return Database::MAX_TEXT_BYTES;
    }
}

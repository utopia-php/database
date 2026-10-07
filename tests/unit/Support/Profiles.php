<?php

namespace Tests\Unit\Support;

use DateTime;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Adapter\Profile;
use Utopia\Database\Capability;
use Utopia\Query\Schema\ColumnType;

/**
 * Profiles for validators built outside a database: only the capabilities, features and limits named, every
 * other limit 0 and the datetime range 0000-01-01 to 9999-12-31.
 */
final class Profiles
{
    /**
     * @param  list<Capability>  $capabilities
     * @param  list<class-string>  $features
     * @param  list<string>  $internalIndexKeys
     */
    public static function of(
        array $capabilities = [],
        array $features = [],
        bool $sharedTables = false,
        bool $migrating = false,
        int $string = 0,
        int $varchar = 0,
        int $integer = 0,
        int $attributes = 0,
        int $indexLength = 0,
        int $uidLength = 36,
        int $documentSize = 0,
        ColumnType $idType = ColumnType::Integer,
        ?DateTime $minDateTime = null,
        ?DateTime $maxDateTime = null,
        array $internalIndexKeys = [],
    ): Profile {
        return new Profile(
            new Limits(
                string: $string,
                varchar: $varchar,
                integer: $integer,
                bigInteger: $integer,
                attributes: $attributes,
                indexes: 0,
                defaultAttributes: 0,
                defaultIndexes: 0,
                indexLength: $indexLength,
                uidLength: $uidLength,
                documentSize: $documentSize,
                minDateTime: $minDateTime ?? new DateTime('0000-01-01'),
                maxDateTime: $maxDateTime ?? new DateTime('9999-12-31'),
                idType: $idType,
                keywords: [],
                internalIndexKeys: $internalIndexKeys,
            ),
            $capabilities,
            $features,
            $sharedTables,
            $migrating,
        );
    }
}

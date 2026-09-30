<?php

namespace Tests\Unit;

use Utopia\Database\Database;

final class FilterRegistry extends Database
{
    /**
     * @return array<string, array{encode: callable, decode: callable, signature: string}>
     */
    public static function filters(): array
    {
        return self::$filters;
    }

    public static function defaultsRegistered(): bool
    {
        return self::$defaultFiltersRegistered;
    }

    /**
     * @param  array<string, array{encode: callable, decode: callable, signature: string}>  $filters
     */
    public static function restore(array $filters, bool $defaultsRegistered): void
    {
        self::$filters = $filters;
        self::$defaultFiltersRegistered = $defaultsRegistered;
    }

    public static function clear(): void
    {
        self::restore([], false);
    }
}

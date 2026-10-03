<?php

namespace Utopia\Database\Cache;

use Utopia\Cache\Cache;
use WeakMap;

/**
 * The caches seen keeping hash fields. Only that answer is remembered: a cache that once lists no field, as after a
 * flush between a save and its list, is asked again, so it never settles on per-token keys its purge cannot remove.
 */
final class Fields
{
    /** @var WeakMap<Cache, true>|null */
    private static ?WeakMap $caches = null;

    public static function kept(Cache $cache): bool
    {
        return isset(self::caches()[$cache]);
    }

    public static function remember(Cache $cache): void
    {
        self::caches()[$cache] = true;
    }

    /**
     * @return WeakMap<Cache, true>
     */
    private static function caches(): WeakMap
    {
        return self::$caches ??= new WeakMap();
    }
}

<?php

namespace Tests\Unit\Support;

use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Database;

final class UncachedTwin
{
    public static function of(Database $database): Database
    {
        return (new Database($database->getAdapter(), new Cache(new None())))
            ->setAuthorization($database->getAuthorization())
            ->setDatabase($database->getDatabase())
            ->setNamespace($database->getNamespace());
    }
}

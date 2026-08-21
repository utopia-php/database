<?php

namespace Utopia\Database\Migration\Strategy;

use Utopia\Database\Database;

class OnlineSchemaChange
{
    public function alter(Database $db, string $collection, callable $changes): void
    {
        $adapter = $db->getAdapter();
        $hadLocks = $adapter->getAlterLocks();
        $adapter->enableAlterLocks(false);

        try {
            $changes($db, $collection);
        } finally {
            $adapter->enableAlterLocks($hadLocks);
        }
    }
}

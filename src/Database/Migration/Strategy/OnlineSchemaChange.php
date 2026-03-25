<?php

namespace Utopia\Database\Migration\Strategy;

use Utopia\Database\Database;

class OnlineSchemaChange
{
    public function alter(Database $db, string $collection, callable $changes): void
    {
        $adapter = $db->getAdapter();

        $hadLocks = true;

        if (\method_exists($adapter, 'enableAlterLocks')) {
            $hadLocks = true;
            $adapter->enableAlterLocks(false);
        }

        try {
            $changes($db, $collection);
        } finally {
            if (\method_exists($adapter, 'enableAlterLocks') && $hadLocks) {
                $adapter->enableAlterLocks(true);
            }
        }
    }
}

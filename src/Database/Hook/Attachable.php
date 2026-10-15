<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Database;

/**
 * A hook that works through the database it is added to. {@see Database::addHook()} hands it that database before
 * registering it.
 */
interface Attachable
{
    public function attach(Database $database): void;
}

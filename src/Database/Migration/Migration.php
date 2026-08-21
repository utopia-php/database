<?php

namespace Utopia\Database\Migration;

use Utopia\Database\Database;

abstract class Migration
{
    abstract public function version(): string;

    abstract public function up(Database $db): void;

    abstract public function down(Database $db): void;

    public function name(): string
    {
        return static::class;
    }
}

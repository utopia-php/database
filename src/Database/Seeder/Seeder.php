<?php

namespace Utopia\Database\Seeder;

use Utopia\Database\Database;

abstract class Seeder
{
    /**
     * @return array<class-string<Seeder>>
     */
    public function dependencies(): array
    {
        return [];
    }

    abstract public function run(Database $db): void;
}

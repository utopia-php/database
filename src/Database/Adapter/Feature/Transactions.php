<?php

namespace Utopia\Database\Adapter\Feature;

interface Transactions
{
    public function startTransaction(): bool;

    public function commitTransaction(): bool;

    public function rollbackTransaction(): bool;
}

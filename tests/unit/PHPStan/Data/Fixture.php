<?php

namespace Tests\Unit\PHPStan\Data;

use Utopia\Database\PDOStatement;

class Fixture
{
    private bool $backing = false;

    public bool $flag {
        get => $this->backing;
        set {
            $this->backing = $value;
        }
    }

    public int $counter = 0 {
        get => $this->counter;
        set => $value + 1;
    }

    public bool $plain = false;

    public function read(): bool
    {
        return $this->flag;
    }

    public function write(): void
    {
        $this->flag = true;
    }

    public function plain(): bool
    {
        return $this->plain;
    }

    public function fromOutside(self $other): int
    {
        return $other->counter;
    }

    public function byName(self $other): bool
    {
        return $other->{'flag'};
    }

    public function statementRead(PDOStatement $statement): mixed
    {
        return $statement->queryString;
    }

    public function statementPrivateRead(PDOStatement $statement): mixed
    {
        return $statement->values;
    }

    public function statementWrite(PDOStatement $statement): void
    {
        $statement->custom = true;
    }

    public function statementMethod(PDOStatement $statement): int
    {
        return $statement->rowCount();
    }
}

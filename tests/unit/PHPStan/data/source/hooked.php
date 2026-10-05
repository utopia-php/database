<?php

namespace Tests\Unit\PHPStan\Data\Source;

class Hooked
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
}

function hookedFromOutside(Hooked $hooked): int
{
    return $hooked->counter;
}

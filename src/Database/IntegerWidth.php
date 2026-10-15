<?php

namespace Utopia\Database;

enum IntegerWidth
{
    case Bits32;
    case Bits64;

    private const int BITS64_SIZE = 8;

    public static function fromSize(?int $size): self
    {
        return $size !== null && $size >= self::BITS64_SIZE ? self::Bits64 : self::Bits32;
    }

    public function size(): ?int
    {
        return match ($this) {
            self::Bits32 => null,
            self::Bits64 => self::BITS64_SIZE,
        };
    }
}

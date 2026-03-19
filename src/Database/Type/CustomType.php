<?php

namespace Utopia\Database\Type;

use Utopia\Query\Schema\ColumnType;

interface CustomType
{
    public function name(): string;

    public function columnType(): ColumnType;

    public function columnSize(): int;

    public function encode(mixed $value): mixed;

    public function decode(mixed $value): mixed;
}

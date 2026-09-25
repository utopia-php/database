<?php

namespace Utopia\Database\Type;

interface Custom
{
    public function name(): string;

    public function encode(mixed $value): mixed;

    public function decode(mixed $value): mixed;
}

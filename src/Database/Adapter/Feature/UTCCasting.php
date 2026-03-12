<?php

namespace Utopia\Database\Adapter\Feature;

interface UTCCasting
{
    public function setUTCDatetime(string $value): mixed;
}

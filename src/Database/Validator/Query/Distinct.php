<?php

namespace Utopia\Database\Validator\Query;

class Distinct extends Base
{
    #[\Override]
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_DISTINCT;
    }
}

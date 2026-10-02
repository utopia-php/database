<?php

namespace Tests\Unit;

use Utopia\Database\Attribute;

final class CountingAttribute extends Attribute
{
    /**
     * @var list<string>
     */
    public array $magicReads = [];

    #[\Override]
    public function __get(string $name): mixed
    {
        $this->magicReads[] = $name;

        return parent::__get($name);
    }
}

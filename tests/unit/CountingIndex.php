<?php

namespace Tests\Unit;

use Utopia\Database\Index;
use Utopia\Query\Schema\IndexType;

final class CountingIndex extends Index
{
    private ?MagicAccessRecorder $recorder = null;

    public static function of(Index $index, MagicAccessRecorder $recorder): self
    {
        $copy = new self('', IndexType::Key);
        $copy->exchangeArray(\iterator_to_array($index));
        $copy->recorder = $recorder;

        return $copy;
    }

    #[\Override]
    public function __get(string $name): mixed
    {
        $this->recorder?->read('Index "'.$this->getKey().'"', $name);

        return parent::__get($name);
    }

    #[\Override]
    public function __set(string $name, mixed $value): void
    {
        $this->recorder?->write('Index "'.$this->getKey().'"', $name);

        parent::__set($name, $value);
    }
}

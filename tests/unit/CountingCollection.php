<?php

namespace Tests\Unit;

use Utopia\Database\Collection;

final class CountingCollection extends Collection
{
    private ?MagicAccessRecorder $recorder = null;

    public static function of(Collection $collection, MagicAccessRecorder $recorder): self
    {
        $copy = new self(metadata: $collection->metadata);
        $copy->exchangeArray(\iterator_to_array($collection));
        $copy->recorder = $recorder;

        return $copy;
    }

    #[\Override]
    public function __get(string $name): mixed
    {
        $this->recorder?->read('Collection "'.$this->getId().'"', $name);

        return parent::__get($name);
    }

    #[\Override]
    public function __set(string $name, mixed $value): void
    {
        $this->recorder?->write('Collection "'.$this->getId().'"', $name);

        parent::__set($name, $value);
    }
}

<?php

namespace Tests\Unit;

use Utopia\Database\Relationship;
use Utopia\Database\RelationType;

final class CountingRelationship extends Relationship
{
    private ?MagicAccessRecorder $recorder = null;

    public static function of(Relationship $relationship, MagicAccessRecorder $recorder): self
    {
        $copy = new self('', '', RelationType::OneToOne);
        $copy->exchangeArray(\iterator_to_array($relationship));
        $copy->recorder = $recorder;

        return $copy;
    }

    #[\Override]
    public function __get(string $name): mixed
    {
        $this->recorder?->read('Relationship "'.$this->getKey().'"', $name);

        return parent::__get($name);
    }

    #[\Override]
    public function __set(string $name, mixed $value): void
    {
        $this->recorder?->write('Relationship "'.$this->getKey().'"', $name);

        parent::__set($name, $value);
    }
}

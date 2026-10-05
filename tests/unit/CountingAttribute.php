<?php

namespace Tests\Unit;

use Utopia\Database\Attribute;

final class CountingAttribute extends Attribute
{
    /**
     * @var list<string>
     */
    public array $magicReads = [];

    private ?MagicAccessRecorder $recorder = null;

    public static function of(Attribute $attribute, MagicAccessRecorder $recorder): self
    {
        $copy = new self();
        $copy->exchangeArray(\iterator_to_array($attribute));
        $copy->recorder = $recorder;

        return $copy;
    }

    #[\Override]
    public function __get(string $name): mixed
    {
        $this->magicReads[] = $name;
        $this->recorder?->read('Attribute "'.$this->getKey().'"', $name);

        return parent::__get($name);
    }

    #[\Override]
    public function __set(string $name, mixed $value): void
    {
        $this->recorder?->write('Attribute "'.$this->getKey().'"', $name);

        parent::__set($name, $value);
    }
}

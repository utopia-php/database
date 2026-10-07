<?php

namespace Utopia\Database;

use Utopia\Query\Schema\ColumnType;

final readonly class AttributeUpdate
{
    /**
     * @param  list<Filter|string>|null  $filters
     */
    public function __construct(
        public ?ColumnType $type = null,
        public ?int $size = null,
        public ?bool $required = null,
        public mixed $default = Unchanged::Value,
        public ?bool $signed = null,
        public ?bool $array = null,
        public ?Format $format = null,
        public ?array $filters = null,
        public ?string $key = null,
    ) {
    }

    public function changesDefault(): bool
    {
        return $this->default !== Unchanged::Value;
    }

    public function isEmpty(): bool
    {
        return $this->type === null
            && $this->size === null
            && $this->required === null
            && ! $this->changesDefault()
            && $this->signed === null
            && $this->array === null
            && $this->format === null
            && $this->filters === null
            && $this->key === null;
    }
}

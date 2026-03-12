<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;
use Utopia\Query\Schema\ColumnType;

class Attribute
{
    public function __construct(
        public string $key = '',
        public ColumnType $type = ColumnType::String,
        public int $size = 0,
        public bool $required = false,
        public mixed $default = null,
        public bool $signed = true,
        public bool $array = false,
        public ?string $format = null,
        public array $formatOptions = [],
        public array $filters = [],
        public ?string $status = null,
        public ?array $options = null,
    ) {
    }

    public function toDocument(): Document
    {
        $data = [
            '$id' => ID::custom($this->key),
            'key' => $this->key,
            'type' => $this->type->value,
            'size' => $this->size,
            'required' => $this->required,
            'default' => $this->default,
            'signed' => $this->signed,
            'array' => $this->array,
            'format' => $this->format,
            'formatOptions' => $this->formatOptions,
            'filters' => $this->filters,
        ];

        if ($this->status !== null) {
            $data['status'] = $this->status;
        }

        if ($this->options !== null) {
            $data['options'] = $this->options;
        }

        return new Document($data);
    }

    public static function fromDocument(Document $document): self
    {
        return new self(
            key: $document->getAttribute('key', $document->getId()),
            type: ColumnType::from($document->getAttribute('type', 'string')),
            size: $document->getAttribute('size', 0),
            required: $document->getAttribute('required', false),
            default: $document->getAttribute('default'),
            signed: $document->getAttribute('signed', true),
            array: $document->getAttribute('array', false),
            format: $document->getAttribute('format'),
            formatOptions: $document->getAttribute('formatOptions', []),
            filters: $document->getAttribute('filters', []),
            status: $document->getAttribute('status'),
            options: $document->getAttribute('options'),
        );
    }

    /**
     * Create from an associative array (used by batch operations).
     *
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $type = $data['type'] ?? 'string';

        return new self(
            key: $data['$id'] ?? $data['key'] ?? '',
            type: $type instanceof ColumnType ? $type : ColumnType::from($type),
            size: $data['size'] ?? 0,
            required: $data['required'] ?? false,
            default: $data['default'] ?? null,
            signed: $data['signed'] ?? true,
            array: $data['array'] ?? false,
            format: $data['format'] ?? null,
            formatOptions: $data['formatOptions'] ?? [],
            filters: $data['filters'] ?? [],
        );
    }
}

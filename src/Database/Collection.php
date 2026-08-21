<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;

/**
 * A collection metadata document. Nested attributes and indexes are Attribute and Index models.
 */
class Collection extends Document
{
    /**
     * @param  array<string, mixed>|string  $id  Storage payload, or collection id for the named constructor
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     * @param  array<string>|null  $permissions  Null means default create-any; empty means none
     * @param  array<string, mixed>  $metadata
     */
    public function __construct(
        string|array $id = '',
        string $name = '',
        array $attributes = [],
        array $indexes = [],
        ?array $permissions = null,
        bool $documentSecurity = true,
        public array $metadata = [],
    ) {
        if (\is_array($id)) {
            if (\is_array($id['attributes'] ?? null)) {
                $id['attributes'] = self::castAttributes($id['attributes']);
            }
            if (\is_array($id['indexes'] ?? null)) {
                $id['indexes'] = self::castIndexes($id['indexes']);
            }
            parent::__construct($id);

            return;
        }

        $data = [
            self::ID => ID::custom($id),
            'name' => $name !== '' ? $name : $id,
            'attributes' => $attributes,
            'indexes' => $indexes,
            'documentSecurity' => $documentSecurity,
        ];
        if ($permissions !== null) {
            $data[self::PERMISSIONS] = $permissions;
        }

        parent::__construct(\array_merge($data, $this->metadata));
    }

    public function __get(string $name): mixed
    {
        return match ($name) {
            'id' => $this->getId(),
            'name' => (string) $this->getAttribute('name', $this->getId()),
            'attributes' => $this->getAttribute('attributes', []),
            'indexes' => $this->getAttribute('indexes', []),
            'permissions' => $this->offsetExists(self::PERMISSIONS) ? $this->getPermissions() : null,
            'documentSecurity' => (bool) $this->getAttribute('documentSecurity', true),
            default => $this->getAttribute($name),
        };
    }

    public function __set(string $name, mixed $value): void
    {
        match ($name) {
            'id' => $this->setAttribute(self::ID, $value),
            'name' => $this->setAttribute('name', $value),
            'attributes' => $this->setAttribute('attributes', $value),
            'indexes' => $this->setAttribute('indexes', $value),
            'permissions' => $this->setAttribute(self::PERMISSIONS, $value ?? []),
            'documentSecurity' => $this->setAttribute('documentSecurity', $value),
            default => $this->setAttribute($name, $value),
        };
    }

    public function __isset(string $name): bool
    {
        return match ($name) {
            'id', 'name', 'attributes', 'indexes', 'documentSecurity', 'metadata' => true,
            'permissions' => true,
            default => $this->offsetExists($name),
        };
    }

    public function toDocument(): Document
    {
        return $this;
    }

    public static function fromDocument(Document $document): self
    {
        if ($document instanceof self) {
            return $document;
        }

        return new self($document->getArrayCopy());
    }

    /**
     * @param  mixed  $attributes
     * @return array<Attribute>
     */
    private static function castAttributes(mixed $attributes): array
    {
        if (! \is_array($attributes)) {
            return [];
        }

        $cast = [];
        foreach ($attributes as $attr) {
            if ($attr instanceof Attribute) {
                $cast[] = $attr;

                continue;
            }
            if (! \is_array($attr)) {
                throw new \InvalidArgumentException('Collection attributes must be Attribute models');
            }
            $typed = [];
            foreach ($attr as $name => $item) {
                if (\is_string($name)) {
                    $typed[$name] = $item;
                }
            }
            $cast[] = Attribute::fromArray($typed);
        }

        return $cast;
    }

    /**
     * @param  mixed  $indexes
     * @return array<Index>
     */
    private static function castIndexes(mixed $indexes): array
    {
        if (! \is_array($indexes)) {
            return [];
        }

        $cast = [];
        foreach ($indexes as $idx) {
            if ($idx instanceof Index) {
                $cast[] = $idx;

                continue;
            }
            if (! \is_array($idx)) {
                throw new \InvalidArgumentException('Collection indexes must be Index models');
            }
            $typed = [];
            foreach ($idx as $name => $item) {
                if (\is_string($name)) {
                    $typed[$name] = $item;
                }
            }
            $cast[] = Index::fromArray($typed);
        }

        return $cast;
    }
}

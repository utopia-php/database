<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;

/**
 * A collection metadata document. Nested attributes and indexes are Attribute and Index models.
 *
 * @property string $id
 * @property string $name
 * @property array<Attribute> $attributes
 * @property array<Index> $indexes
 * @property array<string>|null $permissions
 * @property bool $documentSecurity
 * @property array<string, mixed> $metadata
 */
class Collection extends Document
{
    /**
     * @param  array<Attribute|Document|array<string, mixed>>  $attributes
     * @param  array<Index|Document|array<string, mixed>>  $indexes
     * @param  array<string>|null  $permissions  Null means default create-any; empty means none
     * @param  array<string, mixed>  $metadata
     */
    public function __construct(
        string $id = '',
        string $name = '',
        array $attributes = [],
        array $indexes = [],
        ?array $permissions = null,
        bool $documentSecurity = true,
        public array $metadata = [],
    ) {
        $data = [
            self::ID => ID::custom($id),
            'name' => $name !== '' ? $name : $id,
            'attributes' => self::castAttributes($attributes),
            'indexes' => self::castIndexes($indexes),
        ];
        if ($id !== '') {
            $data['documentSecurity'] = $documentSecurity;
        }
        if ($permissions !== null) {
            $data[self::PERMISSIONS] = $permissions;
        }

        parent::__construct(\array_merge($data, $this->metadata));
    }

    public function isEmpty(): bool
    {
        return $this->getId() === '';
    }

    /**
     * @param  array<string, mixed>  $data
     */
    public static function fromArray(array $data): self
    {
        $id = $data[self::ID] ?? '';
        if (! \is_string($id)) {
            $id = '';
        }

        $name = $data['name'] ?? $id;
        if (! \is_string($name)) {
            $name = $id;
        }

        $permissions = null;
        if (\array_key_exists(self::PERMISSIONS, $data) && \is_array($data[self::PERMISSIONS])) {
            /** @var array<string> $permissions */
            $permissions = $data[self::PERMISSIONS];
        }

        $rawAttributes = $data['attributes'] ?? [];
        $rawIndexes = $data['indexes'] ?? [];

        $collection = new self(
            id: $id,
            name: $name,
            attributes: \is_array($rawAttributes) ? self::castAttributes($rawAttributes) : [],
            indexes: \is_array($rawIndexes) ? self::castIndexes($rawIndexes) : [],
            permissions: $permissions,
            documentSecurity: (bool) ($data['documentSecurity'] ?? true),
        );

        if (\is_string($rawAttributes)) {
            $collection->setAttribute('attributes', $rawAttributes);
        }
        if (\is_string($rawIndexes)) {
            $collection->setAttribute('indexes', $rawIndexes);
        }

        foreach ($data as $key => $value) {
            if (\in_array($key, [self::ID, 'name', 'attributes', 'indexes', self::PERMISSIONS, 'documentSecurity'], true)) {
                continue;
            }
            $collection->setAttribute($key, $value);
        }

        return $collection;
    }

    /**
     * @return (
     *     $name is 'id' ? string :
     *     $name is 'name' ? string :
     *     $name is 'attributes' ? array<Attribute> :
     *     $name is 'indexes' ? array<Index> :
     *     $name is 'permissions' ? array<string>|null :
     *     $name is 'documentSecurity' ? bool :
     *     mixed
     * )
     */
    public function __get(string $name): mixed
    {
        switch ($name) {
            case 'id':
                return $this->getId();
            case 'name':
                /** @var string $value */
                $value = $this->getAttribute('name', $this->getId());

                return $value;
            case 'attributes':
                return self::castAttributes($this->getArray('attributes'));
            case 'indexes':
                return self::castIndexes($this->getArray('indexes'));
            case 'permissions':
                return $this->offsetExists(self::PERMISSIONS) ? $this->getPermissions() : null;
            case 'documentSecurity':
                return (bool) $this->getAttribute('documentSecurity', true);
            default:
                return $this->getAttribute($name);
        }
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
            'id', 'name', 'attributes', 'indexes', 'permissions', 'documentSecurity', 'metadata' => true,
            default => $this->offsetExists($name),
        };
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
            if ($attr instanceof Document) {
                $cast[] = Attribute::fromArray($attr->getArrayCopy());

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
            if ($idx instanceof Document) {
                $cast[] = Index::fromArray($idx->getArrayCopy());

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

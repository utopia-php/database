<?php

namespace Utopia\Database;

use ArrayObject;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Structure as StructureException;

/**
 * Represents a database document as an array-accessible object with support for nested documents and permissions.
 *
 * @extends ArrayObject<string, mixed>
 */
class Document extends ArrayObject
{
    public const string ID = '$id';

    public const string SEQUENCE = '$sequence';

    public const string COLLECTION = '$collection';

    public const string CREATED_AT = '$createdAt';

    public const string UPDATED_AT = '$updatedAt';

    public const string PERMISSIONS = '$permissions';

    public const string TENANT = '$tenant';

    public const string DISTANCE = '$distance';

    public const string DELETED_AT = '$deletedAt';

    /** @var array<string, true>|null */
    private static ?array $internalKeySet = null;

    /**
     * Keyed on the stored value it was parsed from: a write through any path (ArrayAccess, a reference,
     * exchangeArray(), unset) invalidates it without intercepting every write to the document.
     *
     * @var array{source: array<mixed>, permissions: list<string>, roles: array<string, list<string>>}|null
     */
    private ?array $parsedPermissions = null;

    /**
     * @return array<string, true>
     */
    private static function getInternalKeySet(): array
    {
        if (self::$internalKeySet === null) {
            self::$internalKeySet = [];
            foreach (Database::internalAttributesFor(true) as $attribute) {
                self::$internalKeySet[$attribute->key] = true;
            }
        }
        return self::$internalKeySet;
    }
    /**
     * Construct.
     *
     * Construct a new fields object
     *
     * @param  array<string, mixed>  $input
     *
     * @throws DatabaseException
     *
     * @see ArrayObject::__construct
     */
    public function __construct(array $input = [])
    {
        if (array_key_exists(self::ID, $input) && ! \is_string($input[self::ID])) {
            throw new StructureException(self::ID.' must be of type string');
        }

        if (array_key_exists(self::PERMISSIONS, $input)) {
            $input[self::PERMISSIONS] = self::normalizePermissions($input[self::PERMISSIONS]);
        }

        foreach ($input as $key => $value) {
            if (! \is_array($value)) {
                continue;
            }

            if (isset($value[self::ID]) || isset($value[self::COLLECTION])) {
                /** @var array<string, mixed> $value */
                $input[$key] = new self($value);

                continue;
            }

            $converted = false;
            foreach ($value as $childKey => $child) {
                if (\is_array($child) && (isset($child[self::ID]) || isset($child[self::COLLECTION]))) {
                    /** @var array<string, mixed> $child */
                    $value[$childKey] = new self($child);
                    $converted = true;
                }
            }

            if ($converted) {
                $input[$key] = $value;
            }
        }

        parent::__construct($input);
    }

    /**
     * @param  array<string, mixed>  $data
     */
    public static function fromArray(array $data): self
    {
        $class = static::class;

        return new $class($data);
    }

    /**
     * @internal
     *
     * @param  array<int|string, mixed>  $row
     *
     * @throws DatabaseException
     */
    final public static function fromRow(array $row): self
    {
        foreach (\array_keys($row) as $key) {
            if (\is_int($key)) {
                unset($row[$key]);
            }
        }

        if (array_key_exists(self::ID, $row)) {
            if ($row[self::ID] === null) {
                $row[self::ID] = '';
            } elseif (! \is_string($row[self::ID])) {
                throw new StructureException(self::ID.' must be of type string');
            }
        }

        if (array_key_exists(self::PERMISSIONS, $row)) {
            if (! \is_array($row[self::PERMISSIONS])) {
                throw new StructureException(self::PERMISSIONS.' must be of type array');
            }
            $permissions = [];
            foreach ($row[self::PERMISSIONS] as $permission) {
                if (\is_string($permission)) {
                    $permissions[] = $permission;
                }
            }
            $row[self::PERMISSIONS] = \array_values(\array_unique($permissions));
        }

        $document = new self();
        $document->exchangeArray($row);

        return $document;
    }

    /**
     * @internal
     *
     * @param  array<string, mixed>  $data
     *
     * @throws StructureException When $id is not a string or $permissions is not an array
     */
    final public static function fromStorage(array $data): self
    {
        if (array_key_exists(self::ID, $data) && ! \is_string($data[self::ID])) {
            throw new StructureException(self::ID.' must be of type string');
        }

        if (array_key_exists(self::PERMISSIONS, $data)) {
            if (! \is_array($data[self::PERMISSIONS])) {
                throw new StructureException(self::PERMISSIONS.' must be of type array');
            }
            $data[self::PERMISSIONS] = \array_values(\array_unique(\array_filter($data[self::PERMISSIONS], \is_string(...))));
        }

        foreach ($data as $key => $value) {
            if (! \is_array($value)) {
                continue;
            }

            if (isset($value[self::ID]) || isset($value[self::COLLECTION])) {
                /** @var array<string, mixed> $value */
                $data[$key] = self::fromStorage($value);

                continue;
            }

            $converted = false;
            foreach ($value as $childKey => $child) {
                if (\is_array($child) && (isset($child[self::ID]) || isset($child[self::COLLECTION]))) {
                    /** @var array<string, mixed> $child */
                    $value[$childKey] = self::fromStorage($child);
                    $converted = true;
                }
            }

            if ($converted) {
                $data[$key] = $value;
            }
        }

        $document = new self();
        $document->exchangeArray($data);

        return $document;
    }

    /**
     * Get the document's unique identifier.
     *
     * @return string The document ID, or empty string if not set.
     */
    public function getId(): string
    {
        /** @var string $id */
        $id = $this->getAttribute(self::ID, '');
        return $id;
    }

    /**
     * Get the document's auto-generated sequence identifier.
     *
     * @return string|null The sequence value, or null if not set.
     */
    public function getSequence(): ?string
    {
        $sequence = $this->getAttribute(self::SEQUENCE);

        if ($sequence === null) {
            return null;
        }

        /** @var string $sequence */
        return $sequence;
    }

    /**
     * Get the collection ID this document belongs to.
     *
     * @return string The collection ID, or empty string if not set.
     */
    public function getCollection(): string
    {
        /** @var string $collection */
        $collection = $this->getAttribute(self::COLLECTION, '');
        return $collection;
    }

    /**
     * Get all unique permissions assigned to this document.
     *
     * @return list<string>
     *
     * @throws StructureException When the stored permissions are not an array of strings
     */
    public function getPermissions(): array
    {
        return $this->parsePermissions()['permissions'];
    }

    /**
     * Get roles for a specific permission type from this document's permissions.
     *
     * @param PermissionType|string $type A built-in permission type, or a consumer-defined type such as 'execute'
     * @return list<string>
     *
     * @throws StructureException When the stored permissions are not an array of strings
     */
    public function getPermissionsByType(PermissionType|string $type): array
    {
        $type = $type instanceof PermissionType ? $type->value : $type;

        return $this->parsePermissions()['roles'][$type] ?? [];
    }

    /**
     * @return array{source: array<mixed>, permissions: list<string>, roles: array<string, list<string>>}
     *
     * @throws StructureException
     */
    private function parsePermissions(): array
    {
        $source = $this->getAttribute(self::PERMISSIONS, []);

        if ($this->parsedPermissions !== null && $this->parsedPermissions['source'] === $source) {
            return $this->parsedPermissions;
        }

        $permissions = self::normalizePermissions($source);

        $roles = [];
        foreach ($permissions as $permission) {
            $open = \strpos($permission, '(');
            if ($open === false) {
                continue;
            }
            $type = \trim(\substr($permission, 0, $open));
            $roles[$type][] = \str_replace([')', '"', ' '], '', \substr($permission, $open + 1));
        }

        return $this->parsedPermissions = [
            'source' => $source,
            'permissions' => $permissions,
            'roles' => \array_map(static fn (array $names): array => \array_values(\array_unique($names)), $roles),
        ];
    }

    /**
     * @return list<string>
     *
     * @phpstan-assert array<mixed> $permissions
     *
     * @throws StructureException
     */
    private static function normalizePermissions(mixed $permissions): array
    {
        if (! \is_array($permissions)) {
            throw new StructureException(self::PERMISSIONS.' must be of type array');
        }

        $strings = [];
        foreach ($permissions as $permission) {
            if (! \is_string($permission)) {
                throw new StructureException('Every permission must be of type string');
            }
            $strings[] = $permission;
        }

        return \array_values(\array_unique($strings));
    }

    /**
     * Get the document's creation timestamp.
     *
     * @return string|null The creation datetime string, or null if not set.
     */
    public function getCreatedAt(): ?string
    {
        /** @var string|null $createdAt */
        $createdAt = $this->getAttribute(self::CREATED_AT);
        return $createdAt;
    }

    /**
     * Get the document's last update timestamp.
     *
     * @return string|null The update datetime string, or null if not set.
     */
    public function getUpdatedAt(): ?string
    {
        /** @var string|null $updatedAt */
        $updatedAt = $this->getAttribute(self::UPDATED_AT);
        return $updatedAt;
    }

    /**
     * Get the tenant ID associated with this document.
     *
     * Numeric string values are normalized to int for consistent comparison
     * across adapters that may return string representations (e.g. PDO stringify).
     *
     * @return int|string|null The tenant ID, or null if not set.
     */
    public function getTenant(): int|string|null
    {
        $tenant = $this->getAttribute(self::TENANT);

        if (\is_string($tenant) && \ctype_digit($tenant)) {
            return (int) $tenant;
        }

        if (\is_int($tenant) || \is_string($tenant) || $tenant === null) {
            return $tenant;
        }

        return null;
    }

    /**
     * Get Document Attributes
     *
     * @return array<string, mixed>
     */
    public function getAttributes(): array
    {
        $attributes = [];
        $keySet = self::getInternalKeySet();

        foreach ($this as $attribute => $value) {
            if (isset($keySet[$attribute])) {
                continue;
            }

            $attributes[$attribute] = $value;
        }

        return $attributes;
    }

    /**
     * Get Attribute.
     *
     * Method for getting a specific fields attribute. If $name is not found $default value will be returned.
     */
    public function getAttribute(string $name, mixed $default = null): mixed
    {
        if (isset($this[$name])) {
            return $this[$name];
        }

        return $default;
    }

    /**
     * @return array<int|string, mixed>
     */
    public function getArray(string $key): array
    {
        $value = $this->offsetExists($key) ? $this[$key] : [];

        return \is_array($value) ? $value : [];
    }

    /**
     * @return list<self>
     */
    public function getDocuments(string $key): array
    {
        $documents = [];
        foreach ($this->getArray($key) as $item) {
            if ($item instanceof self) {
                $documents[] = $item;
                continue;
            }
            if (! \is_array($item)) {
                continue;
            }
            $typed = [];
            foreach ($item as $name => $value) {
                if (\is_string($name)) {
                    $typed[$name] = $value;
                }
            }
            $documents[] = new self($typed);
        }

        return $documents;
    }

    public function getDocument(string $key): self
    {
        $value = $this->offsetExists($key) ? $this[$key] : null;
        if ($value instanceof self) {
            return $value;
        }
        if (! \is_array($value) || $value === [] || \array_is_list($value)) {
            return new self();
        }

        $typed = [];
        foreach ($value as $name => $item) {
            if (\is_string($name)) {
                $typed[$name] = $item;
            }
        }

        return new self($typed);
    }

    /**
     * Set Attribute.
     *
     * Method for setting a specific field attribute
     *
     * @throws StructureException When $permissions is set to something other than null or an array of strings
     */
    public function setAttribute(string $key, mixed $value, SetType $type = SetType::Assign): static
    {
        if ($type !== SetType::Assign) {
            $current = $this->getArray($key);
            $value = match ($type) {
                SetType::Append => [...$current, $value],
                SetType::Prepend => [$value, ...$current],
            };
        }

        if ($key === self::PERMISSIONS && $value !== null) {
            $value = self::normalizePermissions($value);
        }

        $this[$key] = $value;

        return $this;
    }

    /**
     * Set Attributes.
     *
     * @param  array<string, mixed>  $attributes
     */
    public function setAttributes(array $attributes): static
    {
        foreach ($attributes as $key => $value) {
            $this->setAttribute($key, $value);
        }

        return $this;
    }

    /**
     * Remove Attribute.
     *
     * Method for removing a specific field attribute
     */
    public function removeAttribute(string $key): static
    {
        $this->offsetUnset($key);

        return $this;
    }

    /**
     * Checks if document has data.
     */
    public function isEmpty(): bool
    {
        return ! \count($this);
    }

    /**
     * Checks if a document key is set.
     */
    public function isSet(string $key): bool
    {
        return isset($this[$key]);
    }

    /**
     * The document as a PHP array, with every nested document converted to an array too.
     *
     * @return array<string, mixed>
     */
    #[\Override]
    public function getArrayCopy(): array
    {
        return self::export(parent::getArrayCopy());
    }

    /**
     * The given top-level keys of getArrayCopy(), in the document's order.
     *
     * @param  array<string>  $keys
     * @return array<string, mixed>
     */
    public function only(array $keys): array
    {
        return self::export(\array_intersect_key(parent::getArrayCopy(), \array_flip($keys)));
    }

    /**
     * getArrayCopy() without the given top-level keys.
     *
     * @param  array<string>  $keys
     * @return array<string, mixed>
     */
    public function except(array $keys): array
    {
        return self::export(\array_diff_key(parent::getArrayCopy(), \array_flip($keys)));
    }

    /**
     * @param  array<string, mixed>  $values
     * @return array<string, mixed>
     */
    private static function export(array $values): array
    {
        $output = [];
        foreach ($values as $key => $value) {
            if ($value instanceof self) {
                $output[$key] = $value->getArrayCopy();
            } elseif (\is_array($value)) {
                $items = [];
                foreach ($value as $index => $item) {
                    $items[$index] = $item instanceof self ? $item->getArrayCopy() : $item;
                }
                $output[$key] = $items;
            } else {
                $output[$key] = $value;
            }
        }

        return $output;
    }

    /**
     * Deep clone the document including nested Document instances.
     */
    public function __clone()
    {
        foreach (parent::getArrayCopy() as $key => $value) {
            if ($value instanceof self) {
                $this[$key] = clone $value;

                continue;
            }

            if (! \is_array($value) || $value === []) {
                continue;
            }

            $copy = [];
            foreach ($value as $index => $item) {
                $copy[$index] = $item instanceof self ? clone $item : $item;
            }
            $this[$key] = $copy;
        }
    }
}

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

    public const string VERSION = '$version';

    public const string DISTANCE = '$distance';

    public const string DELETED_AT = '$deletedAt';

    public const string SKIP_PERMISSIONS_UPDATE = '$skipPermissionsUpdate';

    public const string INTERNAL_ID = '$internalId';

    /** @var array<string, true>|null */
    private static ?array $internalKeySet = null;

    /** @var array<string, list<string>>|null */
    private ?array $parsedPermissions = null;

    /**
     * @return array<string, true>
     */
    private static function getInternalKeySet(): array
    {
        if (self::$internalKeySet === null) {
            self::$internalKeySet = [];
            foreach (Database::internalAttributes() as $attr) {
                self::$internalKeySet[$attr->key] = true;
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

        if (array_key_exists(self::PERMISSIONS, $input) && ! is_array($input[self::PERMISSIONS])) {
            throw new StructureException(self::PERMISSIONS.' must be of type array');
        }

        if (array_key_exists(self::PERMISSIONS, $input) && is_array($input[self::PERMISSIONS])) {
            $permissions = [];
            foreach ($input[self::PERMISSIONS] as $permission) {
                if (\is_string($permission)) {
                    $permissions[] = $permission;
                }
            }
            $input[self::PERMISSIONS] = \array_values(\array_unique($permissions));
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

            foreach ($value as $childKey => $child) {
                if (\is_array($child) && (isset($child[self::ID]) || isset($child[self::COLLECTION]))) {
                    /** @var array<string, mixed> $child */
                    $value[$childKey] = new self($child);
                }
            }

            $input[$key] = $value;
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
     * Construct from a raw PDO row.
     *
     * Fast path that skips nested-Document detection. Raw PDO rows from
     * `$stmt->fetch()` only contain scalars or JSON-encoded strings — there
     * are never nested arrays carrying `$id`/`$collection`, so the
     * nested-detection foreach in the constructor is pure waste per row.
     *
     * Callers that build documents from relationship-resolved trees or
     * arbitrary user input must continue to use the regular constructor.
     *
     * @param  array<string, mixed>  $row
     *
     * @throws DatabaseException
     */
    public static function fromRow(array $row): self
    {
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
     * @return array<string>
     */
    public function getPermissions(): array
    {
        /** @var array<string> $permissions */
        $permissions = $this->getAttribute(self::PERMISSIONS, []);
        return $permissions;
    }

    /**
     * Get roles with read permission on this document.
     *
     * @return array<string>
     */
    public function getRead(): array
    {
        return $this->getPermissionsByType(PermissionType::Read);
    }

    /**
     * Get roles with create permission on this document.
     *
     * @return array<string>
     */
    public function getCreate(): array
    {
        return $this->getPermissionsByType(PermissionType::Create);
    }

    /**
     * Get roles with update permission on this document.
     *
     * @return array<string>
     */
    public function getUpdate(): array
    {
        return $this->getPermissionsByType(PermissionType::Update);
    }

    /**
     * Get roles with delete permission on this document.
     *
     * @return array<string>
     */
    public function getDelete(): array
    {
        return $this->getPermissionsByType(PermissionType::Delete);
    }

    /**
     * Get roles with full write permission (create, update, and delete) on this document.
     *
     * @return array<string>
     */
    public function getWrite(): array
    {
        return \array_unique(\array_intersect(
            $this->getCreate(),
            $this->getUpdate(),
            $this->getDelete()
        ));
    }

    /**
     * Get roles for a specific permission type from this document's permissions.
     *
     * @param PermissionType $type The permission type.
     * @return array<string>
     */
    public function getPermissionsByType(PermissionType $type): array
    {
        if ($this->parsedPermissions === null) {
            $this->parsedPermissions = [];
            foreach ($this->getPermissions() as $permission) {
                foreach (PermissionType::cases() as $permissionType) {
                    $t = $permissionType->value;
                    if (\str_starts_with($permission, $t)) {
                        $this->parsedPermissions[$t][] = \str_replace([$t.'(', ')', '"', ' '], '', $permission);
                        break;
                    }
                }
            }
            foreach ($this->parsedPermissions as &$roles) {
                $roles = \array_values(\array_unique($roles));
            }
        }
        return $this->parsedPermissions[$type->value] ?? [];
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

        if (\is_string($tenant) && \ctype_digit($tenant) && (string) (int) $tenant === $tenant) {
            return (int) $tenant;
        }

        if (\is_int($tenant) || \is_string($tenant) || $tenant === null) {
            return $tenant;
        }

        return null;
    }

    /**
     * Get the document's optimistic locking version.
     *
     * @return int|null The version number, or null if not set.
     */
    public function getVersion(): ?int
    {
        $version = $this->getAttribute(self::VERSION);

        if ($version === null) {
            return null;
        }

        /** @var int $version */
        return $version;
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
     */
    public function setAttribute(string $key, mixed $value, SetType $type = SetType::Assign): static
    {
        // Fast path for the dominant Assign case — skip the match dispatch
        // and the type-comparison branches that only matter for Append/Prepend.
        if ($type === SetType::Assign) {
            $this[$key] = $value;
        } else {
            $this[$key] = (! isset($this[$key]) || ! \is_array($this[$key])) ? [] : $this[$key];

            match ($type) {
                SetType::Append => $this[$key] = [...(array) $this[$key], $value],
                SetType::Prepend => $this[$key] = [$value, ...(array) $this[$key]],
            };
        }

        if ($key === self::PERMISSIONS) {
            if (\is_array($this[$key])) {
                $permissions = [];
                foreach ($this[$key] as $permission) {
                    if (\is_string($permission)) {
                        $permissions[] = $permission;
                    }
                }
                $this[$key] = \array_values(\array_unique($permissions));
            }
            $this->parsedPermissions = null;
        }

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
     * Find.
     *
     * @param  mixed  $find
     */
    public function find(string $key, $find, string $subject = ''): mixed
    {
        $subjectData = !empty($subject) ? ($this[$subject] ?? null) : null;
        /** @var array<mixed>|self $resolved */
        $resolved = (empty($subjectData)) ? $this : $subjectData;

        if (is_array($resolved)) {
            foreach ($resolved as $i => $value) {
                if (\is_array($value) && isset($value[$key]) && $value[$key] === $find) {
                    return $value;
                }
                if ($value instanceof self && isset($value[$key]) && $value[$key] === $find) {
                    return $value;
                }
            }

            return false;
        }

        if (isset($resolved[$key]) && $resolved[$key] === $find) {
            return $resolved;
        }

        return false;
    }

    /**
     * Find and Replace.
     *
     * Get array child by key and value match
     *
     * @param  mixed  $find
     * @param  mixed  $replace
     */
    public function findAndReplace(string $key, $find, $replace, string $subject = ''): bool
    {
        if (!empty($subject) && isset($this[$subject]) && \is_array($this[$subject])) {
            /** @var array<mixed> $subjectArray */
            $subjectArray = &$this[$subject];
            foreach ($subjectArray as $i => &$value) {
                if (\is_array($value) && isset($value[$key]) && $value[$key] === $find) {
                    $value = $replace;
                    return true;
                }
                if ($value instanceof self && isset($value[$key]) && $value[$key] === $find) {
                    $subjectArray[$i] = $replace;
                    return true;
                }
            }
            return false;
        }

        /** @var self $resolved */
        $resolved = $this;
        foreach ($resolved as $i => $value) {
            if (\is_array($value) && isset($value[$key]) && $value[$key] === $find) {
                $resolved[$i] = $replace;
                return true;
            }
            if ($value instanceof self && isset($value[$key]) && $value[$key] === $find) {
                $resolved[$i] = $replace;
                return true;
            }
        }

        if (isset($resolved[$key]) && $resolved[$key] === $find) {
            $resolved[$key] = $replace;
            return true;
        }

        return false;
    }

    /**
     * Find and Remove.
     *
     * Get array child by key and value match
     *
     * @param  mixed  $find
     */
    public function findAndRemove(string $key, $find, string $subject = ''): bool
    {
        if (!empty($subject) && isset($this[$subject]) && \is_array($this[$subject])) {
            /** @var array<mixed> $subjectArray */
            $subjectArray = &$this[$subject];
            foreach ($subjectArray as $i => &$value) {
                if (\is_array($value) && isset($value[$key]) && $value[$key] === $find) {
                    unset($subjectArray[$i]);
                    return true;
                }
                if ($value instanceof self && isset($value[$key]) && $value[$key] === $find) {
                    unset($subjectArray[$i]);
                    return true;
                }
            }
            return false;
        }

        /** @var self $resolved */
        $resolved = $this;
        foreach ($resolved as $i => $value) {
            if (\is_array($value) && isset($value[$key]) && $value[$key] === $find) {
                unset($resolved[$i]);
                return true;
            }
            if ($value instanceof self && isset($value[$key]) && $value[$key] === $find) {
                unset($resolved[$i]);
                return true;
            }
        }

        if (isset($resolved[$key]) && $resolved[$key] === $find) {
            unset($resolved[$key]);
            return true;
        }

        return false;
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
     * Get Array Copy.
     *
     * Outputs entity as a PHP array
     *
     * @param  array<string>  $allow
     * @param  array<string>  $disallow
     * @return array<string, mixed>
     */
    public function getArrayCopy(array $allow = [], array $disallow = []): array
    {
        $array = parent::getArrayCopy();

        $output = [];

        foreach ($array as $key => &$value) {
            if (! empty($allow) && ! \in_array($key, $allow)) { // Export only allow fields
                continue;
            }

            if (! empty($disallow) && \in_array($key, $disallow)) { // Don't export disallowed fields
                continue;
            }

            if ($value instanceof self) {
                $output[$key] = $value->getArrayCopy($allow, $disallow);
            } elseif (\is_array($value)) {
                if (empty($value)) {
                    $output[$key] = $value;
                } else {
                    $childOutput = [];
                    foreach ($value as $childKey => $child) {
                        if ($child instanceof self) {
                            $childOutput[$childKey] = $child->getArrayCopy($allow, $disallow);
                        } else {
                            $childOutput[$childKey] = $child;
                        }
                    }
                    $output[$key] = $childOutput;
                }
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
        foreach ($this as $key => $value) {
            if ($value instanceof self) {
                $this[$key] = clone $value;
            } elseif (\is_array($value)) {
                $this[$key] = \array_map(fn ($item) => $item instanceof self ? clone $item : $item, $value);
            }
        }
    }
}

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
        if (array_key_exists('$id', $input) && ! \is_string($input['$id'])) {
            throw new StructureException('$id must be of type string');
        }

        if (array_key_exists('$permissions', $input) && ! is_array($input['$permissions'])) {
            throw new StructureException('$permissions must be of type array');
        }

        foreach ($input as $key => $value) {
            if (! \is_array($value)) {
                continue;
            }

            if (isset($value['$id']) || isset($value['$collection'])) {
                /** @var array<string, mixed> $value */
                $input[$key] = new self($value);

                continue;
            }

            foreach ($value as $childKey => $child) {
                if (\is_array($child) && (isset($child['$id']) || isset($child['$collection']))) {
                    /** @var array<string, mixed> $child */
                    $value[$childKey] = new self($child);
                }
            }

            $input[$key] = $value;
        }

        parent::__construct($input);
    }

    /**
     * Get the document's unique identifier.
     *
     * @return string The document ID, or empty string if not set.
     */
    public function getId(): string
    {
        /** @var string $id */
        $id = $this->getAttribute('$id', '');
        return $id;
    }

    /**
     * Get the document's auto-generated sequence identifier.
     *
     * @return string|null The sequence value, or null if not set.
     */
    public function getSequence(): ?string
    {
        $sequence = $this->getAttribute('$sequence');

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
        $collection = $this->getAttribute('$collection', '');
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
        $permissions = $this->getAttribute('$permissions', []);
        return \array_values(\array_unique($permissions));
    }

    /**
     * Get roles with read permission on this document.
     *
     * @return array<string>
     */
    public function getRead(): array
    {
        return $this->getPermissionsByType(PermissionType::Read->value);
    }

    /**
     * Get roles with create permission on this document.
     *
     * @return array<string>
     */
    public function getCreate(): array
    {
        return $this->getPermissionsByType(PermissionType::Create->value);
    }

    /**
     * Get roles with update permission on this document.
     *
     * @return array<string>
     */
    public function getUpdate(): array
    {
        return $this->getPermissionsByType(PermissionType::Update->value);
    }

    /**
     * Get roles with delete permission on this document.
     *
     * @return array<string>
     */
    public function getDelete(): array
    {
        return $this->getPermissionsByType(PermissionType::Delete->value);
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
     * @param string $type The permission type (e.g., 'read', 'create', 'update', 'delete').
     * @return array<string>
     */
    public function getPermissionsByType(string $type): array
    {
        $typePermissions = [];

        foreach ($this->getPermissions() as $permission) {
            if (! \str_starts_with($permission, $type)) {
                continue;
            }
            $typePermissions[] = \str_replace([$type.'(', ')', '"', ' '], '', $permission);
        }

        return \array_unique($typePermissions);
    }

    /**
     * Get the document's creation timestamp.
     *
     * @return string|null The creation datetime string, or null if not set.
     */
    public function getCreatedAt(): ?string
    {
        /** @var string|null $createdAt */
        $createdAt = $this->getAttribute('$createdAt');
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
        $updatedAt = $this->getAttribute('$updatedAt');
        return $updatedAt;
    }

    /**
     * Get the tenant ID associated with this document.
     *
     * @return int|null The tenant ID, or null if not set.
     */
    public function getTenant(): ?int
    {
        $tenant = $this->getAttribute('$tenant');

        if ($tenant === null) {
            return null;
        }

        /** @var int $tenant */
        return $tenant;
    }

    /**
     * Get Document Attributes
     *
     * @return array<string, mixed>
     */
    public function getAttributes(): array
    {
        $attributes = [];

        $internalKeys = \array_map(
            fn (Attribute $attr) => $attr->key,
            Database::internalAttributes()
        );

        foreach ($this as $attribute => $value) {
            if (\in_array($attribute, $internalKeys)) {
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
     * Set Attribute.
     *
     * Method for setting a specific field attribute
     */
    public function setAttribute(string $key, mixed $value, SetType $type = SetType::Assign): static
    {
        if ($type !== SetType::Assign) {
            $this[$key] = (! isset($this[$key]) || ! \is_array($this[$key])) ? [] : $this[$key];
        }

        match ($type) {
            SetType::Assign => $this[$key] = $value,
            SetType::Append => $this[$key] = [...(array) $this[$key], $value],
            SetType::Prepend => $this[$key] = [$value, ...(array) $this[$key]],
        };

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

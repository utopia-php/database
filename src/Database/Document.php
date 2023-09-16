<?php

namespace Utopia\Database;

use ArrayObject;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

/**
 * @extends ArrayObject<string, mixed>
 */
class Document extends ArrayObject
{
    public const SET_TYPE_ASSIGN = 'assign';
    public const SET_TYPE_PREPEND = 'prepend';
    public const SET_TYPE_APPEND = 'append';

    /**
     * Construct.
     *
     * Construct a new fields object
     *
     * @param array<string, mixed> $input
     * @throws DatabaseException
     * @see ArrayObject::__construct
     *
     */
    public function __construct(array $input = [])
    {
        if (isset($input['$permissions']) && !is_array($input['$permissions'])) {
            throw new DatabaseException('$permissions must be of type array');
        }

        foreach ($input as $key => &$value) {
            if (!\is_array($value)) {
                continue;
            }
            if ((isset($value['$id']) || isset($value['$collection']))) {
                $input[$key] = new self($value);
                continue;
            }
            foreach ($value as $childKey => $child) {
                if ((isset($child['$id']) || isset($child['$collection'])) && (!$child instanceof self)) {
                    $value[$childKey] = new self($child);
                }
            }
        }

        parent::__construct($input);
    }

    /**
     * @return string
     */
    public function getId(): string
    {
        return $this->getAttribute('$id', '');
    }

    /**
     * @return string
     */
    public function getInternalId(): string
    {
        return $this->getAttribute('$internalId', '');
    }

    /**
     * @return string
     */
    public function getCollection(): string
    {
        return $this->getAttribute('$collection', '');
    }

    /**
     * @return array<string>
     */
    public function getPermissions(): array
    {
        return \array_values(\array_unique($this->getAttribute('$permissions', [])));
    }

    /**
     * @return array<string>
     */
    public function getRead(): array
    {
        return $this->getPermissionsByType(Database::PERMISSION_READ);
    }

    /**
     * @return array<string>
     */
    public function getCreate(): array
    {
        return $this->getPermissionsByType(Database::PERMISSION_CREATE);
    }

    /**
     * @return array<string>
     */
    public function getUpdate(): array
    {
        return $this->getPermissionsByType(Database::PERMISSION_UPDATE);
    }

    /**
     * @return array<string>
     */
    public function getDelete(): array
    {
        return $this->getPermissionsByType(Database::PERMISSION_DELETE);
    }

    /**
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
     * @return array<string>
     */
    public function getPermissionsByType(string $type): array
    {
        $typePermissions = [];

        foreach ($this->getPermissions() as $permission) {
            if (!\str_starts_with($permission, $type)) {
                continue;
            }
            $typePermissions[] = \str_replace([$type . '(', ')', '"', ' '], '', $permission);
        }

        return \array_unique($typePermissions);
    }

    /**
     * @return string|null
     */
    public function getCreatedAt(): ?string
    {
        return $this->getAttribute('$createdAt');
    }

    /**
     * @return string|null
     */
    public function getUpdatedAt(): ?string
    {
        return $this->getAttribute('$updatedAt');
    }

    /**
     * Get Document Attributes
     *
     * @return array<string, mixed>
     */
    public function getAttributes(): array
    {
        $attributes = [];

        foreach ($this as $attribute => $value) {
            if (\array_key_exists($attribute, [
                '$id' => true,
                '$internalId' => true,
                '$collection' => true,
                '$permissions' => [Permission::read(Role::any())],
                '$createdAt' => true,
                '$updatedAt' => true,
            ])) {
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
     *
     * @param string $name
     * @param mixed $default
     *
     * @return mixed
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
     *
     * @param string $key
     * @param mixed $value
     * @param string $type
     *
     * @return self
     */
    public function setAttribute(string $key, mixed $value, string $type = self::SET_TYPE_ASSIGN): self
    {
        switch ($type) {
            case self::SET_TYPE_ASSIGN:
                $this[$key] = $value;
                break;
            case self::SET_TYPE_APPEND:
                $this[$key] = (!isset($this[$key]) || !\is_array($this[$key])) ? [] : $this[$key];
                \array_push($this[$key], $value);
                break;
            case self::SET_TYPE_PREPEND:
                $this[$key] = (!isset($this[$key]) || !\is_array($this[$key])) ? [] : $this[$key];
                \array_unshift($this[$key], $value);
                break;
        }

        return $this;
    }

    /**
     * Set Attributes.
     *
     * @param array<string, mixed> $attributes
     * @return self
     */
    public function setAttributes(array $attributes): self
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
     *
     * @param string $key
     *
     * @return self
     */
    public function removeAttribute(string $key): self
    {
        if (\array_key_exists($key, (array)$this)) {
            unset($this[$key]);
        }

        return $this;
    }

    /**
     * Find.
     *
     * @param string $key
     * @param mixed $find
     * @param string $subject
     *
     * @return mixed
     */
    public function find(string $key, $find, string $subject = ''): mixed
    {
        $subject = $this[$subject] ?? null;
        $subject = (empty($subject)) ? $this : $subject;

        if (is_array($subject)) {
            foreach ($subject as $i => $value) {
                if (isset($value[$key]) && $value[$key] === $find) {
                    return $value;
                }
            }
            return false;
        }

        if (isset($subject[$key]) && $subject[$key] === $find) {
            return $subject;
        }
        return false;
    }

    /**
     * Find and Replace.
     *
     * Get array child by key and value match
     *
     * @param string $key
     * @param mixed $find
     * @param mixed $replace
     * @param string $subject
     *
     * @return bool
     */
    public function findAndReplace(string $key, $find, $replace, string $subject = ''): bool
    {
        $subject = &$this[$subject] ?? null;
        $subject = (empty($subject)) ? $this : $subject;

        if (is_array($subject)) {
            foreach ($subject as $i => &$value) {
                if (isset($value[$key]) && $value[$key] === $find) {
                    $value = $replace;
                    return true;
                }
            }
            return false;
        }

        if (isset($subject[$key]) && $subject[$key] === $find) {
            $subject[$key] = $replace;
            return true;
        }
        return false;
    }

    /**
     * Find and Remove.
     *
     * Get array child by key and value match
     *
     * @param string $key
     * @param mixed $find
     * @param string $subject
     *
     * @return bool
     */
    public function findAndRemove(string $key, $find, string $subject = ''): bool
    {
        $subject = &$this[$subject] ?? null;
        $subject = (empty($subject)) ? $this : $subject;

        if (is_array($subject)) {
            foreach ($subject as $i => &$value) {
                if (isset($value[$key]) && $value[$key] === $find) {
                    unset($subject[$i]);
                    return true;
                }
            }
            return false;
        }

        if (isset($subject[$key]) && $subject[$key] === $find) {
            unset($subject[$key]);
            return true;
        }
        return false;
    }

    /**
     * Checks if document has data.
     *
     * @return bool
     */
    public function isEmpty(): bool
    {
        return !\count($this);
    }

    /**
     * Checks if a document key is set.
     *
     * @param string $key
     *
     * @return bool
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
     * @param array<string> $allow
     * @param array<string> $disallow
     *
     * @return array<string, mixed>
     */
    public function getArrayCopy(array $allow = [], array $disallow = []): array
    {
        $array = parent::getArrayCopy();

        $output = [];

        foreach ($array as $key => &$value) {
            if (!empty($allow) && !\in_array($key, $allow)) { // Export only allow fields
                continue;
            }

            if (!empty($disallow) && \in_array($key, $disallow)) { // Don't export disallowed fields
                continue;
            }

            if ($value instanceof self) {
                $output[$key] = $value->getArrayCopy($allow, $disallow);
            } elseif (\is_array($value)) {
                foreach ($value as $childKey => &$child) {
                    if ($child instanceof self) {
                        $output[$key][$childKey] = $child->getArrayCopy($allow, $disallow);
                    } else {
                        $output[$key][$childKey] = $child;
                    }
                }

                if (empty($value)) {
                    $output[$key] = $value;
                }
            } else {
                $output[$key] = $value;
            }
        }

        return $output;
    }

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

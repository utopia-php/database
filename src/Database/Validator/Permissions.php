<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Helpers\Permission;

class Permissions extends Roles
{
    protected string $message = 'Permissions Error';

    /**
     * @var array<string>
     */
    protected array $allowed;

    protected int $length;

    /**
     * @var array<string>
     */
    protected array $columns;

    protected Key $key;

    /**
     * Permissions constructor.
     *
     * @param int $length maximum amount of permissions. 0 means unlimited.
     * @param array<string> $allowed allowed permissions. Defaults to all available.
     * @param array<string> $columns known column keys a permission may be scoped to. Empty means any valid key.
     */
    public function __construct(int $length = 0, array $allowed = [...Database::PERMISSIONS, Database::PERMISSION_WRITE], array $columns = [])
    {
        $this->length = $length;
        $this->allowed = $allowed;
        $this->columns = $columns;
        $this->key = new Key();
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param mixed $permissions
     *
     * @return bool
     */
    public function isValid($permissions): bool
    {
        if (!\is_array($permissions)) {
            $this->message = 'Permissions must be an array of strings.';
            return false;
        }

        if ($this->length && \count($permissions) > $this->length) {
            $this->message = 'You can only provide up to ' . $this->length . ' permissions.';
            return false;
        }

        foreach ($permissions as $permission) {
            if (!\is_string($permission)) {
                $this->message = 'Every permission must be of type string.';
                return false;
            }

            if ($permission === '*') {
                $this->message = 'Wildcard permission "*" has been replaced. Use "any" instead.';
                return false;
            }

            if (\str_contains($permission, 'role:')) {
                $this->message = 'Permissions using the "role:" prefix have been replaced. Use "users", "guests", or "any" instead.';
                return false;
            }

            $isAllowed = false;
            foreach ($this->allowed as $allowed) {
                if (\str_starts_with($permission, $allowed)) {
                    $isAllowed = true;
                    break;
                }
            }
            if (!$isAllowed) {
                $this->message = 'Permission "' . $permission . '" is not allowed. Must be one of: ' . \implode(', ', $this->allowed) . '.';
                return false;
            }

            try {
                $permission = Permission::parse($permission);
            } catch (\Exception $e) {
                $this->message = $e->getMessage();
                return false;
            }

            $column = $permission->getColumn();

            if ($column !== Permission::COLUMN_ALL) {
                $type = $permission->getPermission();

                // Delete removes the whole row, so scoping it to one column is
                // meaningless. Write implies delete, so it inherits the same rule.
                if (\in_array($type, [Database::PERMISSION_DELETE, Database::PERMISSION_WRITE], true)) {
                    $this->message = 'Permission "' . $type . '" cannot be scoped to a column, it applies to the whole row.';
                    return false;
                }

                if (!$this->key->isValid($column)) {
                    $this->message = 'Column "' . $column . '" is not a valid column key.';
                    return false;
                }

                if (!empty($this->columns) && !\in_array($column, $this->columns, true)) {
                    $this->message = 'Column "' . $column . '" does not exist.';
                    return false;
                }
            }

            $role = $permission->getRole();
            $identifier = $permission->getIdentifier();
            $dimension = $permission->getDimension();

            if (!$this->isValidRole($role, $identifier, $dimension)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}

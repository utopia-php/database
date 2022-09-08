<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Permission;
use Utopia\Validator;

class Permissions extends Validator
{
    protected string $message = 'Permissions Error';

    protected array $allowed;

    protected array $userDimensions = [
        'verified',
        'unverified',
    ];

    protected int $length;

    /**
     * Permissions constructor.
     *
     * @param int $length maximum amount of permissions. 0 means unlimited.
     * @param array $allowed allowed permissions. Defaults to all available.
     */
    public function __construct(int $length = 0, array $allowed = [...Database::PERMISSIONS, Database::PERMISSION_WRITE])
    {
        $this->length = $length;
        $this->allowed = $allowed;
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

            $allowed = false;
            foreach ($this->allowed as $allowed) {
                if (\str_starts_with($permission, $allowed)) {
                    $allowed = true;
                    break;
                }
            }
            if (!$allowed) {
                $this->message = 'Permission "' . $permission . '" is not allowed. Must be one of: ' . \implode(', ', $this->allowed) . '.';
                return false;
            }

            try {
                $permission = Permission::parse($permission);
            } catch (\Exception $e) {
                $this->message = $e->getMessage();
                return false;
            }

            $role = $permission->getRole();
            $identifier = $permission->getIdentifier();
            $dimension = $permission->getDimension();
            $key = new Key();

            switch ($role) {
                case Database::ROLE_USERS:
                    if (!empty($identifier)) {
                        $this->message = 'Role "' . $role . '"' . ' can not have an ID value.';
                        return false;
                    }
                    if (!empty($dimension) && !\in_array($dimension, $this->userDimensions)) {
                        $this->message = 'Users dimension "' . $dimension . '" is not allowed. Must be one of: ' . \implode(', ', $this->userDimensions);
                        return false;
                    }
                    break;
                case Database::ROLE_GUESTS:
                case Database::ROLE_ANY:
                    if (!empty($identifier)) {
                        $this->message = 'Role "' . $role . '"' . ' can not have an ID value.';
                        return false;
                    }
                    if (!empty($dimension)) {
                        $this->message = 'Role "' . $role . '"' . ' can not have a dimension value.';
                        return false;
                    }
                    break;
                case Database::ROLE_USER:
                    if (empty($identifier)) {
                        $this->message = 'Role "' . $role . '"' . ' must have an ID value.';
                        return false;
                    }
                    if (!$key->isValid($identifier)) {
                        $this->message = 'Identifier must be a valid key: ' . $key->getDescription();
                        return false;
                    }
                    if (!empty($dimension) && !\in_array($dimension, $this->userDimensions)) {
                        $this->message = 'User dimension "' . $dimension . '" is not allowed. Must be one of: ' . \implode(', ', $this->userDimensions);
                        return false;
                    }
                    break;
                case Database::ROLE_TEAM:
                    if (empty($identifier)) {
                        $this->message = 'Role "' . $role . '"' . ' must have an ID value.';
                        return false;
                    }
                    if (!$key->isValid($identifier)) {
                        $this->message = 'Identifier must be a valid key: ' . $key->getDescription();
                        return false;
                    }
                    if (!empty($dimension) && !$key->isValid($dimension)) {
                        $this->message = 'Dimension must be a valid key: ' . $key->getDescription();
                        return false;
                    }
                    break;
                default:
                    $this->message = 'Role "' . $role . '" is not allowed. Must be one of: ' . \implode(', ', Database::ROLES) . '.';
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

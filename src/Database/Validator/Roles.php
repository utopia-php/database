<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Role;
use Utopia\Validator;

class Roles extends Validator
{
    protected string $message = 'Roles Error';

    protected array $allowed;

    protected int $length;

    /**
     * Roles constructor.
     *
     * @param int $length maximum amount of role. 0 means unlimited.
     * @param array $allowed allowed roles. Defaults to all available.
     */
    public function __construct(int $length = 0, array $allowed = Database::ROLES)
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
     * @param mixed $roles
     *
     * @return bool
     */
    public function isValid($roles): bool
    {
        if (!\is_array($roles)) {
            $this->message = 'Roles must be an array of strings.';
            return false;
        }

        if ($this->length && \count($roles) > $this->length) {
            $this->message = 'You can only provide up to ' . $this->length . ' roles.';
            return false;
        }

        foreach ($roles as $role) {
            if (!\is_string($role)) {
                $this->message = 'Every role must be of type string.';
                return false;
            }
            if ($role === '*') {
                $this->message = 'Wildcard role "*" has been replaced. Use "any" instead.';
                return false;
            }
            if (\str_contains($role, 'role:')) {
                $this->message = 'Roles using the "role:" prefix have been removed. Use "users", "guests", or "any" instead.';
                return false;
            }

            try {
                $role = Role::parse($role);
            } catch (\Exception $e) {
                $this->message = $e->getMessage();
                return false;
            }

            $roleName = $role->getRole();
            $identifier = $role->getIdentifier();
            $dimension = $role->getDimension();

            if (!$this->isValidRole($roleName, $identifier, $dimension)) {
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

    protected function isValidRole(
        string $role,
        string $identifier,
        string $dimension
    ): bool
    {
        $key = new Key();

        switch ($role) {
            case Database::ROLE_USERS:
                if (!empty($identifier)) {
                    $this->message = 'Role "' . $role . '"' . ' can not have an ID value.';
                    return false;
                }
                if (!empty($dimension) && !\in_array($dimension, Database::USER_DIMENSIONS)) {
                    $this->message = 'Users dimension must be one of: ' . \implode(', ', Database::USER_DIMENSIONS);
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
                if (!empty($dimension) && !\in_array($dimension, Database::USER_DIMENSIONS)) {
                    $this->message = 'User dimension must be one of: ' . \implode(', ', Database::USER_DIMENSIONS);
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

        return true;
    }
}

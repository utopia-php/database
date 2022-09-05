<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Role;
use Utopia\Validator;

class Roles extends Validator
{
    protected string $message = 'Roles Error';

    protected array $statusDimensions = [
        'verified',
        'unverified',
    ];

    protected int $length;

    /**
     * Roles constructor.
     *
     * @param int $length maximum amount of role. 0 means unlimited.
     */
    public function __construct(int $length = 0, array $allowed = null)
    {
        $this->length = $length;
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

            $role = Role::parse($role);
            $roleName = $role->getRole();
            $identifier = $role->getIdentifier();
            $dimension = $role->getDimension();

            switch ($role->getRole()) {
                case 'users':
                    if (!empty($dimension) && !\in_array($dimension, $this->statusDimensions)) {
                        $this->message = 'Status dimension must be one of: ' . \implode(', ', $this->statusDimensions);
                        return false;
                    }
                case 'guests':
                case 'any':
                    if (!empty($identifier)) {
                        $this->message = '"' . $roleName . '"' . ' permission can not have an ID value.';
                        return false;
                    }
                    break;
                case 'user':
                    if (!empty($dimension) && !\in_array($dimension, $this->statusDimensions)) {
                        $this->message = 'Status dimension must be one of: ' . \implode(', ', $this->statusDimensions);
                        return false;
                    }
                case 'team':
                    $key = new Key();
                    if (empty($identifier)) {
                        $this->message = '"' . $roleName . '"' . ' permission must have an ID value.';
                        return false;
                    }
                    if (!$key->isValid($identifier)) {
                        $this->message = 'Identifier must be a valid key: ' . $key->getDescription();
                        return false;
                    }
                    break;
                default:
                    $this->message = 'Role "' . $roleName . '" is not allowed.';
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

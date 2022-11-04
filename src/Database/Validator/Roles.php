<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Role;
use Utopia\Database\Exception;
use Utopia\Validator;

class Roles extends Validator
{
    // Roles
    const ROLE_ANY = 'any';
    const ROLE_GUESTS = 'guests';
    const ROLE_USERS = 'users';
    const ROLE_USER = 'user';
    const ROLE_TEAM = 'team';
    const ROLE_MEMBER = 'member';

    const ROLES = [
        self::ROLE_ANY,
        self::ROLE_GUESTS,
        self::ROLE_USERS,
        self::ROLE_USER,
        self::ROLE_TEAM,
        self::ROLE_MEMBER,
    ];

    protected string $message = 'Roles Error';

    protected array $allowed;

    protected int $length;

    const CONFIG = [
        self::ROLE_ANY => [
            'identifier' => [
                'allowed' => false,
                'required' => false,
            ],
            'dimension' =>[
                'allowed' => false,
                'required' => false,
            ],
        ],
        self::ROLE_GUESTS => [
            'identifier' => [
                'allowed' => false,
                'required' => false,
            ],
            'dimension' =>[
                'allowed' => false,
                'required' => false,
            ],
        ],
        self::ROLE_USERS => [
            'identifier' => [
                'allowed' => false,
                'required' => false,
            ],
            'dimension' =>[
                'allowed' => true,
                'required' => false,
                'options' => self::USER_DIMENSIONS
            ],
        ],
        self::ROLE_USER => [
            'identifier' => [
                'allowed' => true,
                'required' => true,
            ],
            'dimension' =>[
                'allowed' => true,
                'required' => false,
                'options' => self::USER_DIMENSIONS
            ],
        ],
        self::ROLE_TEAM => [
            'identifier' => [
                'allowed' => true,
                'required' => true,
            ],
            'dimension' =>[
                'allowed' => true,
                'required' => false,
            ],
        ],
        self::ROLE_MEMBER => [
            'identifier' => [
                'allowed' => true,
                'required' => true,
            ],
            'dimension' =>[
                'allowed' => false,
                'required' => false,
            ],
        ],
    ];

    // Dimensions
    const DIMENSION_VERIFIED = 'verified';
    const DIMENSION_UNVERIFIED = 'unverified';

    const USER_DIMENSIONS = [
        self::DIMENSION_VERIFIED,
        self::DIMENSION_UNVERIFIED,
    ];

    /**
     * Roles constructor.
     *
     * @param int $length maximum amount of role. 0 means unlimited.
     * @param array $allowed allowed roles. Defaults to all available.
     */
    public function __construct(int $length = 0, array $allowed = self::ROLES)
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

            $isAllowed = false;
            foreach ($this->allowed as $allowed) {
                if (\str_starts_with($role, $allowed)) {
                    $isAllowed = true;
                    break;
                }
            }
            if (!$isAllowed) {
                $this->message = 'Role "' . $role . '" is not allowed. Must be one of: ' . \implode(', ', $this->allowed) . '.';
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

        $config = self::CONFIG[$role] ?? null;

        if (empty($config)) {
            $this->message = 'Role "' . $role . '" is not allowed. Must be one of: ' . \implode(', ', self::ROLES) . '.';
            return false;
        }

        if (!isset($config['identifier'])) {
            $this->message = 'Role "' . $role . '" missing identifier configuration.';
            return false;
        }

        if (!isset($config['dimension'])) {
            $this->message = 'Role "' . $role . '" missing dimension configuration.';
            return false;
        }

        // Process identifier configuration
        $allowed = $config['identifier']['allowed'] ?? false;
        $required = $config['identifier']['required'] ?? false;

        // Not allowed and has an identifier
        if (!$allowed && !empty($identifier)) {
            $this->message = 'Role "' . $role . '"' . ' can not have an ID value.';
            return false;
        }

        // Required and has no identifier
        if ($allowed && $required && empty($identifier)) {
            $this->message = 'Role "' . $role . '"' . ' must have an ID value.';
            return false;
        }

        // Allowed and has an invalid identifier
        if ($allowed
            && !empty($identifier)
            && !$key->isValid($identifier)) {
            $this->message = 'Role "' . $role . '"' . ' identifier value is invalid: ' . $key->getDescription();
            return false;
        }

        // Process dimension configuration
        $allowed = $config['dimension']['allowed'] ?? false;
        $required = $config['dimension']['required'] ?? false;
        $options = $config['dimension']['options'] ?? [$dimension];

        // Not allowed and has a dimension
        if (!$allowed && !empty($dimension)) {
            $this->message = 'Role "' . $role . '"' . ' can not have a dimension value.';
            return false;
        }

        // Required and has no dimension
        if ($allowed && $required && empty($dimension)) {
            $this->message = 'Role "' . $role . '"' . ' must have a dimension value.';
            return false;
        }

        if ($allowed && !empty($dimension)) {
            // Allowed and dimension is not an allowed option
            if (!\in_array($dimension, $options)) {
                $this->message = 'Role "' . $role . '"' . ' dimension value is invalid. Must be one of: ' . \implode(', ', $options) . '.';
                return false;
            }
            // Allowed and dimension is not a valid key
            if (!$key->isValid($dimension)) {
                $this->message = 'Role "' . $role . '"' . ' dimension value is invalid: ' . $key->getDescription();
                return false;
            }
        }

        return true;
    }
}

<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Validator;

class Roles extends Validator
{
    protected string $message = 'Roles Error';

    protected array $legacyDimensions = [
        'all',
        'member',
        'guest'
    ];

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
    public function __construct(int $length = 0,)
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
                $this->message = 'Role must be of type string.';
                return false;
            }
            if ($role === '*') {
                $this->message = 'Wildcard role "*" has been replaced. Use "any" instead.';
                return false;
            }
            if (\str_contains($role, 'role:')) {
                $this->message = 'Roles using the "role:" prefix have been deprecated. Use "users", "guests", or "any" instead.';
            }

            $allowedRoles = \implode('|', Database::ROLES);

            $roleMatcher = "/^((?<role>{$allowedRoles})(?::(?<id>[a-zA-Z\d]+[a-zA-Z._\-\d]*))?(?:\/(?<dimension>[a-zA-Z\d]+[a-zA-Z._\-]*))?)$/";

            $matches = [];
            if (!\preg_match($roleMatcher, $role, $matches)) {
                $this->message = 'Must be of the form "role:id/dimension", got "' . $role . '".';
                return false;
            }

            $type = $matches['role'];
            $id = $matches['id'] ?? '';
            $dimension = $matches['dimension'] ?? '';

            switch ($type) {
                case 'any':
                case 'guests':
                case 'users':
                    if (!empty($id)) {
                        $this->message = '"' . $type . '"' . ' role can not have a value.';
                        return false;
                    }
                    if (!empty($dimension) && !\in_array($dimension, $this->statusDimensions)) {
                        $this->message = 'Status dimension must be one of: ' . \implode(', ', $this->statusDimensions);
                        return false;
                    }
                    break;
                /** @noinspection PhpMissingBreakStatementInspection */
                case 'user':
                    if (!empty($dimension) && !\in_array($dimension, $this->statusDimensions)) {
                        $this->message = 'Status dimension must be one of: ' . \implode(', ', $this->statusDimensions);
                        return false;
                    }
                case 'member':
                case 'team':
                    $key = new Key();
                    if (empty($id)) {
                        $this->message = 'ID must not be empty.';
                        return false;
                    }
                    if (!$key->isValid($id)) {
                        $this->message = 'ID must be a valid key: ' . $key->getDescription();
                        return false;
                    }
                    break;
                case 'status':
                    // Dimension is in the ID position for status role e.g. "status:verified"
                    if (!\in_array($id, $this->statusDimensions)) {
                        $this->message = 'Status dimension must be one of: ' . \implode(', ', $this->statusDimensions);
                        return false;
                    }
                    break;
                case 'role':
                    if (empty($id)) {
                        $this->message = 'Role must not be empty.';
                        return false;
                    }
                    if (!\in_array($id, $this->legacyDimensions)) {
                        $this->message = 'Role must be one of: ' . \implode(', ', $this->legacyDimensions);
                        return false;
                    }
                    break;
                default:
                    $this->message = 'Permission must begin with one of: ' . \implode(", ", $roles);
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

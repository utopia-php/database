<?php

namespace Utopia\Database\Helpers;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;

class Permission
{
    private Role $role;

    public function __construct(
        private string $permission,
        string $role,
        string $identifier = '',
        string $dimension = '',
    ) {
        $this->role = new Role($role, $identifier, $dimension);
    }

    /**
     * Create a permission string from this Permission instance
     *
     * @return string
     */
    public function toString(): string
    {
        return $this->permission . '("' . $this->role->toString() . '")';
    }

    /**
     *
     * @return string
     */
    public function getPermission(): string
    {
        return $this->permission;
    }

    /**
     * @return string
     */
    public function getRole(): string
    {
        return $this->role->getRole();
    }

    /**
     * @return string
     */
    public function getIdentifier(): string
    {
        return $this->role->getIdentifier();
    }

    /**
     * @return string
     */
    public function getDimension(): string
    {
        return $this->role->getDimension();
    }

    /**
     * Parse a permission string into a Permission object
     *
     * @param string $permission
     * @return self
     * @throws Exception
     */
    public static function parse(string $permission): self
    {
        $permissionParts = \explode('("', $permission);

        if (\count($permissionParts) !== 2) {
            throw new DatabaseException('Invalid permission string format: "' . $permission . '".');
        }

        $permission = $permissionParts[0];

        if (!\in_array($permission, Database::PERMISSIONS)) {
            throw new DatabaseException('Invalid permission type: "' . $permission . '".');
        }
        $fullRole = \str_replace('")', '', $permissionParts[1]);
        $roleParts = \explode(':', $fullRole);
        $role = $roleParts[0];

        $hasIdentifier = \count($roleParts) > 1;
        $hasDimension = \str_contains($fullRole, '/');

        if (!$hasIdentifier && !$hasDimension) {
            return new self($permission, $role);
        }

        if ($hasIdentifier && !$hasDimension) {
            $identifier = $roleParts[1];
            return new self($permission, $role, $identifier);
        }

        if (!$hasIdentifier) {
            $dimensionParts = \explode('/', $fullRole);
            if (\count($dimensionParts) !== 2) {
                throw new DatabaseException('Only one dimension can be provided');
            }

            $role = $dimensionParts[0];
            $dimension = $dimensionParts[1];

            if (empty($dimension)) {
                throw new DatabaseException('Dimension must not be empty');
            }
            return new self($permission, $role, '', $dimension);
        }

        // Has both identifier and dimension
        $dimensionParts = \explode('/', $roleParts[1]);
        if (\count($dimensionParts) !== 2) {
            throw new DatabaseException('Only one dimension can be provided');
        }

        $identifier = $dimensionParts[0];
        $dimension = $dimensionParts[1];

        if (empty($dimension)) {
            throw new DatabaseException('Dimension must not be empty');
        }

        return new self($permission, $role, $identifier, $dimension);
    }

    /**
     * Create a read permission string from the given Role
     *
     * @param Role $role
     * @return string
     */
    public static function read(Role $role): string
    {
        $permission = new self(
            'read',
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension()
        );
        return $permission->toString();
    }

    /**
     * Create a create permission string from the given Role
     *
     * @param Role $role
     * @return string
     */
    public static function create(Role $role): string
    {
        $permission = new self(
            'create',
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension()
        );
        return $permission->toString();
    }

    /**
     * Create an update permission string from the given Role
     *
     * @param Role $role
     * @return string
     */
    public static function update(Role $role): string
    {
        $permission = new self(
            'update',
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension()
        );
        return $permission->toString();
    }

    /**
     * Create a delete permission string from the given Role
     *
     * @param Role $role
     * @return string
     */
    public static function delete(Role $role): string
    {
        $permission = new self(
            'delete',
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension()
        );
        return $permission->toString();
    }
}

<?php

namespace Utopia\Database;

class Permission
{
    private Role $role;

    public function __construct(
        private string $permission,
        string $role,
        string $identifier = '',
        string $dimension = '',
    )
    {
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
     * @return Permission
     */
    public static function parse(string $permission): Permission
    {
        $parts = \explode('("', $permission);
        $permission = $parts[0];
        $fullRole = \str_replace('")', '', $parts[1]);
        $parts = \explode(':', $fullRole);

        if (\count($parts) === 1) {
            return new Permission($permission, $fullRole);
        }
        $role = $parts[0];
        $fullIdentifier = $parts[1];
        $parts = \explode('/', $fullIdentifier);

        if (\count($parts) === 1) {
            return new Permission($permission, $role, $fullIdentifier);
        }
        $identifier = $parts[0];
        $dimension = $parts[1];

        return new Permission(
            $permission,
            $role,
            $identifier,
            $dimension,
        );
    }

    /**
     * Create a read permission string from the given Role
     *
     * @param Role $role
     * @return string
     */
    public static function read(Role $role): string
    {
        $permission =  new Permission(
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
        $permission =  new Permission(
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
        $permission =  new Permission(
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
        $permission =  new Permission(
            'delete',
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension()
        );
        return $permission->toString();
    }
}


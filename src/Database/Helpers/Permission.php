<?php

namespace Utopia\Database\Helpers;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;

class Permission
{
    private Role $role;

    /**
     * @var array<string, array<string>>
     */
    private static array $aggregates = [
        'write' => [
            Database::PERMISSION_CREATE,
            Database::PERMISSION_UPDATE,
            Database::PERMISSION_DELETE,
        ]
    ];

    public function __construct(
        private string $permission,
        string|Role $role,
        string $identifier = '',
        string $dimension = '',
    ) {
        $this->role = $role instanceof Role
            ? $role
            : new Role($role, $identifier, $dimension);
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
     * @return array<Role>
     */
    public function getRoles(): array
    {
        return $this->role->getRoles();
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
        $separator = \strpos($permission, '("');
        if ($separator === false || !\str_ends_with($permission, '")')) {
            throw new DatabaseException('Invalid permission string format: "' . $permission . '".');
        }

        $role = \substr($permission, $separator + 2, -2);
        $permission = \substr($permission, 0, $separator);

        if (!\in_array($permission, array_merge(Database::PERMISSIONS, [Database::PERMISSION_WRITE]))) {
            throw new DatabaseException('Invalid permission type: "' . $permission . '".');
        }

        return new self($permission, Role::parse($role));
    }

    /**
     * Map aggregate permissions into the set of individual permissions they represent.
     *
     * @param array<string>|null $permissions
     * @param array<string> $allowed
     * @return array<string>|null
     * @throws Exception
     */
    public static function aggregate(?array $permissions, array $allowed = Database::PERMISSIONS): ?array
    {
        if (\is_null($permissions)) {
            return null;
        }
        $mutated = [];
        foreach ($permissions as $i => $permission) {
            $permission = self::parse($permission);
            foreach (self::$aggregates as $type => $subTypes) {
                if ($permission->getPermission() != $type) {
                    $mutated[] = $permission->toString();
                    continue;
                }
                foreach ($subTypes as $subType) {
                    if (!\in_array($subType, $allowed)) {
                        continue;
                    }
                    $mutated[] = (new self(
                        $subType,
                        $permission->role
                    ))->toString();
                }
            }
        }
        return \array_values(\array_unique($mutated));
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
            $role
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
            $role
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
            $role
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
            $role
        );
        return $permission->toString();
    }

    /**
     * Create a write permission string from the given Role
     *
     * @param Role $role
     * @return string
     */
    public static function write(Role $role): string
    {
        $permission = new self(
            'write',
            $role
        );
        return $permission->toString();
    }
}

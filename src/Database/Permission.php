<?php

namespace Utopia\Database;

use Exception;
use Utopia\Database\Exception as DatabaseException;

/**
 * Represents a database permission binding a permission type to a role.
 */
class Permission
{
    /**
     * A permission naming no column covers every column.
     *
     * The empty string rather than null: a permission is parsed from and rendered back to
     * a string, and an absent column has to round-trip through that without becoming the
     * word "null" or a column of that name.
     */
    public const string ALL_COLUMNS = '';

    private Role $role;

    /**
     * @var array<string, array<string>>
     */
    private static array $aggregates = [
        PermissionType::Write->value => [
            PermissionType::Create->value,
            PermissionType::Update->value,
            PermissionType::Delete->value,
        ],
    ];

    /**
     * @param string $permission The permission type (e.g. read, create, update, delete, write)
     * @param string $role The role name
     * @param string $identifier The role identifier
     * @param string $dimension The role dimension
     * @param string $column The column this permission is scoped to, or ALL_COLUMNS for the whole row
     */
    public function __construct(
        private string $permission,
        string $role,
        string $identifier = '',
        string $dimension = '',
        private string $column = self::ALL_COLUMNS,
    ) {
        $this->role = new Role($role, $identifier, $dimension);
    }

    /**
     * Create a permission string from this Permission instance.
     *
     * @return string The formatted permission string (e.g. 'read("user:123")')
     */
    public function toString(): string
    {
        if ($this->column === self::ALL_COLUMNS) {
            return $this->permission.'("'.$this->role->toString().'")';
        }

        return $this->permission.'("'.$this->role->toString().'", "'.$this->column.'")';
    }

    /**
     * The column this permission is scoped to, or ALL_COLUMNS when it covers the whole row.
     */
    public function getColumn(): string
    {
        return $this->column;
    }

    /**
     * Does this permission cover every column rather than naming one?
     */
    public function isForAllColumns(): bool
    {
        return $this->column === self::ALL_COLUMNS;
    }

    /**
     * Get the permission type string.
     *
     * @return string
     */
    public function getPermission(): string
    {
        return $this->permission;
    }

    /**
     * Get the role name associated with this permission.
     *
     * @return string
     */
    public function getRole(): string
    {
        return $this->role->getRole();
    }

    /**
     * Get the role identifier associated with this permission.
     *
     * @return string
     */
    public function getIdentifier(): string
    {
        return $this->role->getIdentifier();
    }

    /**
     * Get the role dimension associated with this permission.
     *
     * @return string
     */
    public function getDimension(): string
    {
        return $this->role->getDimension();
    }

    /**
     * Parse a permission string into a Permission object.
     *
     * @param string $permission The permission string to parse (e.g. 'read("user:123")')
     * @return self
     * @throws DatabaseException If the permission string format or type is invalid
     */
    public static function parse(string $permission): self
    {
        $permissionParts = \explode('("', $permission);

        if (\count($permissionParts) !== 2) {
            throw new DatabaseException('Invalid permission string format: "'.$permission.'".');
        }

        $permission = $permissionParts[0];

        if (! \in_array($permission, array_column(PermissionType::cases(), 'value'))) {
            throw new DatabaseException('Invalid permission type: "'.$permission.'".');
        }
        $fullRole = \str_replace('")', '', $permissionParts[1]);

        // A column-scoped permission is read("role", "column"). Split the column off here so
        // every branch below parses a bare role exactly as it did before -- the role grammar
        // (identifier, dimension) is unchanged by the column, and keeping them separate stops
        // the two from having to know about each other.
        //
        // The role ends at its closing quote, whatever spacing follows the comma. Matching the
        // canonical '", "' alone would read read("any","name") as the role any","name, which is
        // a role that matches nobody -- and these strings come from callers, not only from
        // toString().
        $column = self::ALL_COLUMNS;
        $quote = \strpos($fullRole, '"');

        if ($quote !== false) {
            $rest = \ltrim(\substr($fullRole, $quote + 1));

            if (! \str_starts_with($rest, ',')) {
                throw new DatabaseException('Invalid permission: "'.$permission.'".');
            }

            $rest = \ltrim(\substr($rest, 1));

            if (! \str_starts_with($rest, '"')) {
                throw new DatabaseException('Invalid permission: "'.$permission.'".');
            }

            if ($rest === '"') {
                throw new DatabaseException('Column must not be empty');
            }

            $column = \substr($rest, 1);
            $fullRole = \substr($fullRole, 0, $quote);
        }

        $roleParts = \explode(':', $fullRole);
        $role = $roleParts[0];

        $hasIdentifier = \count($roleParts) > 1;
        $hasDimension = \str_contains($fullRole, '/');

        if (! $hasIdentifier && ! $hasDimension) {
            return new self($permission, $role, column: $column);
        }

        if ($hasIdentifier && ! $hasDimension) {
            $identifier = $roleParts[1];

            return new self($permission, $role, $identifier, column: $column);
        }

        if (! $hasIdentifier) {
            $dimensionParts = \explode('/', $fullRole);
            if (\count($dimensionParts) !== 2) {
                throw new DatabaseException('Only one dimension can be provided');
            }

            $role = $dimensionParts[0];
            $dimension = $dimensionParts[1];

            if (empty($dimension)) {
                throw new DatabaseException('Dimension must not be empty');
            }

            return new self($permission, $role, '', $dimension, $column);
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

        return new self($permission, $role, $identifier, $dimension, $column);
    }

    /**
     * Map aggregate permissions into the set of individual permissions they represent.
     *
     * @param  array<string>|null  $permissions
     * @param  array<string>  $allowed
     * @return array<string>|null
     *
     * @throws Exception
     */
    /**
     * @param  array<string>|null  $permissions
     * @param  array<PermissionType>  $allowed
     * @return array<string>|null
     *
     * @throws Exception
     */
    public static function aggregate(?array $permissions, array $allowed = [PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete]): ?array
    {
        if (\is_null($permissions)) {
            return null;
        }
        $allowedValues = \array_map(fn (PermissionType $p) => $p->value, $allowed);
        $mutated = [];
        foreach ($permissions as $i => $permission) {
            $permission = self::parse($permission);
            foreach (self::$aggregates as $type => $subTypes) {
                if ($permission->getPermission() != $type) {
                    $mutated[] = $permission->toString();

                    continue;
                }
                foreach ($subTypes as $subType) {
                    if (! \in_array($subType, $allowedValues)) {
                        continue;
                    }
                    $mutated[] = (new self(
                        $subType,
                        $permission->getRole(),
                        $permission->getIdentifier(),
                        $permission->getDimension(),
                        $permission->getColumn()
                    ))->toString();
                }
            }
        }

        return \array_values(\array_unique($mutated));
    }

    /**
     * Does any of these permissions name a column?
     *
     * @param  list<string>  $permissions
     *
     * @throws DatabaseException
     */
    public static function hasColumns(array $permissions): bool
    {
        foreach ($permissions as $permission) {
            if (! self::parse($permission)->isForAllColumns()) {
                return true;
            }
        }

        return false;
    }

    /**
     * The same permissions with every column dropped, deduplicated.
     *
     * Whether a caller may reach a row is a question about the role alone: a column narrows what
     * a permission reaches, not who holds it. This is the form a read gate matches against --
     * a side table of roles, a JSONB containment, a Mongo `$in` -- so the column is kept only
     * where the scope is enforced and never where access is decided. A gate handed the column
     * would be comparing `read("any", "name")` against `read("any")` and finding nothing.
     *
     * @param  list<string>  $permissions
     * @return list<string>
     *
     * @throws DatabaseException
     */
    public static function withoutColumns(array $permissions): array
    {
        $stripped = [];

        foreach ($permissions as $permission) {
            $parsed = self::parse($permission);

            $stripped[] = $parsed->isForAllColumns()
                ? $permission
                : (new self(
                    $parsed->getPermission(),
                    $parsed->getRole(),
                    $parsed->getIdentifier(),
                    $parsed->getDimension()
                ))->toString();
        }

        return \array_values(\array_unique($stripped));
    }

    /**
     * Create a read permission string from the given Role.
     *
     * @param Role $role The role to grant read permission to
     * @param string $column Scope the permission to one column, or ALL_COLUMNS for the whole row
     * @return string The formatted permission string
     */
    public static function read(Role $role, string $column = self::ALL_COLUMNS): string
    {
        $permission = new self(
            PermissionType::Read->value,
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension(),
            $column
        );

        return $permission->toString();
    }

    /**
     * Create a create permission string from the given Role.
     *
     * @param Role $role The role to grant create permission to
     * @param string $column Scope the permission to one column, or ALL_COLUMNS for the whole row
     * @return string The formatted permission string
     */
    public static function create(Role $role, string $column = self::ALL_COLUMNS): string
    {
        $permission = new self(
            PermissionType::Create->value,
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension(),
            $column
        );

        return $permission->toString();
    }

    /**
     * Create an update permission string from the given Role.
     *
     * @param Role $role The role to grant update permission to
     * @param string $column Scope the permission to one column, or ALL_COLUMNS for the whole row
     * @return string The formatted permission string
     */
    public static function update(Role $role, string $column = self::ALL_COLUMNS): string
    {
        $permission = new self(
            PermissionType::Update->value,
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension(),
            $column
        );

        return $permission->toString();
    }

    /**
     * Create a delete permission string from the given Role.
     *
     * @param Role $role The role to grant delete permission to
     * @param string $column Scope the permission to one column, or ALL_COLUMNS for the whole row
     * @return string The formatted permission string
     */
    public static function delete(Role $role, string $column = self::ALL_COLUMNS): string
    {
        $permission = new self(
            PermissionType::Delete->value,
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension(),
            $column
        );

        return $permission->toString();
    }

    /**
     * Create a write permission string from the given Role.
     *
     * @param Role $role The role to grant write permission to
     * @param string $column Scope the permission to one column, or ALL_COLUMNS for the whole row
     * @return string The formatted permission string
     */
    public static function write(Role $role, string $column = self::ALL_COLUMNS): string
    {
        $permission = new self(
            PermissionType::Write->value,
            $role->getRole(),
            $role->getIdentifier(),
            $role->getDimension(),
            $column
        );

        return $permission->toString();
    }
}

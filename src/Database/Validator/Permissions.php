<?php

namespace Utopia\Database\Validator;

use Exception;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;

class Permissions extends Roles
{
    #[\Override]
    protected string $message = 'Permissions Error';

    /**
     * @var array<string>
     */
    #[\Override]
    protected array $allowed;

    #[\Override]
    protected int $length;

    /**
     * Columns a permission in this set is allowed to name.
     *
     * @var array<string>|null
     */
    private ?array $columns;

    /**
     * @param  int  $length  maximum amount of permissions. 0 means unlimited.
     * @param  array<PermissionType>  $allowed  allowed permissions. Defaults to all available.
     * @param  array<string>|null  $columns  the collection's column keys. A permission may name one
     *                                       of these and no other. Null means this set may not name
     *                                       a column at all, which is the case for every permission
     *                                       outside a collection's own -- a document's permissions
     *                                       carry no column, and nothing else has a column list to
     *                                       judge one against.
     */
    public function __construct(int $length = 0, array $allowed = [PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete, PermissionType::Write], ?array $columns = null)
    {
        $this->length = $length;
        $this->allowed = \array_map(fn (PermissionType $p) => $p->value, $allowed);
        $this->columns = $columns;
    }

    #[\Override]
    public function getDescription(): string
    {
        return $this->message;
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        if (! \is_array($value)) {
            $this->message = 'Permissions must be an array of strings.';

            return false;
        }

        if ($this->length && \count($value) > $this->length) {
            $this->message = 'You can only provide up to '.$this->length.' permissions.';

            return false;
        }

        foreach ($value as $permission) {
            if (! \is_string($permission)) {
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
            if (! $isAllowed) {
                $this->message = 'Permission "'.$permission.'" is not allowed. Must be one of: '.\implode(', ', $this->allowed).'.';

                return false;
            }

            try {
                $permission = Permission::parse($permission);
            } catch (Exception $e) {
                $this->message = $e->getMessage();

                return false;
            }

            $role = $permission->getRole();
            $identifier = $permission->getIdentifier();
            $dimension = $permission->getDimension();

            if (! $this->isValidRole($role, $identifier, $dimension)) {
                return false;
            }

            if (! $permission->isForAllColumns() && ! $this->isValidColumn($permission)) {
                return false;
            }
        }

        return true;
    }

    /**
     * A permission may name a column only where columns are meaningful, and only one that exists.
     *
     * Reported by the key the caller wrote. Nothing has translated it yet at this point, and an
     * error naming something the caller did not type is worse than no error at all.
     */
    private function isValidColumn(Permission $permission): bool
    {
        if ($this->columns === null) {
            $this->message = 'Permission "'.$permission->toString().'" is scoped to a column. Column-scoped permissions are set on the collection, not here.';

            return false;
        }

        if (! \in_array($permission->getColumn(), $this->columns, true)) {
            $this->message = 'Column "'.$permission->getColumn().'" does not exist.';

            return false;
        }

        return true;
    }

    #[\Override]
    public function isArray(): bool
    {
        return false;
    }

    #[\Override]
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}

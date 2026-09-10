<?php

namespace Utopia\Database\Validator;

use Exception;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\PermissionType;

/**
 * Validates permission strings ensuring they use valid permission types and role formats.
 */
class Permissions extends Roles
{
    protected string $message = 'Permissions Error';

    /**
     * @var array<string>
     */
    protected array $allowed;

    protected int $length;

    /**
     * Permissions constructor.
     *
     * @param  int  $length  maximum amount of permissions. 0 means unlimited.
     * @param  array<PermissionType>  $allowed  allowed permissions. Defaults to all available.
     */
    public function __construct(int $length = 0, array $allowed = [PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete, PermissionType::Write])
    {
        $this->length = $length;
        $this->allowed = \array_map(fn (PermissionType $p) => $p->value, $allowed);
    }

    /**
     * Get Description.
     *
     * Returns validator description
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
     * @param  mixed  $permissions
     */
    public function isValid($permissions): bool
    {
        if (! \is_array($permissions)) {
            $this->message = 'Permissions must be an array of strings.';

            return false;
        }

        if ($this->length && \count($permissions) > $this->length) {
            $this->message = 'You can only provide up to '.$this->length.' permissions.';

            return false;
        }

        foreach ($permissions as $permission) {
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
        }

        return true;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}

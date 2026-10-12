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
     * @param  int  $length  maximum amount of permissions. 0 means unlimited.
     * @param  array<PermissionType>  $allowed  allowed permissions. Defaults to all available.
     */
    public function __construct(int $length = 0, array $allowed = [PermissionType::Create, PermissionType::Read, PermissionType::Update, PermissionType::Delete, PermissionType::Write])
    {
        $this->length = $length;
        $this->allowed = \array_map(fn (PermissionType $p) => $p->value, $allowed);
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

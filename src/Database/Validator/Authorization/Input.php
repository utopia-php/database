<?php

namespace Utopia\Database\Validator\Authorization;

use Utopia\Database\PermissionType;

/**
 * Encapsulates the action and permissions used as input for authorization validation.
 */
class Input
{
    /**
     * @var array<string>
     */
    protected array $permissions;

    protected string $action;

    /**
     * @param  PermissionType|string  $action  A built-in permission type, or a consumer-defined action such as 'execute'
     * @param  array<string>  $permissions  The permissions that grant the action
     */
    public function __construct(PermissionType|string $action, array $permissions)
    {
        $this->permissions = $permissions;
        $this->action = $action instanceof PermissionType ? $action->value : $action;
    }

    /**
     * @param  array<string>  $permissions
     */
    public function setPermissions(array $permissions): static
    {
        $this->permissions = $permissions;

        return $this;
    }

    public function setAction(PermissionType|string $action): static
    {
        $this->action = $action instanceof PermissionType ? $action->value : $action;

        return $this;
    }

    /**
     * @return string[]
     */
    public function getPermissions(): array
    {
        return $this->permissions;
    }

    public function getAction(): string
    {
        return $this->action;
    }
}

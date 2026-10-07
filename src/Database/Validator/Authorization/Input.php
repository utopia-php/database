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
     * @param  array<string>  $permissions  The permissions that grant the action
     */
    public function __construct(PermissionType $action, array $permissions)
    {
        $this->permissions = $permissions;
        $this->action = $action->value;
    }

    /**
     * @param  array<string>  $permissions
     */
    public function setPermissions(array $permissions): static
    {
        $this->permissions = $permissions;

        return $this;
    }

    public function setAction(PermissionType $action): static
    {
        $this->action = $action->value;

        return $this;
    }

    /**
     * Get the permissions to check against.
     *
     * @return string[]
     */
    public function getPermissions(): array
    {
        return $this->permissions;
    }

    /**
     * Get the action being authorized.
     *
     * @return string
     */
    public function getAction(): string
    {
        return $this->action;
    }
}

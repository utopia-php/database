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
     * Create a new authorization input.
     *
     * @param PermissionType|string $action The action being authorized (e.g., read, write)
     * @param string[] $permissions List of permission strings to check against
     */
    public function __construct(PermissionType|string $action, array $permissions)
    {
        $this->permissions = $permissions;
        $this->action = $action instanceof PermissionType ? $action->value : $action;
    }

    /**
     * Set the permissions to check against.
     *
     * @param string[] $permissions List of permission strings
     * @return self
     */
    public function setPermissions(array $permissions): self
    {
        $this->permissions = $permissions;

        return $this;
    }

    /**
     * Set the action being authorized.
     *
     * @param PermissionType|string $action The action name
     * @return self
     */
    public function setAction(PermissionType|string $action): self
    {
        $this->action = $action instanceof PermissionType ? $action->value : $action;

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

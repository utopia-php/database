<?php

namespace Utopia\Database\Validator\Authorization;

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
     * @param string $action The action being authorized (e.g., read, write)
     * @param string[] $permissions List of permission strings to check against
     */
    public function __construct(string $action, array $permissions)
    {
        $this->permissions = $permissions;
        $this->action = $action;
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
     * @param string $action The action name
     * @return self
     */
    public function setAction(string $action): self
    {
        $this->action = $action;

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

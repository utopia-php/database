<?php

namespace Utopia\Database\Validator;

class Authorization
{
    /**
     * @var array<string, bool>
     */
    protected array $roles = [
        'any' => true
    ];

    /**
     * @param string $role
     * @return void
     */
    public function setRole(string $role): void
    {
        $this->roles[$role] = true;
    }

    /**
     * @param string $role
     *
     * @return void
     */
    public function unsetRole(string $role): void
    {
        unset($this->roles[$role]);
    }

    /**
     * @return array<string>
     */
    public function getRoles(): array
    {
        return \array_keys($this->roles);
    }

    /**
     * @return void
     */
    public function cleanRoles(): void
    {
        $this->roles = [];
    }

    /**
     * @param string $role
     *
     * @return bool
     */
    public function isRole(string $role): bool
    {
        return (\array_key_exists($role, $this->roles));
    }

    /**
     * @var bool
     */
    public bool $status = true;

    /**
     * Default value in case we need
     *  to reset Authorization status
     *
     * @var bool
     */
    public bool $statusDefault = true;

    /**
     * Change default status.
     * This will be used for the
     *  value set on the $this->reset() method
     *
     * @param bool $status
     * @return void
     */
    public function setDefaultStatus(bool $status): void
    {
        $this->statusDefault = $status;
        $this->status = $status;
    }

    /**
     * Skip Authorization
     *
     * Skips authorization for the code to be executed inside the callback
     *
     * @template T
     * @param callable(): T $callback
     * @return T
     */
    public function skip(callable $callback): mixed
    {
        $initialStatus = $this->status;
        $this->disable();

        try {
            return $callback();
        } finally {
            $this->status = $initialStatus;
        }
    }

    /**
     * Enable Authorization checks
     *
     * @return void
     */
    public function enable(): void
    {
        $this->status = true;
    }

    /**
     * Disable Authorization checks
     *
     * @return void
     */
    public function disable(): void
    {
        $this->status = false;
    }

    /**
     * Disable Authorization checks
     *
     * @return void
     */
    public function reset(): void
    {
        $this->status = $this->statusDefault;
    }
}

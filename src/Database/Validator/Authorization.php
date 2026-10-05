<?php

namespace Utopia\Database\Validator;

use Utopia\Database\State\Group;
use Utopia\Database\State\Value;
use Utopia\Database\Validator\Authorization\Input;
use Utopia\Validator;

/**
 * Validates authorization by checking if any of the current roles match the required permissions.
 *
 * The status and the roles are shared by every caller, except inside skip(), withStatus() and withRoles(): those
 * scopes belong to the calling coroutine and the coroutines it starts (see {@see Value}). The status and the roles
 * share one {@see Group}, so any of those scopes keeps the status and role changes of a coroutine whose starter has
 * returned local to it.
 */
class Authorization extends Validator
{
    /**
     * @var Value<bool>
     */
    private Value $status;

    /**
     * Default value in case we need
     *  to reset Authorization status
     */
    protected bool $statusDefault = true;

    /**
     * @var Value<array<string, bool>>
     */
    private Value $roles;

    protected string $message = 'Authorization Error';

    public function __construct()
    {
        $group = new Group();
        $this->status = new Value(true, $group);

        /** @var Value<array<string, bool>> $roles */
        $roles = new Value(['any' => true], $group);
        $this->roles = $roles;
    }

    public function __clone()
    {
        $group = new Group();
        $this->status = new Value($this->status->get(), $group);
        $this->roles = new Value($this->roles->get(), $group);
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
     * Validate that the given input has the required permissions for the current roles.
     *
     * @param mixed $input Authorization\Input instance containing action and permissions
     * @return bool
     */
    public function isValid(mixed $input): bool
    {
        if (! ($input instanceof Input)) {
            $this->message = 'Invalid input provided';

            return false;
        }

        $permissions = $input->getPermissions();
        $action = $input->getAction();

        if (! $this->status->get()) {
            return true;
        }

        if (empty($permissions)) {
            $this->message = 'No permissions provided for action \''.$action.'\'';

            return false;
        }

        $permission = '-';
        $roles = $this->roles->get();

        foreach ($permissions as $permission) {
            if (\array_key_exists($permission, $roles)) {
                return true;
            }
        }

        $this->message = 'Missing "'.$action.'" permission for role "'.$permission.'". Only "'.\json_encode($this->getRoles()).'" scopes are allowed and "'.\json_encode($permissions).'" was given.';

        return false;
    }

    /**
     * Add a role to the authorized roles list.
     *
     * @param string $role Role identifier to add
     * @return void
     */
    public function addRole(string $role): void
    {
        $roles = $this->roles->get();
        $roles[$role] = true;
        $this->roles->set($roles);
    }

    /**
     * Remove a role from the authorized roles list.
     *
     * @param string $role Role identifier to remove
     * @return void
     */
    public function removeRole(string $role): void
    {
        $roles = $this->roles->get();
        unset($roles[$role]);
        $this->roles->set($roles);
    }

    /**
     * @return array<string>
     */
    public function getRoles(): array
    {
        return \array_keys($this->roles->get());
    }

    /**
     * Run the callback with exactly these roles for the calling coroutine and the coroutines it starts. Roles
     * added or removed inside the callback change that scope only.
     *
     * @template T
     *
     * @param  array<string>  $roles
     * @param  callable(): T  $callback
     * @return T
     */
    public function withRoles(array $roles, callable $callback): mixed
    {
        return $this->roles->with(\array_fill_keys($roles, true), $callback);
    }

    /**
     * Remove all roles from the authorized roles list.
     *
     * @return void
     */
    public function cleanRoles(): void
    {
        $this->roles->set([]);
    }

    /**
     * Check whether a specific role exists in the authorized roles list.
     *
     * @param string $role Role identifier to check
     * @return bool
     */
    public function hasRole(string $role): bool
    {
        return \array_key_exists($role, $this->roles->get());
    }

    /**
     * Change default status.
     * This will be used for the
     *  value set on the $this->reset() method
     */
    public function setDefaultStatus(bool $status): void
    {
        $this->statusDefault = $status;
        $this->status->set($status);
    }

    /**
     * Change status
     */
    public function setStatus(bool $status): void
    {
        $this->status->set($status);
    }

    /**
     * Get status
     */
    public function getStatus(): bool
    {
        return $this->status->get();
    }

    /**
     * Skip Authorization
     *
     * Skips authorization for the code to be executed inside the callback
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function skip(callable $callback): mixed
    {
        return $this->status->with(false, $callback);
    }

    /**
     * Run the callback with the given status for the calling coroutine and the coroutines it starts
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function withStatus(bool $status, callable $callback): mixed
    {
        return $this->status->with($status, $callback);
    }

    /**
     * Enable Authorization checks
     */
    public function enable(): void
    {
        $this->status->set(true);
    }

    /**
     * Disable Authorization checks
     */
    public function disable(): void
    {
        $this->status->set(false);
    }

    /**
     * Reset the status to the default status
     */
    public function reset(): void
    {
        $this->status->set($this->statusDefault);
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

<?php

namespace Utopia\Database\Validator;

use Utopia\Database\State\Group;
use Utopia\Database\State\Value;
use Utopia\Database\Validator\Authorization\Input;
use Utopia\Validator;

/**
 * Validates authorization by checking if any of the current roles match the required permissions.
 *
 * The status and the roles are shared by every caller, except inside skip() and withRoles(): those
 * scopes belong to the calling coroutine and the coroutines it starts (see {@see Value}). The status and the roles
 * share one {@see Group}, so any of those scopes keeps the status and role changes of a coroutine cut off from it,
 * because a coroutine between them has returned, local to that coroutine.
 */
class Authorization extends Validator
{
    /**
     * @var Value<bool>
     */
    private Value $status;

    /**
     * @var Value<array<string, bool>>
     */
    private Value $roles;

    protected string $message = 'Authorization Error';

    /**
     * @param  bool  $defaultStatus  The status the handle starts with, which reset() restores
     */
    public function __construct(protected readonly bool $defaultStatus = true)
    {
        $group = new Group();
        $this->status = new Value($defaultStatus, $group);

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

    #[\Override]
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Validate that the given Authorization\Input has the required permissions for the current roles.
     */
    #[\Override]
    public function isValid(mixed $value): bool
    {
        if (! ($value instanceof Input)) {
            $this->message = 'Invalid input provided';

            return false;
        }

        $permissions = $value->getPermissions();
        $action = $value->getAction();

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

    public function addRole(string $role): static
    {
        $roles = $this->roles->get();
        $roles[$role] = true;
        $this->roles->set($roles);

        return $this;
    }

    public function removeRole(string $role): static
    {
        $roles = $this->roles->get();
        unset($roles[$role]);
        $this->roles->set($roles);

        return $this;
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

    public function cleanRoles(): static
    {
        $this->roles->set([]);

        return $this;
    }

    public function hasRole(string $role): bool
    {
        return \array_key_exists($role, $this->roles->get());
    }

    public function setStatus(bool $status): static
    {
        $this->status->set($status);

        return $this;
    }

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
     * Run the callback with exactly this status and these roles for the calling coroutine and the coroutines it
     * starts, as Database::withSnapshot() restores them.
     *
     * @internal
     *
     * @template T
     *
     * @param  array<string>  $roles
     * @param  callable(): T  $callback
     * @return T
     */
    public function restore(bool $status, array $roles, callable $callback): mixed
    {
        return $this->status->with($status, fn (): mixed => $this->withRoles($roles, $callback));
    }

    public function enable(): static
    {
        $this->status->set(true);

        return $this;
    }

    public function disable(): static
    {
        $this->status->set(false);

        return $this;
    }

    /**
     * Restore the status the handle was constructed with.
     */
    public function reset(): void
    {
        $this->status->set($this->defaultStatus);
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

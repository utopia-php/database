<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Helpers\Role;
use Utopia\Validator;

class Authorization extends Validator
{
    /**
     * @var array<string, bool>
     */
    private static array $roles = [
        'any' => true
    ];

    /**
     * @var string
     */
    protected string $action = '';

    /**
     * @var string
     */
    protected string $message = 'Authorization Error';

    /**
     * @var string|null
     */
    protected static ?string $user = null;

    /**
     * @param string $action
     */
    public function __construct(string $action)
    {
        $this->action = $action;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
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
     * @param mixed $permissions
     *
     * @return bool
     */
    public function isValid($permissions): bool
    {
        if (!self::$status) {
            return true;
        }

        if (empty($permissions)) {
            $this->message = 'No permissions provided for action \''.$this->action.'\'';
            return false;
        }

        $permission = '-';

        foreach ($permissions as $permission) {
            if (\array_key_exists($permission, self::$roles)) {
                return true;
            }
        }

        $this->message = 'Missing "'.$this->action.'" permission for role "'.$permission.'". Only "'.\json_encode(self::getRoles()).'" scopes are allowed and "'.\json_encode($permissions).'" was given.';

        return false;
    }

    /**
     * @param string $role
     * @return void
     */
    public static function setRole(string $role): void
    {
        $parsedRole = Role::parse($role);
        if ($parsedRole->getRole() === 'user') {
            $userIdetifier = $parsedRole->getIdentifier();
            self::$user = $userIdetifier;
        }
        self::$roles[$role] = true;
    }

    /**
     * @param string $role
     *
     * @return void
     */
    public static function unsetRole(string $role): void
    {
        $parsedRole = Role::parse($role);
        if ($parsedRole->getRole() === 'user' && self::$user === $parsedRole->getIdentifier()) {
            self::$user = null;
        }
        unset(self::$roles[$role]);
    }

    /**
     * Get current user
     *
     * @return string|null
     */
    public static function getUser(): string|null
    {
        return self::$user;
    }

    /**
     * @return array<string>
     */
    public static function getRoles(): array
    {
        return \array_keys(self::$roles);
    }

    /**
     * @return void
     */
    public static function cleanRoles(): void
    {
        self::$roles = [];
    }

    /**
     * @param string $role
     *
     * @return bool
     */
    public static function isRole(string $role): bool
    {
        return (\array_key_exists($role, self::$roles));
    }

    /**
     * @var bool
     */
    public static bool $status = true;

    /**
     * Default value in case we need
     *  to reset Authorization status
     *
     * @var bool
     */
    public static bool $statusDefault = true;

    /**
     * Change default status.
     * This will be used for the
     *  value set on the self::reset() method
     *
     * @param bool $status
     * @return void
     */
    public static function setDefaultStatus(bool $status): void
    {
        self::$statusDefault = $status;
        self::$status = $status;
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
    public static function skip(callable $callback): mixed
    {
        $initialStatus = self::$status;
        self::disable();

        try {
            return $callback();
        } finally {
            self::$status = $initialStatus;
        }
    }

    /**
     * Enable Authorization checks
     *
     * @return void
     */
    public static function enable(): void
    {
        self::$status = true;
    }

    /**
     * Disable Authorization checks
     *
     * @return void
     */
    public static function disable(): void
    {
        self::$status = false;
    }

    /**
     * Disable Authorization checks
     *
     * @return void
     */
    public static function reset(): void
    {
        self::$status = self::$statusDefault;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}

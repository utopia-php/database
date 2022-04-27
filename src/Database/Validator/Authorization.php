<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Document;
use Utopia\Validator;

class Authorization extends Validator
{
    /**
     * Array of allowed roles.
     * 
     * @var array
     */
    static array $roles = ['role:all' => true];

    /**
     * Action of the authorization validation.
     * Mainly 'read' or 'write'.
     * 
     * @var string
     */
    protected string $action = '';

    /**
     * Error message explaining why validation failed.
     * 
     * @var string
     */
    protected string $message = 'Authorization Error';

    /**
     * @param string $action Action to check for during validation.
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
    public function isValid(mixed $permissions): bool
    {
        if (!self::$status) {
            return true;
        }

        if(empty($permissions)) {
            $this->message = 'No permissions provided for action \''.$this->action.'\'';
            return false;
        }

        $permission = '-';

        foreach ($permissions as $permission) {
            if (\array_key_exists($permission, self::$roles)) {
                return true;
            }
        }

        $this->message = 'Missing "'.$this->action.'" permission. Scopes "'.\json_encode(self::getRoles()).'" are given, but these scopes are allowed: "'.\json_encode($permissions).'"';

        return false;
    }

    /**
     * Add role to list of allowed roles for validation.
     * 
     * @param string $role Role to add
     * 
     * @return void
     */
    public static function setRole(string $role): void
    {
        self::$roles[$role] = true;
    }

    /**
     * Remove role from list of allowed roles for validation.
     * 
     * @param string $role Role to remove
     *
     * @return void
     */
    public static function unsetRole(string $role): void
    {
        unset(self::$roles[$role]);
    }

    /**
     * Get list of allowed roles.
     * 
     * @return array
     */
    public static function getRoles(): array
    {
        return \array_keys(self::$roles);
    }

    /**
     * Clear list of allowed roles.
     * 
     * @return void
     */
    public static function cleanRoles(): void
    {
        self::$roles = [];
    }

    /**
     * Check if role is already allowed.
     * 
     * @param string $role Role name to check for
     * 
     * @return bool Returns true if role is already in the list
     */
    public static function isRole(string $role): bool
    {
        return (\array_key_exists($role, self::$roles));
    }

    /**
     * Current Authorization status.
     * 
     * true = authorization checks are active
     * false = authorization checks will not run
     * 
     * @var bool
     */
    public static bool $status = true;
    
    /**
     * Default value in case we need
     *  to reset Authorization status.
     *
     * @var bool
     */
    public static bool $statusDefault = true;

    /**
     * Change default status.
     * 
     * This will be used for the
     *  value set on the self::reset() method.
     * 
     * @param bool $status
     * 
     * @return void
     */
    public static function setDefaultStatus(bool $status): void
    {
        self::$statusDefault = $status;
        self::$status = $status;
    }

    /**
     * Skip Authorization for the code executed inside the callback.
     * 
     * @param callable $callback function to run without authorization
     * 
     * @return mixed
     */
    public static function skip(callable $callback): mixed
    {
        $enabled = self::$status;

        if ($enabled) self::disable();
        $result = $callback();
        if ($enabled) self::reset();

        return $result;
    }

    /**
     * Enable Authorization checks.
     * 
     * @return void
     */
    public static function enable(): void
    {
        self::$status = true;
    }

    /**
     * Disable Authorization checks.
     * 
     * @return void
     */
    public static function disable(): void
    {
        self::$status = false;
    }

    /**
     * Reset Authorization checks.
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
     * Get validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}

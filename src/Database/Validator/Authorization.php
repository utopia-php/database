<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Document;
use Utopia\Validator;

class Authorization extends Validator
{
    /**
     * @var array
     */
    static $roles = ['role:all' => true];

    /**
     * @var string
     */
    protected $action = '';

    /**
     * @var string
     */
    protected $message = 'Authorization Error';

    /**
     * @param string $action
     */
    public function __construct($action)
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

        $this->message = 'Missing "'.$this->action.'" permission for role "'.$permission.'". Only this scopes "'.\json_encode(self::getRoles()).'" are given and only this are allowed "'.\json_encode($permissions).'".';

        return false;
    }

    /**
     * @param string $role
     * @return void
     */
    public static function setRole(string $role): void
    {
        self::$roles[$role] = true;
    }

    /**
     * @param string $role
     *
     * @return void
     */
    public static function unsetRole(string $role): void
    {
        unset(self::$roles[$role]);
    }

    /**
     * @return array
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
    public static $status = true;
    
    /**
     * Default value in case we need
     *  to reset Authorization status
     *
     * @var bool
     */
    public static $statusDefault = true;

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
     * @return mixed
     */
    public static function skip(callable $callback)
    {
        self::disable();
        $result = $callback();
        self::reset();

        return $result;
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

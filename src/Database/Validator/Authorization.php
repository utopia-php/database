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
     * @var Document
     */
    protected $collection;

    /**
     * @var string
     */
    protected $action = '';

    /**
     * @var string
     */
    protected $message = 'Authorization Error';

    /**
     * @return array
     */
    protected function getCollectionPermissions(): array
    {
        switch ($this->action){
            case 'read':
                return $this->collection->getRead();
                break;
            case 'write':
                return $this->collection->getWrite();
                break;
            default:
                return [];
                break;
        }
    }

    /**
     * @param Document $document
     * @param string   $action
     */
    public function __construct(Document $collection, $action)
    {
        $this->collection = $collection;
        $this->action = $action;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription()
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
    public function isValid($permissions)
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
            if (!empty($this->getCollectionPermissions)) {
                if (\array_key_exists($permission, $this->getCollectionPermissions())) {
                    return true;
                } else {
                    $this->message = 'Missing "'.$this->action.'" permission for role "'.$permission.'". Only this scopes "'.\json_encode(self::getRoles()).'" are given and only this are allowed "'.\json_encode($this->getCollectionPermissions()).'".';
                    return false;
                }
            }

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

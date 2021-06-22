<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;
use Utopia\Database\Validator\Key;

class Permissions extends Validator
{
    /**
     * @var string
     */
    protected $message = 'Permissions Error';

    /**
     * @var string[]
     */
    protected $permissions = [
        'member',
        'role',
        'team',
        'user',
    ];

    /**
     * @var string[]
     */
    protected $roles = [
        'all',
        'guest',
        'member',
    ];

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
     * @param mixed $roles
     *
     * @return bool
     */
    public function isValid($roles)
    {
        if(!is_array($roles)) {
            $this->message = 'Permissions roles must be an array of strings.';
            return false;
        }

        foreach ($roles as $role) {
            if (!\is_string($role)) {
                $this->message = 'Permissions role must be of type string.';

                return false;
            }

            if ($role === '*') {
                $this->message = 'Wildcard permission "*" deprecated. Use "role:all" instead.';

                return false;
            }

            // Should only contain a single ":" char
            $pos = \strpos($role, ':');

            if ($pos === false || $pos !== \strrpos($role, ':')) {
                $this->message = 'Permission roles must contain one and only one ":" character.';

                return false;
            }

            /**
             * Split role into format {$type}:{$value}
             *
             * Substring before ":" $type must be a known permission type
             * Substring after ":" $value must not be empty and satisty requirements per $type
             */

            $type = \substr($role, 0, \strpos($role, ':'));
            $value = \substr($role, \strpos($role, ':') + 1);

            if (strlen($value) === 0) {
                $this->message = 'Permission role value must not be empty';

                return false;
            }

            switch ($type) {
                case 'role':
                    // role:$value must be in list of $roles
                    if (!\in_array($value, $this->roles)) {
                        $this->message = 'Permission roles must be one of: ' . \implode(", ", $this->roles);
                        return false;
                    }
                    break;
                case 'team':
                case 'user':
                case 'member':
                    // every valid $value must be a valid Key
                    $key = new Key();
                    if (!$key->isValid($value)) {
                        $this->message = $key->getDescription();

                        return false;
                    }
                    break;

                default:
                    $this->message = 'Permission role must begin with one of: ' . \implode(", ", $this->permissions);
                    return false; break;
            }
        }

        return true;
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

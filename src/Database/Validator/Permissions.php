<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

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

            // Substring before ":" must be a known permission type
            $type = \substr($role, 0, \strpos($role, ':'));
            $value = \substr($role, \strpos($role, ':') + 1);

            if (!\in_array($type, $this->permissions)) {
                $this->message = 'Permission role must begin with one of: ' . \implode(", ", $this->permissions);

                return false;
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

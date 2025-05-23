<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class AuthorizationValidator extends Validator
{

    protected string $message = 'Authorization Error';

    public function __construct(
        private readonly Authorization $authorization,
        private readonly string $action,
    )
    {
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
        if (!$this->authorization->status) {
            return true;
        }

        if (empty($permissions)) {
            $this->message = 'No permissions provided for action \''.$this->action.'\'';
            return false;
        }

        $permission = '-';

        foreach ($permissions as $permission) {
            if (\in_array($permission, $this->authorization->getRoles())) {
                return true;
            }
        }

        $this->message = 'Missing "'.$this->action.'" permission for role "'.$permission.'". Only "'.\json_encode($this->authorization->getRoles()).'" scopes are allowed and "'.\json_encode($permissions).'" was given.';

        return false;
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
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
    public function getDescription(): string
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
    public function isValid($roles): bool
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

            $type = \substr($role, 0, $pos);
            $value = \substr($role, $pos + 1);

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
                case 'user':
                case 'member':
                    // user:$id and member:$id must be valid Keys
                    $key = new Key();
                    if (!$key->isValid($value)) {
                        $this->message = '[role:$id] $id must be a valid key: ' . $key->getDescription();

                        return false;
                    }
                    break;
                case 'team':
                    // team:$teamId or team:$teamId/$teamRole
                    $key = new Key();

                    // must have at most a single "/" char
                    $pos = \strpos($value, '/');

                    // if no team role is given and and $id is not valid
                    if ($pos === false && !$key->isValid($value)) {
                        $this->message = '[role:$id] $id must be a valid key: ' . $key->getDescription();

                        return false;
                    }

                    // if "/" is at index zero
                    if ($pos === 0) {
                        $this->message = 'Team ID must not be empty.';

                        return false;
                    }

                    // if a "/" is found, ensure is unique and both substrings are valid
                    if ($pos > 0) {
                        // Split into format {$teamId}/{$teamRole}
                        $teamId = \substr($value, 0, $pos);
                        $teamRole = \substr($value, $pos + 1);

                        // $teamRole must not be empty
                        // Case $teamId < 1 already covered.
                        if (strlen($teamRole) < 1) {
                            $this->message = 'Team role must not be empty.';

                            return false;
                        } 

                        // Ensure "/" is unique
                        if ($pos !== \strrpos($value, '/')) {
                            $this->message = 'Permission roles may contain at most one "/" character.';

                            return false;
                        }

                        // $teamId and $teamRole must both be valid Keys
                        if (!$key->isValid($teamId) || !$key->isValid($teamRole)) {
                            $this->message = '[team:$teamId/$role] $teamID and $role must be valid keys: ' . $key->getDescription();

                            return false;
                        }
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

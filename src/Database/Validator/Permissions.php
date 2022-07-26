<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class Permissions extends Validator
{
    protected string $message = 'Permissions Error';

    protected array $methods = [
        'create',
        'read',
        'update',
        'delete',
        'write',

        'admin',
    ];

    protected array $permissions = [
        'users',
        'user',
        'team',
        'member',
        'guests',
        'any',

        'status',
    ];

    protected array $dimensions = [
        'verified',
        'unverified',
    ];

    protected int $length;

    /**
     * Permissions constructor.
     *
     * @param int $length maximum amount of permissions. 0 means unlimited.
     */
    public function __construct(int $length = 0)
    {
        $this->length = $length;
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
        if (!\is_array($permissions)) {
            $this->message = 'Permissions must be an array of strings.';
            return false;
        }
        if ($this->length && \count($permissions) > $this->length) {
            $this->message = 'You can only provide up to ' . $this->length . ' permissions.';
            return false;
        }

        foreach ($permissions as $permission) {
            if (!\is_string($permission)) {
                $this->message = 'Permission must be of type string.';
                return false;
            }
            if ($permission === '*') {
                $this->message = 'Wildcard permissions "*" have been replaced. Use "any" instead.';
                return false;
            }
            if (\str_starts_with($permission, 'role:')) {
                $this->message = 'Permissions using the "role:" prefix have been replaced. Use "users", "guests", or "any" instead.';
                return false;
            }

            $matches = [];

            $regex = '/^(?<method>' . \implode('|', $this->methods) . ')\((?<permission>' . \implode('|', $this->permissions) . ')(:(?<id>[a-z\d]+))?(\/(?<dimension>' . \implode('|', $this->dimensions) . '))?\)$/';
            if (!\preg_match($regex, $permission, $matches)) {
                $this->message = 'Permission must be of the form "method(permission:id/dimension)", got "' . $permission . '".  ID and dimension are optional. Method must be one of: ' . \implode(', ', $this->methods) . '. Permission must be one of: ' . \implode(', ', $this->permissions) . '. Dimension must be one of: ' . \implode(', ', $this->dimensions);
                return false;
            }

            $method = $matches['method'];
            $permission = $matches['permission'];
            $id = $matches['id'] ?? '';
            $dimension = $matches['dimension'] ?? '';

            switch ($permission) {
                case 'any':
                case 'guests':
                case 'users':
                    if (!empty($id)) {
                        $this->message = '"' . $permission . '"' . ' permission can not have a value.';
                        return false;
                    }
                    if (!empty($dimension) && !\in_array($dimension, $this->dimensions)) {
                        $this->message = 'Dimension must be one of: ' . \implode(', ', $this->dimensions);
                        return false;
                    }
                    break;
                case 'user':
                case 'member':
                case 'team':
                    if (empty($id)) {
                        $this->message = 'ID must be a valid key: ';
                        return false;
                    }
                    break;
                case 'status':
                    if (!\in_array($id, $this->dimensions)) {
                        $this->message = 'Dimension must be one of: ' . \implode(', ', $this->dimensions);
                        return false;
                    }
                    break;
                default:
                    $this->message = 'Permission must begin with one of: ' . \implode(", ", $this->permissions);
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

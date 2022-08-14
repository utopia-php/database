<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Validator;

class Permissions extends Validator
{
    protected string $message = 'Permissions Error';

    protected array $permissions = [
        ...Database::PERMISSIONS,
        // Validator allows aggregate permissions
        'write',
    ];

    protected array $legacyDimensions = [
        'all',
        'member',
        'guest'
    ];

    protected array $statusDimensions = [
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
                $this->message = 'Wildcard permission "*" has been replaced. Use "any" instead.';
                return false;
            }
            if (\str_contains($permission, 'role:')) {
                $this->message = 'Permissions using the "role:" prefix have been deprecated. Use "users", "guests", or "any" instead.';
            }

            // Parse permissions string in two parts:
            //  1. Match method name against known methods, and capture entire permissions strings.
            //    - Given ["read(users)", "create(user:123abc)", "delete(team:123abc/edit)"]
            //      - The "read", "create" and delete partitions will be matched against all of $this->methods.
            //      - Then "users", "user:123abc", "team:123abc/edit" permission strings will be captured.
            //      - If any invalid method is found, the entire permission parameter will be considered invalid.
            //  2. Match permission name against known permissions and capture permission name, id and dimension.
            //    - Given ["users", "user:123abc", "team:123abc/edit"]
            //      - For each permission string:
            //        - The "users", "user" and "team" partitions will be matched against all of $this->permissions.
            //        - Then first iteration will not match any permission ID or dimension.
            //          - If the permission ID is found, the entire permission string will be considered invalid.
            //        - Then the second iteration will match "123abc" as a permission ID, with type "user".
            //          - If the permission ID is not found, the entire permission parameter will be considered invalid.
            //          - If the permissions ID is not a valid ID, the entire permission parameter will be considered invalid.
            //        - Then the third iteration will match "123abc" as a permissions ID and "edit" as a dimension.
            //          - If the permission ID is not found, the entire permission parameter will be considered invalid.
            //          - If the permissions ID is not a valid ID, the entire permission parameter will be considered invalid.
            //          - If the dimension should be from the known set and is not found, the entire permission parameter will be considered invalid.
            //        - If any invalid permission name is found, the entire permission string will be considered invalid.

            $allowedRoles = \implode('|', Database::ROLES);
            $allowedPermissions = \implode('|', $this->permissions);

            // Inner permission string matcher (e.g. "user:123abc", "team:123abc/role") where role name is matched against known roles.
            $roleMatcher = "(\"(?:{$allowedRoles})(?::(?:[a-zA-Z\d]+[a-zA-Z._\-\d]*))?(?:\/(?:[a-zA-Z\d]+[a-zA-Z._\-]*))?\")";

            // Captures the permission type (e.g. "read", "update") and ensures a role is provided.
            $permissionString = "/^(?:{$allowedPermissions})\({$roleMatcher}\)$/";

            // Inner role string capture. Same as $roleMatcher, but captures the role and optionally the ID and dimension.
            $roleCapture = "/^\"(?<role>{$allowedRoles})(?::(?<id>[a-zA-Z\d]+[a-zA-Z._\-\d]*))?(?:\/(?<dimension>[a-zA-Z\d]+[a-zA-Z._\-]*))?\"$/";

            $matches = [];
            if (!\preg_match($permissionString, $permission, $matches)) {
                $this->message = 'Must be of the form "permission("role:id/dimension")", got "' . $permission . '".';
                return false;
            }

            $submatches = [];
            \array_shift($matches);
            foreach ($matches as $match) {
                if (!\preg_match($roleCapture, $match, $submatches)) {
                    $this->message = 'Must be of the form "role:id/dimension", got "' . $match . '"';
                    return false;
                }

                $role = $submatches['role'];
                $id = $submatches['id'] ?? '';
                $dimension = $submatches['dimension'] ?? '';

                switch ($role) {
                    case 'any':
                    case 'guests':
                    case 'users':
                        if (!empty($id)) {
                            $this->message = '"' . $role . '"' . ' permission can not have a value.';
                            return false;
                        }
                        if (!empty($dimension) && !\in_array($dimension, $this->statusDimensions)) {
                            $this->message = 'Status dimension must be one of: ' . \implode(', ', $this->statusDimensions);
                            return false;
                        }
                        break;
                    /** @noinspection PhpMissingBreakStatementInspection */
                    case 'user':
                        if (!empty($dimension) && !\in_array($dimension, $this->statusDimensions)) {
                            $this->message = 'Status dimension must be one of: ' . \implode(', ', $this->statusDimensions);
                            return false;
                        }
                    case 'member':
                    case 'team':
                        $key = new Key();
                        if (empty($id)) {
                            $this->message = 'ID must not be empty.';
                            return false;
                        }
                        if (!$key->isValid($id)) {
                            $this->message = 'ID must be a valid key: ' . $key->getDescription();
                            return false;
                        }
                        break;
                    case 'status':
                        // Dimension is in the ID position for status permission e.g. "status:verified"
                        if (!\in_array($id, $this->statusDimensions)) {
                            $this->message = 'Status dimension must be one of: ' . \implode(', ', $this->statusDimensions);
                            return false;
                        }
                        break;
                    case 'role':
                        if (empty($id)) {
                            $this->message = 'Role must not be empty.';
                            return false;
                        }
                        if (!\in_array($id, $this->legacyDimensions)) {
                            $this->message = 'Role must be one of: ' . \implode(', ', $this->legacyDimensions);
                            return false;
                        }
                        break;
                    default:
                        $this->message = 'Permission must begin with one of: ' . \implode(", ", $permissions);
                        return false;
                }
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

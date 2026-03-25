<?php

namespace Utopia\Database\Helpers;

use Exception;

/**
 * Represents a role used for permission checks, consisting of a role type, identifier, and dimension.
 */
class Role
{
    /**
     * @param string $role The role type (e.g. user, users, team, any, guests, member, label)
     * @param string $identifier The role identifier (e.g. user ID, team ID)
     * @param string $dimension The role dimension (e.g. user status, team role)
     */
    public function __construct(
        private string $role,
        private string $identifier = '',
        private string $dimension = '',
    ) {
    }

    /**
     * Create a role string from this Role instance.
     *
     * @return string The formatted role string (e.g. 'user:123/verified')
     */
    public function toString(): string
    {
        $str = $this->role;
        if ($this->identifier) {
            $str .= ':'.$this->identifier;
        }
        if ($this->dimension) {
            $str .= '/'.$this->dimension;
        }

        return $str;
    }

    /**
     * Get the role type.
     *
     * @return string
     */
    public function getRole(): string
    {
        return $this->role;
    }

    /**
     * Get the role identifier.
     *
     * @return string
     */
    public function getIdentifier(): string
    {
        return $this->identifier;
    }

    /**
     * Get the role dimension.
     *
     * @return string
     */
    public function getDimension(): string
    {
        return $this->dimension;
    }

    /**
     * Parse a role string into a Role object.
     *
     * @param string $role The role string to parse (e.g. 'user:123/verified')
     * @return self
     * @throws Exception If the dimension format is invalid
     */
    public static function parse(string $role): self
    {
        $roleParts = \explode(':', $role);
        $hasIdentifier = \count($roleParts) > 1;
        $hasDimension = \str_contains($role, '/');
        $role = $roleParts[0];

        if (! $hasIdentifier && ! $hasDimension) {
            return new self($role);
        }

        if ($hasIdentifier && ! $hasDimension) {
            $identifier = $roleParts[1];

            return new self($role, $identifier);
        }

        if (! $hasIdentifier) {
            $dimensionParts = \explode('/', $role);
            if (\count($dimensionParts) !== 2) {
                throw new Exception('Only one dimension can be provided');
            }

            $role = $dimensionParts[0];
            $dimension = $dimensionParts[1];

            if (empty($dimension)) {
                throw new Exception('Dimension must not be empty');
            }

            return new self($role, '', $dimension);
        }

        // Has both identifier and dimension
        $dimensionParts = \explode('/', $roleParts[1]);
        if (\count($dimensionParts) !== 2) {
            throw new Exception('Only one dimension can be provided');
        }

        $identifier = $dimensionParts[0];
        $dimension = $dimensionParts[1];

        if (empty($dimension)) {
            throw new Exception('Dimension must not be empty');
        }

        return new self($role, $identifier, $dimension);
    }

    /**
     * Create a user role from the given ID.
     *
     * @param string $identifier The user ID
     * @param string $status The user status dimension (e.g. 'verified')
     * @return Role
     */
    public static function user(string $identifier, string $status = ''): Role
    {
        return new self('user', $identifier, $status);
    }

    /**
     * Create a users role representing all authenticated users.
     *
     * @param string $status The user status dimension (e.g. 'verified')
     * @return self
     */
    public static function users(string $status = ''): self
    {
        return new self('users', '', $status);
    }

    /**
     * Create a team role from the given ID and dimension.
     *
     * @param string $identifier The team ID
     * @param string $dimension The team role dimension (e.g. 'admin', 'member')
     * @return self
     */
    public static function team(string $identifier, string $dimension = ''): self
    {
        return new self('team', $identifier, $dimension);
    }

    /**
     * Create a label role from the given identifier.
     *
     * @param string $identifier The label identifier
     * @return self
     */
    public static function label(string $identifier): self
    {
        return new self('label', $identifier, '');
    }

    /**
     * Create a role that matches any user, authenticated or not.
     *
     * @return Role
     */
    public static function any(): Role
    {
        return new Role('any');
    }

    /**
     * Create a role representing unauthenticated guest users.
     *
     * @return self
     */
    public static function guests(): self
    {
        return new self('guests');
    }

    /**
     * Create a member role from the given identifier.
     *
     * @param string $identifier The member ID
     * @return self
     */
    public static function member(string $identifier): self
    {
        return new self('member', $identifier);
    }
}

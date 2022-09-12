<?php

namespace Utopia\Database;

class Role
{
    public function __construct(
        private string $role,
        private string $identifier = '',
        private string $dimension = '',
    )
    {
    }

    /**
     * Create a role string from this Role instance
     *
     * @return string
     */
    public function toString(): string
    {
        $str = $this->role;
        if ($this->identifier) {
            $str .= ':' . $this->identifier;
        }
        if ($this->dimension) {
            $str .= '/' . $this->dimension;
        }
        return $str;
    }

    /**
     * @return string
     */
    public function getRole(): string
    {
        return $this->role;
    }

    /**
     * @return string
     */
    public function getIdentifier(): string
    {
        return $this->identifier;
    }

    /**
     * @return string
     */
    public function getDimension(): string
    {
        return $this->dimension;
    }

    /**
     * Parse a role string into a Role object
     *
     * @param string $role
     * @return Role
     * @throws \Exception
     */
    public static function parse(string $role): Role
    {
        $parts = \explode(':', $role);
        $hasIdentifier = \count($parts) > 1;
        $hasDimension = \str_contains($role, '/');
        $role = $parts[0];

        if (!$hasIdentifier && !$hasDimension) {
            return new  Role($role);
        }

        if ($hasIdentifier && !$hasDimension) {
            return new Role($role, $parts[1]);
        }

        if (!$hasIdentifier && $hasDimension) {
            $parts = \explode('/', $role);
            if (\count($parts) !== 2) {
                throw new \Exception('Only one dimension can be provided.');
            }
            if (empty($parts[1])) {
                throw new \Exception('Dimension must not be empty.');
            }
            return new Role($parts[0], '', $parts[1]);
        }

        // Has both identifier and dimension
        $parts = \explode('/', $parts[1]);
        if (\count($parts) !== 2) {
            throw new \Exception('Only one dimension can be provided.');
        }
        if (empty($parts[1])) {
            throw new \Exception('Dimension must not be empty.');
        }
        return new Role($role, $parts[0], $parts[1]);
    }

    /**
     * Create a user role from the given ID
     *
     * @param string $identifier
     * @param string $status
     * @return Role
     */
    public static function user(string $identifier, string $status = ''): Role
    {
        return new Role('user', $identifier, $status);
    }

    /**
     * Create a users role
     *
     * @param string $status
     * @return Role
     */
    public static function users(string $status = ''): Role
    {
        return new Role('users', '', $status);
    }

    /**
     * Create a team role from the given ID and dimension
     *
     * @param string $identifier
     * @param string $dimension
     * @return Role
     */
    public static function team(string $identifier, string $dimension = ''): Role
    {
        return new Role('team', $identifier, $dimension);
    }

    /**
     * Create an any satisfy role
     *
     * @return Role
     */
    public static function any(): Role
    {
        return new Role('any');
    }

    /**
     * Create a guests role
     *
     * @return Role
     */
    public static function guests(): Role
    {
        return new Role('guests');
    }

    public static function member(string $identifier): Role
    {
        return new Role('member', $identifier);
    }

}

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
     */
    public static function parse(string $role): Role
    {
        $parts = \explode(':', $role);

        if (\count($parts) === 1) {
            return new Role($role);
        }
        $role = $parts[0];
        $fullIdentifier = $parts[1];
        $parts = \explode('/', $fullIdentifier);

        if (\count($parts) === 1) {
            return new Role($role, $fullIdentifier);
        }
        $identifier = $parts[0];
        $dimension = $parts[1];

        return new Role(
            $role,
            $identifier,
            $dimension,
        );
    }

    /**
     * Create a user role from the given ID
     *
     * @param string $identifier
     * @return Role
     */
    public static function user(string $identifier): Role
    {
        return new Role('user', $identifier);
    }

    /**
     * Create a users role
     *
     * @return Role
     */
    public static function users(): Role
    {
        return new Role('users');
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

    /**
     * Create a status role from the given status
     *
     * @param string $status
     * @return Role
     */
    public static function status(string $status): Role
    {
        return new Role('status', $status);
    }
}
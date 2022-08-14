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

    public static function fromString(string $role): Role
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

    public static function user(string $identifier): Role
    {
        return new Role('user', $identifier);
    }

    public static function users(): Role
    {
        return new Role('users');
    }

    public static function team(string $identifier, string $dimension = ''): Role
    {
        return new Role('team', $identifier, $dimension);
    }

    public static function any(): Role
    {
        return new Role('any');
    }

    public static function guests(): Role
    {
        return new Role('guests');
    }

    public static function status(string $status): Role
    {
        return new Role('status', $status);
    }

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
}
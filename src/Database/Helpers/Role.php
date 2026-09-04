<?php

namespace Utopia\Database\Helpers;

class Role
{
    /**
     * @param array<Role> $roles
     */
    public function __construct(
        private string $role,
        private string $identifier = '',
        private string $dimension = '',
        private array $roles = [],
    ) {
    }

    /**
     * Create a role string from this Role instance
     *
     * @return string
     */
    public function toString(): string
    {
        if (!empty($this->roles)) {
            return 'allOf(' . \implode(',', \array_map(
                fn (Role $role) => $role->toString(),
                $this->roles
            )) . ')';
        }

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
     * @return array<Role>
     */
    public function getRoles(): array
    {
        return empty($this->roles) ? [$this] : $this->roles;
    }

    /**
     * Parse a role string into a Role object
     *
     * @param string $role
     * @return self
     * @throws \Exception
     */
    public static function parse(string $role): self
    {
        if (\str_starts_with($role, 'allOf(')) {
            if (!\str_ends_with($role, ')')) {
                throw new \Exception('Invalid allOf role format');
            }

            $roles = \explode(',', \substr($role, 6, -1));

            return self::allOf(\array_map(
                fn (string $role) => self::parse($role),
                $roles
            ));
        }

        $roleParts = \explode(':', $role);
        $hasIdentifier = \count($roleParts) > 1;
        $hasDimension = \str_contains($role, '/');
        $role = $roleParts[0];

        if (!$hasIdentifier && !$hasDimension) {
            return new self($role);
        }

        if ($hasIdentifier && !$hasDimension) {
            $identifier = $roleParts[1];
            return new self($role, $identifier);
        }

        if (!$hasIdentifier) {
            $dimensionParts = \explode('/', $role);
            if (\count($dimensionParts) !== 2) {
                throw new \Exception('Only one dimension can be provided');
            }

            $role = $dimensionParts[0];
            $dimension = $dimensionParts[1];

            if (empty($dimension)) {
                throw new \Exception('Dimension must not be empty');
            }
            return new self($role, '', $dimension);
        }

        // Has both identifier and dimension
        $dimensionParts = \explode('/', $roleParts[1]);
        if (\count($dimensionParts) !== 2) {
            throw new \Exception('Only one dimension can be provided');
        }

        $identifier = $dimensionParts[0];
        $dimension = $dimensionParts[1];

        if (empty($dimension)) {
            throw new \Exception('Dimension must not be empty');
        }
        return new self($role, $identifier, $dimension);
    }

    /**
     * Create a user role from the given ID
     *
     * @param string $identifier
     * @param string $status
     * @return self
     */
    public static function user(string $identifier, string $status = ''): Role
    {
        return new self('user', $identifier, $status);
    }

    /**
     * Create a users role
     *
     * @param string $status
     * @return self
     */
    public static function users(string $status = ''): self
    {
        return new self('users', '', $status);
    }

    /**
     * Create a team role from the given ID and dimension
     *
     * @param string $identifier
     * @param string $dimension
     * @return self
     */
    public static function team(string $identifier, string $dimension = ''): self
    {
        return new self('team', $identifier, $dimension);
    }

    /**
     * Create a label role from the given ID
     *
     * @param string $identifier
     * @return self
     */
    public static function label(string $identifier): self
    {
        return new self('label', $identifier, '');
    }

    /**
     * Create an any satisfy role
     *
     * @return self
     */
    public static function any(): Role
    {
        return new Role('any');
    }

    /**
     * Create a guests role
     *
     * @return self
     */
    public static function guests(): self
    {
        return new self('guests');
    }

    public static function member(string $identifier): self
    {
        return new self('member', $identifier);
    }

    /**
     * Require both roles to grant access.
     *
     * @param array<Role> $roles
     */
    public static function allOf(array $roles): self
    {
        if (\count($roles) !== 2) {
            throw new \InvalidArgumentException('allOf requires exactly two roles');
        }

        foreach ($roles as $role) {
            if (!$role instanceof self) {
                throw new \InvalidArgumentException('allOf only accepts Role instances');
            }

            if (\count($role->getRoles()) !== 1) {
                throw new \InvalidArgumentException('Nested allOf roles are not supported');
            }
        }

        \usort($roles, fn (Role $a, Role $b) => $a->toString() <=> $b->toString());

        if ($roles[0]->toString() === $roles[1]->toString()) {
            throw new \InvalidArgumentException('allOf requires two distinct roles');
        }

        return new self('allOf', roles: $roles);
    }
}

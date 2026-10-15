<?php

namespace Tests\E2E\Adapter\Support;

use Utopia\Database\Document;

class User extends Document
{
    public function getEmail(): string
    {
        /** @var string $email */
        $email = $this->getAttribute('email', '');

        return $email;
    }

    public function getName(): string
    {
        /** @var string $name */
        $name = $this->getAttribute('name', '');

        return $name;
    }

    public function isActive(): bool
    {
        return $this->getAttribute('status') === 'active';
    }
}

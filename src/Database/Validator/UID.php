<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;

class UID extends Key
{
    public function __construct(int $maxLength = Database::MAX_UID_DEFAULT_LENGTH)
    {
        parent::__construct(false, $maxLength);
    }

    #[\Override]
    public function getDescription(): string
    {
        return 'UID must contain at most '.$this->maxLength.' chars. Valid chars are a-z, A-Z, 0-9, and underscore. Can\'t start with a leading underscore';
    }
}

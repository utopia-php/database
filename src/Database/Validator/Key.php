<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Validator;

/**
 * Validates key strings ensuring they contain only alphanumeric chars, periods, hyphens, and underscores.
 */
class Key extends Validator
{
    protected string $message;

    #[\Override]
    public function getDescription(): string
    {
        return $this->message;
    }

    public function __construct(
        protected readonly bool $allowInternal = false,
        protected readonly int $maxLength = Database::MAX_UID_DEFAULT_LENGTH,
    ) {
        $this->message = 'Parameter must contain at most '.$this->maxLength.' chars. Valid chars are a-z, A-Z, 0-9, period, hyphen, and underscore. Can\'t start with a special char';
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        if (! \is_string($value)) {
            return false;
        }

        if ($value === '') {
            return false;
        }

        $leading = \mb_substr($value, 0, 1);
        if ($leading === '_' || $leading === '.' || $leading === '-') {
            return false;
        }

        $isInternal = $leading === '$';

        if ($isInternal && ! $this->allowInternal) {
            return false;
        }

        if ($isInternal) {
            $allowList = [Document::ID, Document::CREATED_AT, Document::UPDATED_AT];

            return \in_array($value, $allowList);
        }

        // Valid chars: A-Z, a-z, 0-9, underscore, hyphen, period
        if (\preg_match('/[^A-Za-z0-9\_\-\.]/', $value)) {
            return false;
        }

        if (\mb_strlen($value) > $this->maxLength) {
            return false;
        }

        return true;
    }

    #[\Override]
    public function isArray(): bool
    {
        return false;
    }

    #[\Override]
    public function getType(): string
    {
        return self::TYPE_STRING;
    }
}

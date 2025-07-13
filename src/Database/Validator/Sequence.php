<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class Sequence extends Validator
{
    private string $idAttributeType;

    public function getDescription(): string
    {
        return 'Invalid sequence value';
    }

    /**
     * Expression constructor
     */
    public function __construct(string $idAttributeType)
    {
        $this->idAttributeType = $idAttributeType;
    }

    public function isArray(): bool
    {
        return false;
    }

    public function getType(): string
    {
        if($this->idAttributeType === 'string'){
            return self::TYPE_STRING;
        }

        return self::TYPE_INTEGER;
    }

    public function isValid($value): bool
    {
        return true;
    }
}

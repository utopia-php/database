<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Validator;
use Utopia\Validator\Integer;
use Utopia\Validator\Range;

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
        if ($this->idAttributeType === 'string') {
            return preg_match('/^[a-f0-9]{24}$/i', $value) === 1;
        }
        else if ($this->idAttributeType === 'int') {
            $value = (int)$value;

            $validator = new Integer();
            if (!$validator->isValid($value)){
                return false;
            }

            $validator = new Range(1, Database::BIG_INT_MAX, Database::VAR_INTEGER);
            if (!$validator->isValid($value)){
                return false;
            }

            return true;
        }

        return false;
    }
}

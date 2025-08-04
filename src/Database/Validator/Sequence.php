<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Validator;
use Utopia\Validator\Integer;
use Utopia\Validator\Range;

class Sequence extends Validator
{
    private string $idAttributeType;
    private bool $primary;

    public function getDescription(): string
    {
        return 'Invalid sequence value';
    }

    /**
     * Expression constructor
     */
    public function __construct(string $idAttributeType, bool $primary)
    {
        $this->primary = $primary;
        $this->idAttributeType = $idAttributeType;
    }

    public function isArray(): bool
    {
        return false;
    }

    public function getType(): string
    {
        return self::TYPE_STRING;
    }

    public function isValid($value): bool
    {
        if ($this->primary && empty($value)) {
            return false;
        }

        if (!\is_string($value)) {
            return false;
        }

        switch ($this->idAttributeType) {
            case Database::VAR_OBJECT_ID:
                return preg_match('/^[a-f0-9]{24}$/i', $value) === 1;

            case Database::VAR_INTEGER:
                $start = ($this->primary) ? 1 : 0;
                $validator = new Range($start, Database::BIG_INT_MAX, Database::VAR_INTEGER);
                return $validator->isValid($value);

            default:
                return false;
        }
    }
}

<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class Datetime extends Validator
{
    /**
     * @var string
     */
    protected string $message = 'Date is not valid';

    /**
     * @var bool
     */
    protected bool $allowOldDates = true;

    public function __construct(?bool $allowOldDates = null)
    {
        if (!is_null($allowOldDates)) {
            $this->allowOldDates = $allowOldDates;
        }
    }

    /**
     * Validator Description.
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is valid.
     * Returns true if valid or false if not.
     * @param mixed $value
     * @return bool
     */
    public function isValid($value): bool
    {
        if (empty($value)) {
            return false;
        }

        try {
            $date = new \DateTime($value);
            $now = new \DateTime();

            if ($this->allowOldDates === false && $date < $now) {
                $this->message = 'Date is in the past';
                return false;
            }
        } catch(\Exception $e) {
            $this->message = $e->getMessage();
            return false;
        }

        return true;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_STRING;
    }
}

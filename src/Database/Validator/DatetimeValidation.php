<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class DatetimeValidation extends Validator
{
    /**
     * @var string
     */
    protected string $message = 'Date is not valid';

    public function __construct()
    {

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
     * @param $value
     * @return bool
     */
    public function isValid($value): bool
    {

        if (empty($value)) {
            return false;
        }

        if(strlen($value) === 10){
            $value = $value . ' 00:00:00';
        }

        if(!preg_match("/(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/", $value)){
            return false;
        }

        try {
            new \DateTime($value);
        }
        catch(\Exception $e) {
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

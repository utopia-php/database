<?php

namespace Utopia\Database\Validator;

use Utopia\Validator;

class Datetime extends Validator
{
    public const PRECISION_DAYS = 'days';
    public const PRECISION_HOURS = 'hours';
    public const PRECISION_MINUTES = 'minutes';
    public const PRECISION_SECONDS = 'seconds';
    public const PRECISION_ANY = 'any';

    /**
     * @var string
     */
    protected string $precision = self::PRECISION_ANY;

    /**
     * @var bool
     */
    protected bool $requireDateInFuture = false;

    public function __construct(?bool $requireDateInFuture = false, ?string $precision = self::PRECISION_ANY)
    {
        $this->requireDateInFuture = $requireDateInFuture;
        $this->precision = $precision;
    }

    /**
     * Validator Description.
     * @return string
     */
    public function getDescription(): string
    {
        $message = 'Value must be valid date';

        if($this->requireDateInFuture) {
            $message .= " in future";
        }

        if($this->precision !== self::PRECISION_ANY) {
            $message .= " with " . $this->precision . " precision";
        }

        $message .= '.';
        return $message;
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

            if ($this->requireDateInFuture === true && $date < $now) {
                return false;
            }

            // Constants from: https://www.php.net/manual/en/datetime.format.php
            $denyConstants = [];

            switch ($this->precision) {
                case self::PRECISION_DAYS:
                    $denyConstants = [ 'H', 'i', 's', 'v' ];
                    break;
                case self::PRECISION_HOURS:
                    $denyConstants = [ 'i', 's', 'v' ];
                    break;
                case self::PRECISION_MINUTES:
                    $denyConstants = [ 's', 'v' ];
                    break;
                case self::PRECISION_SECONDS:
                    $denyConstants = [ 'v' ];
                    break;
            }

            foreach($denyConstants as $constant) {
                if(\intval($date->format($constant)) !== 0) {
                    return false;
                }
            }
        } catch(\Exception $e) {
            return false;
        }

        [$year] = explode('-', $value);

        if ((int)$year > 9999) {
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

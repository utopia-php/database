<?php

namespace Utopia\Database\Exception;

use Throwable;
use Utopia\Database\Exception;

/**
 * Thrown when a query order clause is invalid or references an unsupported attribute.
 */
class Order extends Exception
{
    protected ?string $attribute;

    /**
     * @param string $message The exception message
     * @param int|string $code The exception code
     * @param Throwable|null $previous The previous throwable for chaining
     * @param string|null $attribute The attribute that caused the ordering error
     */
    public function __construct(string $message, int|string $code = 0, ?Throwable $previous = null, ?string $attribute = null)
    {
        $this->attribute = $attribute;
        parent::__construct($message, $code, $previous);
    }

    /**
     * Get the attribute that caused the ordering error.
     *
     * @return string|null
     */
    public function getAttribute(): ?string
    {
        return $this->attribute;
    }
}

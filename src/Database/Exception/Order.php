<?php

namespace Utopia\Database\Exception;

use Throwable;

/**
 * Thrown when a query order clause is invalid or references an unsupported attribute.
 */
class Order extends Query
{
    /**
     * @param string|null $attribute The attribute that caused the ordering error
     */
    public function __construct(
        string $message,
        protected readonly ?string $attribute = null,
        int|string $code = 0,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * Get the attribute that caused the ordering error.
     */
    public function getAttribute(): ?string
    {
        return $this->attribute;
    }
}

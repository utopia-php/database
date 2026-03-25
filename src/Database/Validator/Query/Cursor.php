<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\UID;
use Utopia\Query\Method;

/**
 * Validates cursor-based pagination queries (cursorAfter and cursorBefore).
 */
class Cursor extends Base
{
    /**
     * Create a new cursor query validator.
     *
     * @param int $maxLength Maximum allowed UID length for cursor values
     */
    public function __construct(private readonly int $maxLength = Database::MAX_UID_DEFAULT_LENGTH)
    {
    }

    /**
     * Is valid.
     *
     * Returns true if method is cursorBefore or cursorAfter and value is not null
     *
     * Otherwise, returns false
     *
     * @param  mixed  $value
     */
    public function isValid($value): bool
    {
        if (! $value instanceof Query) {
            return false;
        }

        $method = $value->getMethod();

        if ($method === Method::CursorAfter || $method === Method::CursorBefore) {
            $cursor = $value->getValue();

            if ($cursor instanceof Document) {
                $cursor = $cursor->getId();
            }

            $validator = new UID($this->maxLength);
            if ($validator->isValid($cursor)) {
                return true;
            }
            $this->message = 'Invalid cursor: '.$validator->getDescription();

            return false;
        }

        return false;
    }

    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_CURSOR;
    }
}

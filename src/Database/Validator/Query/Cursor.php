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
     * @param int $maxLength Maximum allowed UID length for cursor values
     */
    public function __construct(private readonly int $maxLength = Database::MAX_UID_DEFAULT_LENGTH)
    {
    }

    /**
     * A cursorAfter or cursorBefore query whose value is a document ID, or a document holding one. A document
     * without an ID is a row a join or a distinct read returned; the read decides whether its values name a row.
     * Any other value, an array included, is refused.
     *
     * @param  mixed  $value
     */
    #[\Override]
    public function isValid(mixed $value): bool
    {
        if (! $value instanceof Query) {
            return false;
        }

        $method = $value->getMethod();

        if ($method === Method::CursorAfter || $method === Method::CursorBefore) {
            $cursor = $value->getValue();

            if ($cursor instanceof Document) {
                if ($cursor->getId() === '') {
                    return true;
                }

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

    #[\Override]
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_CURSOR;
    }
}

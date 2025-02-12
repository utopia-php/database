<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Validator;

class Queries extends Validator
{
    /**
     * @var string
     */
    protected string $message = 'Invalid queries';

    /**
     * @var array<Base>
     */
    protected array $validators;

    /**
     * @var int
     */
    protected int $length;

    /**
     * Queries constructor
     *
     * @param array<Base> $validators
     */
    public function __construct(array $validators = [], int $length = 0)
    {
        $this->validators = $validators;
        $this->length = $length;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * @param array<Query|string> $value
     * @return bool
     */
    public function isValid($value): bool
    {
        if (!is_array($value)) {
            $this->message = 'Queries must be an array';
            return false;
        }

        if ($this->length && \count($value) > $this->length) {
            return false;
        }

        foreach ($value as $query) {
            if (!$query instanceof Query) {
                try {
                    $query = Query::parse($query);
                } catch (\Throwable $e) {
                    $this->message = 'Invalid query: ' . $e->getMessage();
                    return false;
                }
            }

            if ($query->isNested()) {
                if (!self::isValid($query->getValues())) {
                    return false;
                }
            }

            $method = $query->getMethod();
            $methodType = match ($method) {
                Query::TYPE_SELECT => Base::METHOD_TYPE_SELECT,
                Query::TYPE_LIMIT => Base::METHOD_TYPE_LIMIT,
                Query::TYPE_OFFSET => Base::METHOD_TYPE_OFFSET,
                Query::TYPE_CURSOR_AFTER,
                Query::TYPE_CURSOR_BEFORE => Base::METHOD_TYPE_CURSOR,
                Query::TYPE_ORDER_ASC,
                Query::TYPE_ORDER_DESC => Base::METHOD_TYPE_ORDER,
                Query::TYPE_EQUAL,
                Query::TYPE_NOT_EQUAL,
                Query::TYPE_LESSER,
                Query::TYPE_LESSER_EQUAL,
                Query::TYPE_GREATER,
                Query::TYPE_GREATER_EQUAL,
                Query::TYPE_SEARCH,
                Query::TYPE_IS_NULL,
                Query::TYPE_IS_NOT_NULL,
                Query::TYPE_BETWEEN,
                Query::TYPE_STARTS_WITH,
                Query::TYPE_CONTAINS,
                Query::TYPE_ENDS_WITH,
                Query::TYPE_AND,
                Query::TYPE_OR => Base::METHOD_TYPE_FILTER,
                default => '',
            };

            $methodIsValid = false;
            foreach ($this->validators as $validator) {
                if ($validator->getMethodType() !== $methodType) {
                    continue;
                }
                if (!$validator->isValid($query)) {
                    $this->message = 'Invalid query: ' . $validator->getDescription();
                    return false;
                }

                $methodIsValid = true;
            }

            if (!$methodIsValid) {
                $this->message = 'Invalid query method: ' . $method;
                return false;
            }
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
        return true;
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
        return self::TYPE_OBJECT;
    }
}

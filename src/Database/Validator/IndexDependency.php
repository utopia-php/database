<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Attribute as AttributeVO;
use Utopia\Database\Document;
use Utopia\Database\Index as IndexVO;
use Utopia\Validator;

/**
 * Validates that an attribute can be safely deleted or renamed by checking for index dependencies.
 */
class IndexDependency extends Validator
{
    protected string $message = "Attribute can't be deleted or renamed because it is used in an index";

    protected bool $castIndexSupport;

    /**
     * @var array<IndexVO>
     */
    protected array $indexes;

    /**
     * @param  array<IndexVO|Document>  $indexes
     */
    public function __construct(array $indexes, bool $castIndexSupport)
    {
        $this->castIndexSupport = $castIndexSupport;
        $this->indexes = [];
        foreach ($indexes as $index) {
            $this->indexes[] = $index instanceof IndexVO ? $index : IndexVO::fromDocument($index);
        }
    }

    /**
     * Returns validator description
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is valid.
     *
     * @param  AttributeVO|Document  $value
     */
    public function isValid($value): bool
    {
        if (! $this->castIndexSupport) {
            return true;
        }

        $attr = $value instanceof AttributeVO ? $value : AttributeVO::fromDocument($value);

        if (! $attr->array) {
            return true;
        }

        $key = \strtolower($attr->key);

        foreach ($this->indexes as $index) {
            foreach ($index->attributes as $attribute) {
                if ($key === \strtolower($attribute)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }
}

<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Validator;

/**
 * Validates that an attribute can be safely deleted or renamed by checking for index dependencies.
 */
class IndexDependency extends Validator
{
    protected string $message = "Attribute can't be deleted or renamed because it is used in an index";

    protected bool $castIndexSupport;

    /**
     * @var list<Index>
     */
    protected array $indexes;

    /**
     * @param  array<Index|Document>  $indexes
     */
    public function __construct(array $indexes, bool $castIndexSupport)
    {
        $this->castIndexSupport = $castIndexSupport;
        $this->indexes = [];
        foreach ($indexes as $index) {
            $this->indexes[] = $index instanceof Index ? $index : Index::fromDocument($index);
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
     * @param  Attribute|Document  $value
     */
    public function isValid($value): bool
    {
        if (! $this->castIndexSupport) {
            return true;
        }

        $attribute = $value instanceof Attribute ? $value : Attribute::fromDocument($value);

        if (! $attribute->array) {
            return true;
        }

        $key = \strtolower($attribute->key);

        foreach ($this->indexes as $index) {
            foreach ($index->attributes as $indexedAttribute) {
                if ($key === \strtolower($indexedAttribute)) {
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

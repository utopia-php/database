<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

/**
 * Validates groupBy query methods ensuring the grouped attributes exist in the schema.
 */
class GroupBy extends Base
{
    /**
     * @var array<string, true>
     */
    protected array $schema = [];

    protected bool $joinedAttributes = false;

    /**
     * @param  array<Document>  $attributes
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true)
    {
        foreach ($attributes as $attribute) {
            $key = $attribute->getAttribute('key', $attribute->getAttribute(Document::ID));

            if (\is_string($key)) {
                $this->schema[$key] = true;
            }
        }

        foreach (Database::internalAttributes() as $attribute) {
            $this->schema[$attribute->key] = true;
        }
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_GROUP_BY;
    }

    protected function isValidQuery(Query $query): bool
    {
        $columns = $query->getValues();

        if (empty($columns)) {
            $this->message = 'GroupBy requires at least one attribute';

            return false;
        }

        foreach ($columns as $column) {
            if (! \is_string($column) || $column === '') {
                $this->message = 'GroupBy attributes must be non-empty strings';

                return false;
            }

            if (! $this->joinedAttributes && $this->supportForAttributes && ! isset($this->schema[$column])) {
                $this->message = 'Attribute not found in schema: '.$column;

                return false;
            }
        }

        return true;
    }

    /**
     * Stand the schema check down for a query set that joins: the joined collection's
     * attributes are legitimate operands here and are not in this collection's schema.
     */
    public function allowJoinedAttributes(): void
    {
        $this->joinedAttributes = true;
    }

    public function resetJoinedAttributes(): void
    {
        $this->joinedAttributes = false;
    }
}

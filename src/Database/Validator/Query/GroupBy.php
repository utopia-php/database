<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Document;
use Utopia\Database\Query;

/**
 * Validates groupBy query methods ensuring the grouped attributes exist in the schema.
 */
class GroupBy extends Base
{
    use JoinedAttributes;

    /**
     * @var array<string, true>
     */
    protected array $schema = [];

    /**
     * @param  array<Document>  $attributes
     * @param  bool  $sharedTables  Whether the tables hold `$tenant`, as they do under shared tables
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true, bool $sharedTables = false)
    {
        foreach ($attributes as $attribute) {
            $key = $attribute->getAttribute('key', $attribute->getAttribute(Document::ID));

            if (\is_string($key)) {
                $this->schema[$key] = true;
            }
        }

        $this->schema += self::internalColumns($sharedTables);
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

            if (
                $this->supportForAttributes
                && ! isset($this->schema[$column])
                && ! $this->isJoinedAttribute($column)
            ) {
                return false;
            }
        }

        return true;
    }

    protected function acceptsMainAttribute(string $attribute): bool
    {
        return isset($this->schema[$attribute]);
    }
}

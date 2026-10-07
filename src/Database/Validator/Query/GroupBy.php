<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Joined\Attributes;
use Utopia\Database\Validator\Query\Joined\Collection;

/**
 * Validates groupBy query methods ensuring the grouped attributes exist in the schema.
 */
class GroupBy extends Base
{
    use Attributes;

    /**
     * @var array<string, true>
     */
    protected array $schema = [];

    /**
     * Every attribute of the collection, and whether it holds a column.
     *
     * @var array<string, bool>
     */
    protected array $columns = [];

    /**
     * @param  array<Attribute|Document>  $attributes
     * @param  bool  $sharedTables  Whether the tables hold `$tenant`, as they do under shared tables
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true, bool $sharedTables = false)
    {
        $attributes = \array_map(
            static fn (Attribute|Document $attribute): Attribute => $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute),
            $attributes,
        );

        foreach ($attributes as $attribute) {
            $this->schema[$attribute->key] = true;
        }

        $this->schema += self::internalColumns($sharedTables);
        $this->columns = Collection::columns($attributes);
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

            if (($this->columns[$column] ?? true) === false) {
                $this->message = 'Cannot group by virtual relationship attribute: '.$column;

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

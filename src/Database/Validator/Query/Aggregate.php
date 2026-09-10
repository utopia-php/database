<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

/**
 * Validates aggregate query methods ensuring the aggregated attribute exists in the schema.
 */
class Aggregate extends Base
{
    private const ALIAS_PATTERN = '/^[A-Za-z_][A-Za-z0-9_]*$/';

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
        return self::METHOD_TYPE_AGGREGATE;
    }

    protected function isValidQuery(Query $query): bool
    {
        $attribute = $query->getAttribute();

        if (
            $attribute !== '*'
            && ! $this->joinedAttributes
            && $this->supportForAttributes
            && ! isset($this->schema[$attribute])
        ) {
            $this->message = 'Attribute not found in schema: '.$attribute;

            return false;
        }

        $alias = $query->getValues()[0] ?? null;

        if ($alias !== null && (! \is_string($alias) || \preg_match(self::ALIAS_PATTERN, $alias) !== 1)) {
            $this->message = 'Invalid aggregate alias';

            return false;
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

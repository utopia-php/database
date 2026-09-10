<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

class Nested extends Base
{
    /**
     * @var array<int|string, mixed>
     */
    protected array $schema = [];

    /**
     * @param array<Document> $attributes
     * @param int $maxUIDLength
     * @param bool $supportForAttributes
     */
    public function __construct(
        array $attributes,
        protected int $maxUIDLength = 36,
        protected bool $supportForAttributes = true
    ) {
        foreach ($attributes as $attribute) {
            $this->schema[$attribute->getAttribute('key', $attribute->getId())] = $attribute->getArrayCopy();
        }
    }

    /**
     * @param Query $value
     * @return bool
     */
    public function isValid($value): bool
    {
        if (!$value instanceof Query) {
            return false;
        }

        if ($value->getMethod() !== Query::TYPE_NESTED) {
            $this->message = 'Invalid query method: ' . $value->getMethod();
            return false;
        }

        $attribute = $value->getAttribute();

        if (empty($attribute)) {
            $this->message = 'Nested queries require a relationship attribute';
            return false;
        }

        if ($this->supportForAttributes) {
            if (
                !isset($this->schema[$attribute])
                || $this->schema[$attribute]['type'] !== Database::VAR_RELATIONSHIP
            ) {
                $this->message = 'Nested queries can only be used on relationship attributes: ' . $attribute;
                return false;
            }
        }

        $queries = $value->getValues();

        if (empty($queries)) {
            $this->message = 'Nested queries can only contain queries';
            return false;
        }

        foreach ($queries as $query) {
            if (!$query instanceof Query) {
                $this->message = 'Nested queries can only contain queries';
                return false;
            }

            if ($query->getMethod() === Query::TYPE_NESTED) {
                $this->message = 'Nested queries cannot be nested';
                return false;
            }
        }

        $hasPagination = false;

        foreach ($queries as $query) {
            $validator = match ($query->getMethod()) {
                Query::TYPE_LIMIT => new Limit(),
                Query::TYPE_OFFSET => new Offset(),
                Query::TYPE_CURSOR_AFTER,
                Query::TYPE_CURSOR_BEFORE => new Cursor($this->maxUIDLength),
                default => null,
            };

            if ($validator === null) {
                continue;
            }

            $hasPagination = true;

            if (!$validator->isValid($query)) {
                $this->message = $validator->getDescription();
                return false;
            }
        }

        if (!$hasPagination || !$this->supportForAttributes || !isset($this->schema[$attribute]['options'])) {
            return true;
        }

        $options = $this->schema[$attribute]['options'];
        $relationType = $options['relationType'] ?? null;
        $side = $options['side'] ?? null;

        $isSingular = $relationType === Database::RELATION_ONE_TO_ONE
            || ($relationType === Database::RELATION_MANY_TO_ONE && $side === Database::RELATION_SIDE_PARENT)
            || ($relationType === Database::RELATION_ONE_TO_MANY && $side === Database::RELATION_SIDE_CHILD);

        if ($isSingular) {
            $this->message = 'Nested pagination is not supported on a singular relationship: ' . $attribute;
            return false;
        }

        return true;
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_NESTED;
    }
}

<?php

namespace Utopia\Database\Validator\Queries;

use DateTime;
use Exception;
use Utopia\Database\Attribute;
use Utopia\Database\Document as BaseDocument;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Schema\ColumnType;

/**
 * Validates queries for single document retrieval: selections of the document's attributes, and,
 * unless turned off, joins whose conditions meet the filter rules a listing applies to them.
 */
class Document extends Queries
{
    /**
     * @var array<Attribute>
     */
    private readonly array $attributes;

    private ?Queries $conditions = null;

    /**
     * @param  array<Attribute|BaseDocument>  $attributes
     * @param  bool  $sharedTables  Whether the tables hold `$tenant`, as they do under shared tables
     * @param  bool  $supportForJoins  Whether join queries are accepted
     *
     * @throws Exception
     */
    public function __construct(
        array $attributes,
        private readonly bool $supportForAttributes = true,
        private readonly string $idAttributeType = ColumnType::Integer->value,
        private readonly int $maxValuesCount = 5000,
        private readonly DateTime $minAllowedDate = new DateTime('0000-01-01'),
        private readonly DateTime $maxAllowedDate = new DateTime('9999-12-31'),
        private readonly bool $supportUnsignedBigInt = true,
        bool $sharedTables = false,
        bool $supportForJoins = true,
    ) {
        $attributes = [
            ...\array_map(
                static fn (Attribute|BaseDocument $attribute): Attribute => $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute),
                $attributes,
            ),
            ...Documents::internalAttributes(),
        ];

        $this->attributes = $attributes;

        $validators = [new Select($attributes, $supportForAttributes, $sharedTables)];

        if ($supportForJoins) {
            $validators[] = new Join($attributes, $supportForAttributes);
        }

        parent::__construct($validators);
    }

    /**
     * Filters stay invalid at the top level of a document read, but the conditions of its joins
     * are checked as a listing checks them.
     *
     * @param  mixed  $value
     */
    public function isValid($value): bool
    {
        if (! parent::isValid($value)) {
            return false;
        }

        /** @var array<Query|string> $value */
        $joins = [];
        $nested = false;
        foreach ($value as $query) {
            $query = $query instanceof Query ? $query : Query::parse($query);

            if ($query->getMethod()->isJoin()) {
                $joins[] = $query;
                $nested = $nested || $query->isNestedJoin();
            }
        }

        if (! $nested) {
            return true;
        }

        $conditions = $this->conditions ??= new Queries([
            new Filter(
                $this->attributes,
                $this->idAttributeType,
                $this->maxValuesCount,
                $this->minAllowedDate,
                $this->maxAllowedDate,
                $this->supportForAttributes,
                $this->supportUnsignedBigInt,
            ),
            new Join($this->attributes, $this->supportForAttributes),
        ]);
        $conditions->setJoinedCollections(\array_values($this->joinedCollections));

        if (! $conditions->isValid($joins)) {
            $this->message = $conditions->getDescription();

            return false;
        }

        return true;
    }
}

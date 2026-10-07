<?php

namespace Utopia\Database\Validator\Queries;

use Exception;
use Utopia\Database\Adapter\Profile;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Document as BaseDocument;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Select;

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
     *
     * @throws Exception
     */
    public function __construct(
        array $attributes,
        private readonly Profile $profile,
        private readonly int $maxValuesCount = 5000,
    ) {
        $attributes = [
            ...\array_map(
                static fn (Attribute|BaseDocument $attribute): Attribute => $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute),
                $attributes,
            ),
            ...Documents::internalAttributes(),
        ];

        $this->attributes = $attributes;

        $supportForAttributes = $profile->supports(Capability::DefinedAttributes);
        $validators = [new Select($attributes, $supportForAttributes, $profile->sharedTables)];

        if ($profile->supports(Capability::Joins)) {
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

        $supportForAttributes = $this->profile->supports(Capability::DefinedAttributes);
        $limits = $this->profile->limits;
        $conditions = $this->conditions ??= new Queries([
            new Filter(
                $this->attributes,
                $limits->idType->value,
                $this->maxValuesCount,
                $limits->minDateTime,
                $limits->maxDateTime,
                $supportForAttributes,
                $this->profile->supports(Capability::UnsignedBigInt),
            ),
            new Join($this->attributes, $supportForAttributes),
        ]);
        $conditions->setJoinedCollections(\array_values($this->joinedCollections));

        if (! $conditions->isValid($joins)) {
            $this->message = $conditions->getDescription();

            return false;
        }

        return true;
    }
}

<?php

namespace Utopia\Database\Validator\Queries;

use Utopia\Database\Adapter\Profile;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Distinct;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Database\Validator\Query\Having;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;
use Utopia\Database\Validator\Query\Select;

/**
 * Validates queries for document listing: filters, ordering, selection and pagination, plus joins
 * and aggregations (aggregate functions, group by, having and distinct) when enabled.
 */
class Documents extends Indexed
{
    /**
     * @var list<Attribute>|null
     */
    private static ?array $internalAttributes = null;

    /**
     * @param  array<Attribute|Document>  $attributes
     * @param  array<Index|Document>  $indexes
     *
     * @throws \Utopia\Database\Exception
     */
    public function __construct(
        array $attributes,
        array $indexes,
        Profile $profile,
        int $maxValuesCount = 5000,
    ) {
        $attributes = [
            ...\array_map(
                static fn (Attribute|Document $attribute): Attribute => $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute),
                $attributes,
            ),
            ...self::internalAttributes(),
        ];

        $supportForAttributes = $profile->supports(Capability::DefinedAttributes);
        $limits = $profile->limits;

        $validators = [
            new Limit(),
            new Offset(),
            new Cursor($limits->uidLength),
            new Filter(
                $attributes,
                $limits->idType->value,
                $maxValuesCount,
                $limits->minDateTime,
                $limits->maxDateTime,
                $supportForAttributes,
                $profile->supports(Capability::UnsignedBigInt),
            ),
            new Order($attributes, $supportForAttributes, $profile->supports(Capability::OrderRandom)),
            new Select($attributes, $supportForAttributes, $profile->sharedTables),
        ];

        if ($profile->supports(Capability::Joins)) {
            $validators[] = new Join($attributes, $supportForAttributes);
        }

        if ($profile->supports(Capability::Aggregations)) {
            \array_push(
                $validators,
                new Aggregate($attributes, $supportForAttributes, $profile->sharedTables),
                new GroupBy($attributes, $supportForAttributes, $profile->sharedTables),
                new Having(),
                new Distinct(),
            );
        }

        parent::__construct($attributes, $indexes, $validators);
    }

    /**
     * The attributes every collection holds besides its own that queries may name like the collection's
     * attributes: the document id, sequence and timestamps.
     *
     * @internal
     *
     * @return list<Attribute>
     */
    public static function internalAttributes(): array
    {
        return self::$internalAttributes ??= [
            Attribute::string(Document::ID),
            Attribute::id(Document::SEQUENCE),
            Attribute::datetime(Document::CREATED_AT),
            Attribute::datetime(Document::UPDATED_AT),
        ];
    }
}

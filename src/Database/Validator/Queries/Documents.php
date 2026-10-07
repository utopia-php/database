<?php

namespace Utopia\Database\Validator\Queries;

use DateTime;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Validator\IndexedQueries;
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
class Documents extends IndexedQueries
{
    /**
     * @var list<Attribute>|null
     */
    private static ?array $internalAttributes = null;

    /**
     * @param  array<Attribute|Document>  $attributes
     * @param  array<Index|Document>  $indexes
     * @param  bool  $sharedTables  Whether the tables hold `$tenant`, as they do under shared tables
     * @param  bool  $supportForOrderRandom  Whether the adapter can order by random (Capability::OrderRandom)
     *
     * @throws \Utopia\Database\Exception
     */
    public function __construct(
        array $attributes,
        array $indexes,
        string $idAttributeType,
        int $maxValuesCount = 5000,
        int $maxUIDLength = 36,
        DateTime $minAllowedDate = new DateTime('0000-01-01'),
        DateTime $maxAllowedDate = new DateTime('9999-12-31'),
        bool $supportForAttributes = true,
        bool $supportUnsignedBigInt = true,
        bool $supportForJoins = false,
        bool $supportForAggregations = false,
        bool $sharedTables = false,
        bool $supportForOrderRandom = true,
    ) {
        $attributes = [
            ...\array_map(
                static fn (Attribute|Document $attribute): Attribute => $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute),
                $attributes,
            ),
            ...self::internalAttributes(),
        ];

        $validators = [
            new Limit(),
            new Offset(),
            new Cursor($maxUIDLength),
            new Filter(
                $attributes,
                $idAttributeType,
                $maxValuesCount,
                $minAllowedDate,
                $maxAllowedDate,
                $supportForAttributes,
                $supportUnsignedBigInt
            ),
            new Order($attributes, $supportForAttributes, $supportForOrderRandom),
            new Select($attributes, $supportForAttributes, $sharedTables),
        ];

        if ($supportForJoins) {
            $validators[] = new Join($attributes, $supportForAttributes);
        }

        if ($supportForAggregations) {
            \array_push(
                $validators,
                new Aggregate($attributes, $supportForAttributes, $sharedTables),
                new GroupBy($attributes, $supportForAttributes, $sharedTables),
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

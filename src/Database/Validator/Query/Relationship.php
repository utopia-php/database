<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Query\Method;

/**
 * Validates a relationship query: the attribute it names is a relationship, and the queries it holds shape the
 * related documents without nesting another relationship query or paginating a relationship that holds one document.
 *
 * The filters, orders and selects it holds name the related collection's attributes, so they are checked when the
 * related documents are read, not here.
 */
class Relationship extends Base
{
    private const array PAGINATION_METHODS = [
        Method::Limit,
        Method::Offset,
        Method::CursorAfter,
        Method::CursorBefore,
    ];

    /**
     * @var array<string, Attribute>
     */
    private array $schema = [];

    /**
     * @param  array<Attribute|Document>  $attributes
     */
    public function __construct(
        array $attributes,
        private readonly int $maxUIDLength = Database::MAX_UID_DEFAULT_LENGTH,
        private readonly bool $supportForAttributes = true,
    ) {
        foreach ($attributes as $attribute) {
            $attribute = $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute);
            $this->schema[$attribute->key] = $attribute;
        }
    }

    #[\Override]
    protected function isValidQuery(Query $query): bool
    {
        if ($query->getMethod() !== Method::Relationship) {
            $this->message = 'Invalid query method: '.$query->getMethod()->value;

            return false;
        }

        $key = $query->getAttribute();

        if ($key === '') {
            $this->message = 'Relationship queries require a relationship attribute';

            return false;
        }

        $attribute = $this->schema[$key] ?? null;

        if ($this->supportForAttributes && $attribute?->relationship === null) {
            $this->message = 'Relationship queries can only be used on relationship attributes: '.$key;

            return false;
        }

        $queries = $query->getValues();

        if ($queries === []) {
            $this->message = 'Relationship queries can only contain queries';

            return false;
        }

        $paginated = false;

        foreach ($queries as $inner) {
            if (! $inner instanceof Query) {
                $this->message = 'Relationship queries can only contain queries';

                return false;
            }

            if (self::holdsRelationshipQuery($inner)) {
                $this->message = 'Relationship queries cannot contain relationship queries';

                return false;
            }

            $validator = match ($inner->getMethod()) {
                Method::Select => new Select(supportForAttributes: false),
                Method::Limit => new Limit(),
                Method::Offset => new Offset(),
                Method::CursorAfter, Method::CursorBefore => new Cursor($this->maxUIDLength),
                default => null,
            };

            if ($validator !== null && ! $validator->isValid($inner)) {
                $this->message = $validator->getDescription();

                return false;
            }

            $paginated = $paginated || \in_array($inner->getMethod(), self::PAGINATION_METHODS, true);
        }

        if ($paginated && $this->supportForAttributes && $attribute !== null && self::holdsOneDocument($attribute)) {
            $this->message = 'Relationship pagination is not supported on a singular relationship: '.$key;

            return false;
        }

        return true;
    }

    private static function holdsRelationshipQuery(Query $query): bool
    {
        if ($query->getMethod() === Method::Relationship) {
            return true;
        }

        if (! \in_array($query->getMethod(), Query::LOGICAL_TYPES, true)) {
            return false;
        }

        foreach ($query->getValues() as $value) {
            if ($value instanceof Query && self::holdsRelationshipQuery($value)) {
                return true;
            }
        }

        return false;
    }

    private static function holdsOneDocument(Attribute $attribute): bool
    {
        return match ($attribute->relationship?->type) {
            RelationshipType::OneToOne => true,
            RelationshipType::ManyToOne => $attribute->side === RelationshipSide::Parent,
            RelationshipType::OneToMany => $attribute->side === RelationshipSide::Child,
            default => false,
        };
    }

    #[\Override]
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_RELATIONSHIP;
    }
}

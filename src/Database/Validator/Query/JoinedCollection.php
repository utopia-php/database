<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;

/**
 * A collection one join of a query set reads, as the query validators check it: the alias its
 * columns are referenced by and what the collection declares.
 */
final readonly class JoinedCollection
{
    /**
     * @param  string  $alias  The alias the join declares, empty when it declares none
     * @param  array<string, true>  $attributes  The attributes the collection declares, relationships left out
     * @param  array<string, ColumnType>  $numeric  The type of each attribute that holds a single number
     * @param  array<string, true>  $encrypted  The attributes whose values are stored encrypted
     * @param  array<string, bool>  $columns  Every attribute the collection declares, and whether it holds a column a join condition can compare
     * @param  string  $collection  The id of the collection the join reads
     */
    public function __construct(
        public string $alias,
        public array $attributes,
        public array $numeric,
        public array $encrypted = [],
        public array $columns = [],
        public string $collection = '',
    ) {
    }

    /**
     * The collection a join reads, under the alias the join declares.
     */
    public static function of(string $alias, Document $collection): self
    {
        /** @var array<Attribute|Document> $definitions */
        $definitions = $collection->getAttribute('attributes', []);

        $attributes = [];
        $encrypted = [];
        foreach ($definitions as $definition) {
            if (! Attribute::isRelationship($definition)) {
                $attributes[$definition->getId()] = true;
            }

            $filters = $definition->getAttribute('filters', []);
            if (\is_array($filters) && \in_array('encrypt', $filters, true)) {
                $encrypted[$definition->getId()] = true;
            }
        }

        return new self(
            $alias,
            $attributes,
            Aggregate::numericTypes($definitions),
            $encrypted,
            self::columns($definitions),
            $collection->getId(),
        );
    }

    /**
     * Every attribute of a collection, and whether its table holds a column for it: every attribute
     * but a relationship does, and a relationship does on the side that stores the related
     * document's id.
     *
     * @param  array<Attribute|Document>  $attributes
     * @return array<string, bool>
     */
    public static function columns(array $attributes): array
    {
        $columns = [];
        foreach ($attributes as $attribute) {
            $key = $attribute->getAttribute('key', $attribute->getId());
            if (! \is_string($key) || $key === '') {
                continue;
            }

            $columns[$key] = ! Attribute::isRelationship($attribute) || self::storesRelatedId(Relationship::fromDocument('', $attribute));
        }

        return $columns;
    }

    private static function storesRelatedId(Relationship $relationship): bool
    {
        return match ($relationship->type) {
            RelationType::OneToOne => $relationship->side === RelationSide::Parent || $relationship->twoWay,
            RelationType::OneToMany => $relationship->side === RelationSide::Child,
            RelationType::ManyToOne => $relationship->side === RelationSide::Parent,
            RelationType::ManyToMany => false,
        };
    }
}

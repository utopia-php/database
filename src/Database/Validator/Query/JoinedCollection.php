<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Query\Schema\ColumnType;

/**
 * A collection one join of a query set reads, as the query validators check it: the alias its
 * columns are referenced by and what the collection declares.
 */
final readonly class JoinedCollection
{
    private const string ENCRYPT = 'encrypt';

    /**
     * @param  string  $alias  The alias the join declares, empty when it declares none
     * @param  array<string, true>  $attributes  The attributes the collection declares, relationships left out
     * @param  array<string, ColumnType>  $numeric  The type of each attribute that holds a single number
     * @param  array<string, true>  $encrypted  The attributes whose values are stored encrypted
     * @param  array<string, bool>  $columns  Every attribute the collection declares, and whether it holds a column a join condition can compare
     * @param  string  $collection  The id of the collection the join reads
     * @param  array<string, array<string, mixed>>  $schema  The definition of each attribute in $attributes, its type a ColumnType, as Filter holds the main collection's
     */
    public function __construct(
        public string $alias,
        public array $attributes,
        public array $numeric,
        public array $encrypted = [],
        public array $columns = [],
        public string $collection = '',
        public array $schema = [],
    ) {
    }

    /**
     * The collection a join reads, under the alias the join declares.
     */
    public static function of(string $alias, Document $collection): self
    {
        $definitions = $collection instanceof Collection
            ? $collection->attributes()
            : Collection::fromArray($collection->getArrayCopy())->attributes();

        $attributes = [];
        $numeric = [];
        $encrypted = [];
        $columns = [];
        $schema = [];
        foreach ($definitions as $definition) {
            $key = $definition->key;
            if ($key === '') {
                continue;
            }

            $columns[$key] = self::storesColumn($definition);

            if ($definition->relationship === null) {
                $attributes[$key] = true;
                $schema[$key] = ['type' => $definition->type] + $definition->toDocument()->getArrayCopy();
            }

            if (! $definition->array && $definition->isNumeric()) {
                $numeric[$key] = $definition->type;
            }

            if (\in_array(self::ENCRYPT, $definition->filters, true)) {
                $encrypted[$key] = true;
            }
        }

        return new self(
            $alias,
            $attributes,
            $numeric,
            $encrypted,
            $columns,
            $collection->getId(),
            $schema,
        );
    }

    /**
     * Whether the collection's table holds a column for the attribute: every attribute but a
     * relationship does, and a relationship does on the side that stores the related document's id.
     */
    public function holdsColumn(string $attribute): bool
    {
        return $this->columns[$attribute] ?? false;
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
            if ($attribute instanceof Document) {
                $key = $attribute->getAttribute('key', $attribute->getId());
                if (! \is_string($key) || $key === '') {
                    continue;
                }

                $columns[$key] = ! Attribute::isRelationship($attribute) || self::storesColumn(Attribute::fromDocument($attribute));

                continue;
            }

            if ($attribute->key !== '') {
                $columns[$attribute->key] = self::storesColumn($attribute);
            }
        }

        return $columns;
    }

    private static function storesColumn(Attribute $attribute): bool
    {
        $relationship = $attribute->relationship;
        if ($relationship === null) {
            return true;
        }

        $parent = $attribute->side === RelationshipSide::Parent;

        return match ($relationship->type) {
            RelationshipType::OneToOne => $parent || $relationship->twoWay,
            RelationshipType::OneToMany => ! $parent,
            RelationshipType::ManyToOne => $parent,
            RelationshipType::ManyToMany => false,
        };
    }
}

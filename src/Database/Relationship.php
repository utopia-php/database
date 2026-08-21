<?php

namespace Utopia\Database;

use Utopia\Query\Schema\ForeignKeyAction;

/**
 * Represents a relationship between two database collections, including its type, direction, and delete behavior.
 */
class Relationship
{
    public function __construct(
        public string $collection,
        public string $relatedCollection,
        public RelationType $type,
        public bool $twoWay = false,
        public string $key = '',
        public string $twoWayKey = '',
        public ForeignKeyAction $onDelete = ForeignKeyAction::Restrict,
        public RelationSide $side = RelationSide::Parent,
    ) {
    }

    public static function oneToOne(
        string $collection,
        string $relatedCollection,
        bool $twoWay = false,
        string $key = '',
        string $twoWayKey = '',
        ForeignKeyAction $onDelete = ForeignKeyAction::Restrict,
        RelationSide $side = RelationSide::Parent,
    ): self {
        return new self(
            collection: $collection,
            relatedCollection: $relatedCollection,
            type: RelationType::OneToOne,
            twoWay: $twoWay,
            key: $key,
            twoWayKey: $twoWayKey,
            onDelete: $onDelete,
            side: $side,
        );
    }

    public static function oneToMany(
        string $collection,
        string $relatedCollection,
        bool $twoWay = false,
        string $key = '',
        string $twoWayKey = '',
        ForeignKeyAction $onDelete = ForeignKeyAction::Restrict,
        RelationSide $side = RelationSide::Parent,
    ): self {
        return new self(
            collection: $collection,
            relatedCollection: $relatedCollection,
            type: RelationType::OneToMany,
            twoWay: $twoWay,
            key: $key,
            twoWayKey: $twoWayKey,
            onDelete: $onDelete,
            side: $side,
        );
    }

    public static function manyToOne(
        string $collection,
        string $relatedCollection,
        bool $twoWay = false,
        string $key = '',
        string $twoWayKey = '',
        ForeignKeyAction $onDelete = ForeignKeyAction::Restrict,
        RelationSide $side = RelationSide::Parent,
    ): self {
        return new self(
            collection: $collection,
            relatedCollection: $relatedCollection,
            type: RelationType::ManyToOne,
            twoWay: $twoWay,
            key: $key,
            twoWayKey: $twoWayKey,
            onDelete: $onDelete,
            side: $side,
        );
    }

    public static function manyToMany(
        string $collection,
        string $relatedCollection,
        bool $twoWay = false,
        string $key = '',
        string $twoWayKey = '',
        ForeignKeyAction $onDelete = ForeignKeyAction::Restrict,
        RelationSide $side = RelationSide::Parent,
    ): self {
        return new self(
            collection: $collection,
            relatedCollection: $relatedCollection,
            type: RelationType::ManyToMany,
            twoWay: $twoWay,
            key: $key,
            twoWayKey: $twoWayKey,
            onDelete: $onDelete,
            side: $side,
        );
    }

    /**
     * Convert this relationship to a Document representation.
     *
     * @return Document
     */
    public function toDocument(): Document
    {
        return new Document([
            'relatedCollection' => $this->relatedCollection,
            'relationType' => $this->type->value,
            'twoWay' => $this->twoWay,
            'twoWayKey' => $this->twoWayKey,
            'onDelete' => $this->onDelete->value,
            'side' => $this->side->value,
        ]);
    }

    /**
     * Create a Relationship instance from a collection ID and attribute Document.
     *
     * @param string $collection The parent collection ID
     * @param Attribute|Document $attribute The relationship attribute
     * @return self
     */
    public static function fromDocument(string $collection, Attribute|Document $attribute): self
    {
        $options = $attribute->getAttribute('options', []);

        if ($options instanceof Document) {
            $options = $options->getArrayCopy();
        }

        if (!\is_array($options)) {
            $options = [];
        }

        /** @var string $relatedCollection */
        $relatedCollection = $options['relatedCollection'] ?? '';
        /** @var RelationType|string $relationType */
        $relationType = $options['relationType'] ?? 'oneToOne';
        /** @var bool $twoWay */
        $twoWay = $options['twoWay'] ?? false;
        /** @var string $key */
        $key = $attribute->getAttribute('key', $attribute->getId());
        /** @var string $twoWayKey */
        $twoWayKey = $options['twoWayKey'] ?? '';
        /** @var ForeignKeyAction|string $onDelete */
        $onDelete = $options['onDelete'] ?? ForeignKeyAction::Restrict;
        /** @var RelationSide|string $side */
        $side = $options['side'] ?? RelationSide::Parent;

        return self::make(
            collection: $collection,
            relatedCollection: $relatedCollection,
            type: $relationType instanceof RelationType ? $relationType : RelationType::from($relationType),
            twoWay: $twoWay,
            key: $key,
            twoWayKey: $twoWayKey,
            onDelete: $onDelete instanceof ForeignKeyAction ? $onDelete : ForeignKeyAction::from($onDelete),
            side: $side instanceof RelationSide ? $side : RelationSide::from($side),
        );
    }

    private static function make(
        string $collection,
        string $relatedCollection,
        RelationType $type,
        bool $twoWay,
        string $key,
        string $twoWayKey,
        ForeignKeyAction $onDelete,
        RelationSide $side,
    ): self {
        return match ($type) {
            RelationType::OneToOne => self::oneToOne(
                collection: $collection,
                relatedCollection: $relatedCollection,
                twoWay: $twoWay,
                key: $key,
                twoWayKey: $twoWayKey,
                onDelete: $onDelete,
                side: $side,
            ),
            RelationType::OneToMany => self::oneToMany(
                collection: $collection,
                relatedCollection: $relatedCollection,
                twoWay: $twoWay,
                key: $key,
                twoWayKey: $twoWayKey,
                onDelete: $onDelete,
                side: $side,
            ),
            RelationType::ManyToOne => self::manyToOne(
                collection: $collection,
                relatedCollection: $relatedCollection,
                twoWay: $twoWay,
                key: $key,
                twoWayKey: $twoWayKey,
                onDelete: $onDelete,
                side: $side,
            ),
            RelationType::ManyToMany => self::manyToMany(
                collection: $collection,
                relatedCollection: $relatedCollection,
                twoWay: $twoWay,
                key: $key,
                twoWayKey: $twoWayKey,
                onDelete: $onDelete,
                side: $side,
            ),
        };
    }
}

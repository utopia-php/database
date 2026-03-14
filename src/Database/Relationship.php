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
     * @param Document $attribute The attribute document containing relationship options
     * @return self
     */
    public static function fromDocument(string $collection, Document $attribute): self
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

        return new self(
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
}

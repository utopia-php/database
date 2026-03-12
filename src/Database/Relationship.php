<?php

namespace Utopia\Database;

use Utopia\Query\Schema\ForeignKeyAction;

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
    ) {}

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

    public static function fromDocument(string $collection, Document $attribute): self
    {
        $options = $attribute->getAttribute('options', []);

        if ($options instanceof Document) {
            $options = $options->getArrayCopy();
        }

        return new self(
            collection: $collection,
            relatedCollection: $options['relatedCollection'] ?? '',
            type: RelationType::from($options['relationType'] ?? 'oneToOne'),
            twoWay: $options['twoWay'] ?? false,
            key: $attribute->getAttribute('key', $attribute->getId()),
            twoWayKey: $options['twoWayKey'] ?? '',
            onDelete: ForeignKeyAction::from($options['onDelete'] ?? ForeignKeyAction::Restrict->value),
            side: RelationSide::from($options['side'] ?? RelationSide::Parent->value),
        );
    }
}

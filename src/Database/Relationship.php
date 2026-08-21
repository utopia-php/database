<?php

namespace Utopia\Database;

use Utopia\Query\Schema\ForeignKeyAction;

/**
 * Represents a relationship between two database collections, including its type, direction, and delete behavior.
 *
 * @property string $collection
 * @property string $relatedCollection
 * @property RelationType $type
 * @property bool $twoWay
 * @property string $key
 * @property string $twoWayKey
 * @property ForeignKeyAction $onDelete
 * @property RelationSide $side
 */
class Relationship extends Document
{
    public function __construct(
        string $collection,
        string $relatedCollection,
        RelationType $type,
        bool $twoWay = false,
        string $key = '',
        string $twoWayKey = '',
        ForeignKeyAction $onDelete = ForeignKeyAction::Restrict,
        RelationSide $side = RelationSide::Parent,
    ) {
        parent::__construct([
            self::ID => $key,
            'key' => $key,
            'collection' => $collection,
            'relatedCollection' => $relatedCollection,
            'relationType' => $type->value,
            'twoWay' => $twoWay,
            'twoWayKey' => $twoWayKey,
            'onDelete' => $onDelete->value,
            'side' => $side->value,
        ]);
    }

    public function __get(string $name): mixed
    {
        switch ($name) {
            case 'collection':
                /** @var string $collection */
                $collection = $this->getAttribute('collection', '');

                return $collection;
            case 'relatedCollection':
                /** @var string $relatedCollection */
                $relatedCollection = $this->getAttribute('relatedCollection', '');

                return $relatedCollection;
            case 'type':
                $type = $this->getAttribute('relationType', RelationType::OneToOne->value);
                if ($type instanceof RelationType) {
                    return $type;
                }

                return RelationType::from(\is_string($type) ? $type : RelationType::OneToOne->value);
            case 'twoWay':
                return (bool) $this->getAttribute('twoWay', false);
            case 'key':
                /** @var string $key */
                $key = $this->getAttribute('key', $this->getId());

                return $key;
            case 'twoWayKey':
                /** @var string $twoWayKey */
                $twoWayKey = $this->getAttribute('twoWayKey', '');

                return $twoWayKey;
            case 'onDelete':
                $onDelete = $this->getAttribute('onDelete', ForeignKeyAction::Restrict->value);
                if ($onDelete instanceof ForeignKeyAction) {
                    return $onDelete;
                }

                return ForeignKeyAction::from(\is_string($onDelete) ? $onDelete : ForeignKeyAction::Restrict->value);
            case 'side':
                $side = $this->getAttribute('side', RelationSide::Parent->value);
                if ($side instanceof RelationSide) {
                    return $side;
                }

                return RelationSide::from(\is_string($side) ? $side : RelationSide::Parent->value);
            default:
                return $this->getAttribute($name);
        }
    }

    public function __set(string $name, mixed $value): void
    {
        match ($name) {
            'collection' => $this->setAttribute('collection', $value),
            'relatedCollection' => $this->setAttribute('relatedCollection', $value),
            'type' => $this->setAttribute('relationType', $value instanceof RelationType ? $value->value : $value),
            'twoWay' => $this->setAttribute('twoWay', $value),
            'key' => $this->setAttribute('key', $value)->setAttribute(self::ID, $value),
            'twoWayKey' => $this->setAttribute('twoWayKey', $value),
            'onDelete' => $this->setAttribute('onDelete', $value instanceof ForeignKeyAction ? $value->value : $value),
            'side' => $this->setAttribute('side', $value instanceof RelationSide ? $value->value : $value),
            default => $this->setAttribute($name, $value),
        };
    }

    public function __isset(string $name): bool
    {
        return match ($name) {
            'collection', 'relatedCollection', 'type', 'twoWay', 'key', 'twoWayKey', 'onDelete', 'side' => true,
            default => $this->offsetExists($name),
        };
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
     * @param  array<string, mixed>  $data
     */
    public static function fromArray(array $data): self
    {
        $options = $data['options'] ?? [];
        if ($options instanceof Document) {
            $options = $options->getArrayCopy();
        }
        if (! \is_array($options)) {
            $options = [];
        }

        /** @var string $key */
        $key = $data[self::ID] ?? $data['key'] ?? '';
        /** @var string $collection */
        $collection = $data['collection'] ?? '';
        /** @var string $relatedCollection */
        $relatedCollection = $data['relatedCollection'] ?? $options['relatedCollection'] ?? '';
        /** @var RelationType|string $type */
        $type = $data['relationType'] ?? $options['relationType'] ?? RelationType::OneToOne->value;
        /** @var bool $twoWay */
        $twoWay = $data['twoWay'] ?? $options['twoWay'] ?? false;
        /** @var string $twoWayKey */
        $twoWayKey = $data['twoWayKey'] ?? $options['twoWayKey'] ?? '';
        /** @var ForeignKeyAction|string $onDelete */
        $onDelete = $data['onDelete'] ?? $options['onDelete'] ?? ForeignKeyAction::Restrict;
        /** @var RelationSide|string $side */
        $side = $data['side'] ?? $options['side'] ?? RelationSide::Parent;

        $relationship = self::make(
            collection: $collection,
            relatedCollection: $relatedCollection,
            type: $type instanceof RelationType ? $type : RelationType::from((string) $type),
            twoWay: (bool) $twoWay,
            key: $key,
            twoWayKey: $twoWayKey,
            onDelete: $onDelete instanceof ForeignKeyAction ? $onDelete : ForeignKeyAction::from((string) $onDelete),
            side: $side instanceof RelationSide ? $side : RelationSide::from((string) $side),
        );

        foreach ($data as $name => $value) {
            if (\in_array($name, [
                self::ID,
                'key',
                'collection',
                'relatedCollection',
                'relationType',
                'twoWay',
                'twoWayKey',
                'onDelete',
                'side',
                'options',
            ], true)) {
                continue;
            }
            $relationship->setAttribute($name, $value);
        }

        return $relationship;
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
        $data = $attribute->getArrayCopy();
        $data['collection'] = $collection;

        return self::fromArray($data);
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

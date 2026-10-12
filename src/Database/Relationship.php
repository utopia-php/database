<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Query\Schema\ForeignKeyAction;

final readonly class Relationship
{
    public const string SIDE = 'side';

    private const string RELATED_COLLECTION = 'relatedCollection';

    private const string RELATION_TYPE = 'relationType';

    private const string TWO_WAY = 'twoWay';

    private const string KEY = 'key';

    private const string TWO_WAY_KEY = 'twoWayKey';

    private const string ON_DELETE = 'onDelete';

    private function __construct(
        public string $relatedCollection,
        public RelationshipType $type,
        public bool $twoWay,
        public ?string $key,
        public ?string $twoWayKey,
        public RelationshipDeleteAction $onDelete,
    ) {
    }

    public static function oneToOne(
        string $relatedCollection,
        ?string $key = null,
        bool $twoWay = false,
        ?string $twoWayKey = null,
        RelationshipDeleteAction $onDelete = RelationshipDeleteAction::Restrict,
    ): self {
        return new self($relatedCollection, RelationshipType::OneToOne, $twoWay, $key, $twoWayKey, $onDelete);
    }

    public static function oneToMany(
        string $relatedCollection,
        ?string $key = null,
        bool $twoWay = false,
        ?string $twoWayKey = null,
        RelationshipDeleteAction $onDelete = RelationshipDeleteAction::Restrict,
    ): self {
        return new self($relatedCollection, RelationshipType::OneToMany, $twoWay, $key, $twoWayKey, $onDelete);
    }

    public static function manyToOne(
        string $relatedCollection,
        ?string $key = null,
        bool $twoWay = false,
        ?string $twoWayKey = null,
        RelationshipDeleteAction $onDelete = RelationshipDeleteAction::Restrict,
    ): self {
        return new self($relatedCollection, RelationshipType::ManyToOne, $twoWay, $key, $twoWayKey, $onDelete);
    }

    public static function manyToMany(
        string $relatedCollection,
        ?string $key = null,
        bool $twoWay = false,
        ?string $twoWayKey = null,
        RelationshipDeleteAction $onDelete = RelationshipDeleteAction::Restrict,
    ): self {
        return new self($relatedCollection, RelationshipType::ManyToMany, $twoWay, $key, $twoWayKey, $onDelete);
    }

    public function apply(RelationshipUpdate $update): self
    {
        return new self(
            $this->relatedCollection,
            $this->type,
            $update->twoWay ?? $this->twoWay,
            $update->key ?? $this->key,
            $update->twoWayKey ?? $this->twoWayKey,
            $update->onDelete ?? $this->onDelete,
        );
    }

    public function inverse(string $collection): self
    {
        return new self($collection, $this->type, $this->twoWay, $this->twoWayKey, $this->key, $this->onDelete);
    }

    /**
     * @throws RelationshipException
     */
    public static function fromDocument(Document $document): self
    {
        return self::hydrate(
            $document->getAttribute(self::RELATED_COLLECTION),
            $document->getAttribute(self::RELATION_TYPE),
            $document->getAttribute(self::TWO_WAY),
            $document->getAttribute(self::KEY),
            $document->getAttribute(self::TWO_WAY_KEY),
            $document->getAttribute(self::ON_DELETE),
        );
    }

    /**
     * @param  array<string, mixed>  $data
     *
     * @throws RelationshipException
     */
    public static function fromArray(array $data): self
    {
        return self::hydrate(
            $data[self::RELATED_COLLECTION] ?? null,
            $data[self::RELATION_TYPE] ?? null,
            $data[self::TWO_WAY] ?? null,
            $data[self::KEY] ?? null,
            $data[self::TWO_WAY_KEY] ?? null,
            $data[self::ON_DELETE] ?? null,
        );
    }

    public function toDocument(): Document
    {
        return new Document($this->fields());
    }

    /**
     * @return array<string, mixed>
     */
    public function toOptions(RelationshipSide $side): array
    {
        $options = $this->fields();
        unset($options[self::KEY]);
        $options[self::SIDE] = $side->value;

        return $options;
    }

    /**
     * @return array<string, mixed>
     */
    private function fields(): array
    {
        return [
            self::RELATED_COLLECTION => $this->relatedCollection,
            self::RELATION_TYPE => $this->type->value,
            self::TWO_WAY => $this->twoWay,
            self::KEY => $this->key,
            self::TWO_WAY_KEY => $this->twoWayKey,
            self::ON_DELETE => $this->onDelete->value,
        ];
    }

    /**
     * @throws RelationshipException
     */
    private static function hydrate(
        mixed $relatedCollection,
        mixed $type,
        mixed $twoWay,
        mixed $key,
        mixed $twoWayKey,
        mixed $onDelete,
    ): self {
        if (! \is_string($relatedCollection) || $relatedCollection === '') {
            throw new RelationshipException('Relationship has no related collection');
        }

        return new self(
            $relatedCollection,
            self::hydrateType($type),
            (bool) $twoWay,
            self::hydrateKey(self::KEY, $key),
            self::hydrateKey(self::TWO_WAY_KEY, $twoWayKey),
            self::hydrateOnDelete($onDelete),
        );
    }

    /**
     * @throws RelationshipException
     */
    private static function hydrateType(mixed $type): RelationshipType
    {
        if ($type instanceof RelationshipType) {
            return $type;
        }

        return (\is_string($type) ? RelationshipType::tryFrom($type) : null)
            ?? throw new RelationshipException('Unknown relationship type "'.self::describe($type).'"');
    }

    /**
     * @throws RelationshipException
     */
    private static function hydrateKey(string $name, mixed $key): ?string
    {
        if ($key === null || $key === '') {
            return null;
        }

        if (! \is_string($key)) {
            throw new RelationshipException('Relationship '.$name.' must be a string, got '.\get_debug_type($key));
        }

        return $key;
    }

    /**
     * @throws RelationshipException
     */
    private static function hydrateOnDelete(mixed $onDelete): RelationshipDeleteAction
    {
        if ($onDelete === null) {
            return RelationshipDeleteAction::Restrict;
        }

        if ($onDelete instanceof RelationshipDeleteAction) {
            return $onDelete;
        }

        if ($onDelete instanceof ForeignKeyAction) {
            $onDelete = $onDelete->value;
        }

        return (\is_string($onDelete) ? RelationshipDeleteAction::tryFrom($onDelete) : null)
            ?? throw new RelationshipException(
                'Unsupported relationship onDelete action "'.self::describe($onDelete).'"; expected one of: '
                .\implode(', ', \array_column(RelationshipDeleteAction::cases(), 'value'))
            );
    }

    private static function describe(mixed $value): string
    {
        return \is_string($value) ? $value : \get_debug_type($value);
    }
}

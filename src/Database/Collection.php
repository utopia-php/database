<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Structure as StructureException;

class Collection extends Document
{
    public const string NAME = 'name';

    public const string ATTRIBUTES = 'attributes';

    public const string INDEXES = 'indexes';

    public const string DOCUMENT_SECURITY = 'documentSecurity';

    private const array CORE_KEYS = [self::ATTRIBUTES, self::INDEXES, self::DOCUMENT_SECURITY];

    /** @var list<Attribute>|null */
    private ?array $attributeModels = null;

    private mixed $attributeSource = null;

    /** @var list<Attribute>|null */
    private ?array $attributesWithInternal = null;

    /** @var list<Attribute>|null */
    private ?array $internalSource = null;

    /** @var list<Index>|null */
    private ?array $indexModels = null;

    private mixed $indexSource = null;

    private ?string $fingerprint = null;

    private mixed $fingerprintPermissions = null;

    private mixed $fingerprintDocumentSecurity = null;

    /**
     * @param  array<string, mixed>  $input
     */
    private function __construct(array $input = [])
    {
        parent::__construct($input);
    }

    /**
     * @param  list<Attribute>  $attributes
     * @param  list<Index>  $indexes
     * @param  list<string>|null  $permissions  null grants the default create-any permission on creation; [] grants none
     * @param  array<string, mixed>  $metadata  only keys the metadata collection stores; createCollection() refuses
     *                                          any other key with Exception\Structure
     *
     * @throws StructureException
     */
    public static function create(
        string $id,
        string $name = '',
        array $attributes = [],
        array $indexes = [],
        ?array $permissions = null,
        bool $documentSecurity = true,
        array $metadata = [],
    ): self {
        $reserved = \array_values(\array_intersect(self::CORE_KEYS, \array_keys($metadata)));
        if ($reserved !== []) {
            throw new StructureException('Collection metadata must not set '.\implode(', ', $reserved).'; pass them as arguments');
        }

        $data = [
            self::ID => $id,
            self::NAME => $name !== '' ? $name : $id,
            self::ATTRIBUTES => \array_map(static fn (Attribute $attribute): Document => $attribute->toDocument(), $attributes),
            self::INDEXES => \array_map(static fn (Index $index): Document => $index->toDocument(), $indexes),
            self::DOCUMENT_SECURITY => $documentSecurity,
        ];

        if ($permissions !== null) {
            $data[self::PERMISSIONS] = $permissions;
        }

        return new self(\array_merge($data, $metadata));
    }

    /**
     * @param  array<string, mixed>  $data
     *
     * @throws StructureException
     * @throws IndexException
     */
    #[\Override]
    public static function fromArray(array $data): self
    {
        $id = $data[self::ID] ?? '';
        $data[self::ID] = $id = \is_string($id) ? $id : '';

        $name = $data[self::NAME] ?? '';
        $data[self::NAME] = \is_string($name) && $name !== '' ? $name : $id;

        if (\array_key_exists(self::PERMISSIONS, $data) && ! \is_array($data[self::PERMISSIONS])) {
            unset($data[self::PERMISSIONS]);
        }

        $data[self::DOCUMENT_SECURITY] = (bool) ($data[self::DOCUMENT_SECURITY] ?? true);
        $data[self::ATTRIBUTES] = self::attributeDocuments($data[self::ATTRIBUTES] ?? []);
        $data[self::INDEXES] = self::indexDocuments($data[self::INDEXES] ?? []);

        return new self($data);
    }

    /**
     * The collection a stored definition describes; a definition that already is one is returned as it is, so its
     * hydrated attributes and indexes are kept.
     *
     * @throws StructureException
     * @throws IndexException
     */
    public static function fromDocument(Document $document): self
    {
        return $document instanceof self ? $document : self::fromArray($document->getArrayCopy());
    }

    public function toDocument(): Document
    {
        $document = new Document();
        $document->exchangeArray(\iterator_to_array(clone $this));

        return $document;
    }

    /**
     * @return list<Attribute>
     *
     * @throws StructureException
     * @throws RelationshipException
     */
    public function attributes(): array
    {
        $source = $this->getAttribute(self::ATTRIBUTES);
        if ($this->attributeModels !== null && $source === $this->attributeSource) {
            return $this->attributeModels;
        }

        $stored = self::storedList($source)
            ?? throw new StructureException('Collection attributes must be a list of attribute documents');

        $models = [];
        foreach ($stored as $attribute) {
            $models[] = match (true) {
                $attribute instanceof Document => Attribute::fromDocument($attribute),
                \is_array($attribute) => Attribute::fromDocument(new Document(self::stringKeyed($attribute))),
                default => throw new StructureException('Collection attributes must be attribute documents'),
            };
        }

        $this->fingerprint = null;
        $this->attributesWithInternal = null;
        $this->attributeSource = $source;

        return $this->attributeModels = $models;
    }

    /**
     * The declared attributes followed by $internal, built at most once per schema state so per-document passes
     * over the whole schema do not rebuild the list.
     *
     * @internal
     *
     * @param  list<Attribute>  $internal
     * @return list<Attribute>
     *
     * @throws StructureException
     */
    public function attributesWith(array $internal): array
    {
        $attributes = $this->attributes();
        if ($this->attributesWithInternal !== null && $internal === $this->internalSource) {
            return $this->attributesWithInternal;
        }

        $this->internalSource = $internal;

        return $this->attributesWithInternal = [...$attributes, ...$internal];
    }

    /**
     * @return list<Index>
     *
     * @throws IndexException
     */
    public function indexes(): array
    {
        $source = $this->getAttribute(self::INDEXES);
        if ($this->indexModels !== null && $source === $this->indexSource) {
            return $this->indexModels;
        }

        $stored = self::storedList($source)
            ?? throw new IndexException('Collection indexes must be a list of index documents');

        $models = [];
        foreach ($stored as $index) {
            $models[] = match (true) {
                $index instanceof Document => Index::fromDocument($index),
                \is_array($index) => Index::fromArray(self::stringKeyed($index)),
                default => throw new IndexException('Collection indexes must be index documents'),
            };
        }

        $this->fingerprint = null;
        $this->indexSource = $source;

        return $this->indexModels = $models;
    }

    /**
     * A hash of everything a query against this collection is validated and cached by: its attributes, indexes,
     * permissions and document security. Computed at most once per schema state.
     *
     * @internal
     *
     * @throws StructureException
     * @throws IndexException
     */
    public function fingerprint(): string
    {
        $attributes = $this->attributes();
        $indexes = $this->indexes();
        $permissions = $this->getAttribute(self::PERMISSIONS);
        $documentSecurity = $this->getAttribute(self::DOCUMENT_SECURITY);

        if (
            $this->fingerprint !== null
            && $permissions === $this->fingerprintPermissions
            && $documentSecurity === $this->fingerprintDocumentSecurity
        ) {
            return $this->fingerprint;
        }

        $this->fingerprintPermissions = $permissions;
        $this->fingerprintDocumentSecurity = $documentSecurity;

        return $this->fingerprint = \hash('xxh128', \serialize([
            $attributes,
            $indexes,
            $this->getPermissions(),
            $this->documentSecurity(),
        ]));
    }

    public function name(): string
    {
        $name = $this->getAttribute(self::NAME);

        return \is_string($name) && $name !== '' ? $name : $this->getId();
    }

    public function documentSecurity(): bool
    {
        return (bool) $this->getAttribute(self::DOCUMENT_SECURITY, true);
    }

    /**
     * @return list<string>|null
     */
    public function declaredPermissions(): ?array
    {
        return $this->offsetExists(self::PERMISSIONS) ? $this->getPermissions() : null;
    }

    #[\Override]
    public function __clone()
    {
        $attributes = $this->attributeModels !== null && $this->getAttribute(self::ATTRIBUTES) === $this->attributeSource
            ? $this->attributeModels
            : null;
        $indexes = $this->indexModels !== null && $this->getAttribute(self::INDEXES) === $this->indexSource
            ? $this->indexModels
            : null;
        $fingerprint = $attributes !== null && $indexes !== null ? $this->fingerprint : null;

        parent::__clone();

        $this->forget(self::ATTRIBUTES);
        $this->forget(self::INDEXES);
        if ($attributes !== null) {
            $this->attributeModels = $attributes;
            $this->attributeSource = $this->getAttribute(self::ATTRIBUTES);
        }
        if ($indexes !== null) {
            $this->indexModels = $indexes;
            $this->indexSource = $this->getAttribute(self::INDEXES);
        }
        $this->fingerprint = $fingerprint;
    }

    #[\Override]
    public function offsetSet(mixed $key, mixed $value): void
    {
        parent::offsetSet($key, $value);
        $this->forget($key);
    }

    #[\Override]
    public function offsetUnset(mixed $key): void
    {
        parent::offsetUnset($key);
        $this->forget($key);
    }

    /**
     * @param  array<string, mixed>|object  $array
     * @return array<mixed>
     */
    #[\Override]
    public function exchangeArray(array|object $array): array
    {
        $previous = parent::exchangeArray($array);

        if (($previous[self::ATTRIBUTES] ?? null) !== $this->getAttribute(self::ATTRIBUTES)) {
            $this->forget(self::ATTRIBUTES);
        }
        if (($previous[self::INDEXES] ?? null) !== $this->getAttribute(self::INDEXES)) {
            $this->forget(self::INDEXES);
        }

        return $previous;
    }

    private function forget(mixed $key): void
    {
        if ($key === self::ATTRIBUTES) {
            $this->attributeModels = null;
            $this->attributeSource = null;
            $this->fingerprint = null;
        } elseif ($key === self::INDEXES) {
            $this->indexModels = null;
            $this->indexSource = null;
            $this->fingerprint = null;
        }
    }

    /**
     * The stored list as written, or decoded from the JSON a raw metadata row holds; null when it is neither.
     *
     * @return array<mixed>|null
     */
    private static function storedList(mixed $source): ?array
    {
        if ($source === null) {
            return [];
        }

        if (\is_string($source)) {
            $source = \json_decode($source, true);
        }

        return \is_array($source) ? $source : null;
    }

    /**
     * @return list<Document>|string
     *
     * @throws StructureException
     */
    private static function attributeDocuments(mixed $attributes): array|string
    {
        if (\is_string($attributes)) {
            return $attributes;
        }

        if (! \is_array($attributes)) {
            return [];
        }

        $documents = [];
        foreach ($attributes as $attribute) {
            $documents[] = match (true) {
                $attribute instanceof Attribute => $attribute->toDocument(),
                $attribute instanceof Document => $attribute,
                \is_array($attribute) => new Document(self::stringKeyed($attribute)),
                default => throw new StructureException('Collection attributes must be attribute documents'),
            };
        }

        return $documents;
    }

    /**
     * @return list<Document>|string
     *
     * @throws IndexException
     */
    private static function indexDocuments(mixed $indexes): array|string
    {
        if (\is_string($indexes)) {
            return $indexes;
        }

        if (! \is_array($indexes)) {
            return [];
        }

        $documents = [];
        foreach ($indexes as $index) {
            $documents[] = match (true) {
                $index instanceof Index => $index->toDocument(),
                $index instanceof Document => $index,
                \is_array($index) => new Document(self::stringKeyed($index)),
                default => throw new IndexException('Collection indexes must be index documents'),
            };
        }

        return $documents;
    }

    /**
     * @param  array<mixed>  $data
     * @return array<string, mixed>
     */
    private static function stringKeyed(array $data): array
    {
        $keyed = [];
        foreach ($data as $key => $value) {
            if (\is_string($key)) {
                $keyed[$key] = $value;
            }
        }

        return $keyed;
    }
}

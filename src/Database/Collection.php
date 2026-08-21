<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;

/**
 * Represents a database collection with its attributes, indexes, permissions, and configuration.
 */
class Collection
{
    /**
     * @var array<Attribute>
     */
    public array $attributes = [];

    /**
     * @var array<Index>
     */
    public array $indexes = [];

    /**
     * @param  array<Attribute|Document>  $attributes
     * @param  array<Index|Document>  $indexes
     * @param  array<string>  $permissions
     * @param  array<string, mixed>  $metadata
     */
    public function __construct(
        public string $id = '',
        public string $name = '',
        array $attributes = [],
        array $indexes = [],
        public array $permissions = [],
        public bool $documentSecurity = true,
        public array $metadata = [],
    ) {
        $this->attributes = array_values(array_map(
            fn (Attribute|Document $attr): Attribute => $attr instanceof Attribute ? $attr : Attribute::fromDocument($attr),
            $attributes,
        ));
        $this->indexes = array_values(array_map(
            fn (Index|Document $idx): Index => $idx instanceof Index ? $idx : Index::fromDocument($idx),
            $indexes,
        ));
    }

    /**
     * Convert this collection to a Document representation.
     *
     * @return Document
     */
    public function toDocument(): Document
    {
        return new Document([
            Document::ID => ID::custom($this->id),
            'name' => $this->name ?: $this->id,
            'attributes' => \array_map(fn (Attribute $attr) => $attr->toDocument(), $this->attributes),
            'indexes' => \array_map(fn (Index $idx) => $idx->toDocument(), $this->indexes),
            Document::PERMISSIONS => $this->permissions,
            'documentSecurity' => $this->documentSecurity,
        ]);
    }

    /**
     * Create a Collection instance from a Document.
     *
     * @param  Document  $document  The document to convert
     * @return self
     */
    public static function fromDocument(Document $document): self
    {
        /** @var string $id */
        $id = $document->getId();
        /** @var string $name */
        $name = $document->getAttribute('name', $id);
        /** @var bool $documentSecurity */
        $documentSecurity = $document->getAttribute('documentSecurity', true);
        /** @var array<string> $permissions */
        $permissions = $document->getPermissions();

        /** @var array<Document> $rawAttributes */
        $rawAttributes = $document->getAttribute('attributes', []);
        $attributes = \array_map(
            fn (Document $attr) => Attribute::fromDocument($attr),
            $rawAttributes
        );

        /** @var array<Document> $rawIndexes */
        $rawIndexes = $document->getAttribute('indexes', []);
        $indexes = \array_map(
            fn (Document $idx) => Index::fromDocument($idx),
            $rawIndexes
        );

        return new self(
            id: $id,
            name: $name,
            attributes: $attributes,
            indexes: $indexes,
            permissions: $permissions,
            documentSecurity: $documentSecurity,
        );
    }
}

<?php

namespace Utopia\Database\Mirroring;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

abstract class Filter
{
    /**
     * Called before any action is executed, when the filter is constructed.
     */
    public function init(
        Database $source,
        ?Database $destination,
    ): void {
    }

    /**
     * Called after all actions are executed, when the filter is destructed.
     */
    public function shutdown(
        Database $source,
        ?Database $destination,
    ): void {
    }

    /**
     * Called before collection is created in the destination database
     */
    public function beforeCreateCollection(
        Database $source,
        Database $destination,
        string $collectionId,
        ?Document $collection = null,
    ): ?Document {
        return $collection;
    }

    /**
     * Called before collection is updated in the destination database
     */
    public function beforeUpdateCollection(
        Database $source,
        Database $destination,
        string $collectionId,
        ?Document $collection = null,
    ): ?Document {
        return $collection;
    }

    /**
     * Called after collection is deleted in the destination database
     */
    public function beforeDeleteCollection(
        Database $source,
        Database $destination,
        string $collectionId,
    ): void {
    }

    public function beforeCreateAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
        ?Document $attribute = null,
    ): ?Document {
        return $attribute;
    }

    public function beforeUpdateAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
        ?Document $attribute = null,
    ): ?Document {
        return $attribute;
    }

    public function beforeDeleteAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
    ): void {
    }

    // Indexes

    public function beforeCreateIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
        ?Document $index = null,
    ): ?Document {
        return $index;
    }

    public function beforeUpdateIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
        ?Document $index = null,
    ): ?Document {
        return $index;
    }

    public function beforeDeleteIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
    ): void {
    }

    /**
     * Called before document is created in the destination database
     */
    public function beforeCreateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document {
        return $document;
    }

    /**
     * Called after document is created in the destination database
     */
    public function afterCreateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document {
        return $document;
    }

    /**
     * Called before document is updated in the destination database
     */
    public function beforeUpdateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document {
        return $document;
    }

    /**
     * Called after document is updated in the destination database
     */
    public function afterUpdateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document {
        return $document;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function beforeUpdateDocuments(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $updates,
        array $queries
    ): Document {
        return $updates;
    }

    /**
     * @param  array<Query>  $queries
     */
    public function afterUpdateDocuments(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $updates,
        array $queries
    ): void {
    }

    /**
     * Called before document is deleted in the destination database
     */
    public function beforeDeleteDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        string $documentId,
    ): void {
    }

    /**
     * Called after document is deleted in the destination database
     */
    public function afterDeleteDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        string $documentId,
    ): void {
    }

    /**
     * @param  array<Query>  $queries
     */
    public function beforeDeleteDocuments(
        Database $source,
        Database $destination,
        string $collectionId,
        array $queries
    ): void {
    }

    /**
     * @param  array<Query>  $queries
     */
    public function afterDeleteDocuments(
        Database $source,
        Database $destination,
        string $collectionId,
        array $queries
    ): void {
    }

    /**
     * Called before document is upserted in the destination database
     */
    public function beforeCreateOrUpdateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document {
        return $document;
    }

    /**
     * Called after document is upserted in the destination database
     */
    public function afterCreateOrUpdateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document {
        return $document;
    }
}

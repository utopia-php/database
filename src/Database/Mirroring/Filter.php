<?php

namespace Utopia\Database\Mirroring;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

/**
 * Abstract filter for intercepting and transforming mirrored database operations between source and destination.
 */
abstract class Filter
{
    /**
     * Called before any action is executed, when the filter is constructed.
     *
     * @param Database $source The source database instance
     * @param Database|null $destination The destination database instance, or null if unavailable
     * @return void
     */
    public function init(
        Database $source,
        ?Database $destination,
    ): void {
    }

    /**
     * Called after all actions are executed, when the filter is destructed.
     *
     * @param Database $source The source database instance
     * @param Database|null $destination The destination database instance, or null if unavailable
     * @return void
     */
    public function shutdown(
        Database $source,
        ?Database $destination,
    ): void {
    }

    /**
     * Called before a collection is created in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document|null $collection The collection document, or null to skip creation
     * @return Document|null The possibly transformed collection document, or null to skip
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
     * Called before a collection is updated in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document|null $collection The collection document, or null to skip update
     * @return Document|null The possibly transformed collection document, or null to skip
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
     * Called before a collection is deleted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @return void
     */
    public function beforeDeleteCollection(
        Database $source,
        Database $destination,
        string $collectionId,
    ): void {
    }

    /**
     * Called before an attribute is created in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param string $attributeId The attribute identifier
     * @param Document|null $attribute The attribute document, or null to skip creation
     * @return Document|null The possibly transformed attribute document, or null to skip
     */
    public function beforeCreateAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
        ?Document $attribute = null,
    ): ?Document {
        return $attribute;
    }

    /**
     * Called before an attribute is updated in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param string $attributeId The attribute identifier
     * @param Document|null $attribute The attribute document, or null to skip update
     * @return Document|null The possibly transformed attribute document, or null to skip
     */
    public function beforeUpdateAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
        ?Document $attribute = null,
    ): ?Document {
        return $attribute;
    }

    /**
     * Called before an attribute is deleted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param string $attributeId The attribute identifier
     * @return void
     */
    public function beforeDeleteAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
    ): void {
    }

    /**
     * Called before an index is created in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param string $indexId The index identifier
     * @param Document|null $index The index document, or null to skip creation
     * @return Document|null The possibly transformed index document, or null to skip
     */
    public function beforeCreateIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
        ?Document $index = null,
    ): ?Document {
        return $index;
    }

    /**
     * Called before an index is updated in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param string $indexId The index identifier
     * @param Document|null $index The index document, or null to skip update
     * @return Document|null The possibly transformed index document, or null to skip
     */
    public function beforeUpdateIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
        ?Document $index = null,
    ): ?Document {
        return $index;
    }

    /**
     * Called before an index is deleted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param string $indexId The index identifier
     * @return void
     */
    public function beforeDeleteIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
    ): void {
    }

    /**
     * Called before a document is created in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document $document The document to create
     * @return Document The possibly transformed document
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
     * Called after a document is created in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document $document The created document
     * @return Document The possibly transformed document
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
     * Called before a document is updated in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document $document The document to update
     * @return Document The possibly transformed document
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
     * Called after a document is updated in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document $document The updated document
     * @return Document The possibly transformed document
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
     * Called before documents are bulk-updated in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document $updates The document containing the update fields
     * @param array<Query> $queries The queries filtering which documents to update
     * @return Document The possibly transformed updates document
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
     * Called after documents are bulk-updated in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document $updates The document containing the update fields
     * @param array<Query> $queries The queries filtering which documents were updated
     * @return void
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
     * Called before a document is deleted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param string $documentId The document identifier
     * @return void
     */
    public function beforeDeleteDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        string $documentId,
    ): void {
    }

    /**
     * Called after a document is deleted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param string $documentId The document identifier
     * @return void
     */
    public function afterDeleteDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        string $documentId,
    ): void {
    }

    /**
     * Called before documents are bulk-deleted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param array<Query> $queries The queries filtering which documents to delete
     * @return void
     */
    public function beforeDeleteDocuments(
        Database $source,
        Database $destination,
        string $collectionId,
        array $queries
    ): void {
    }

    /**
     * Called after documents are bulk-deleted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param array<Query> $queries The queries filtering which documents were deleted
     * @return void
     */
    public function afterDeleteDocuments(
        Database $source,
        Database $destination,
        string $collectionId,
        array $queries
    ): void {
    }

    /**
     * Called before a document is upserted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document $document The document to upsert
     * @return Document The possibly transformed document
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
     * Called after a document is upserted in the destination database.
     *
     * @param Database $source The source database instance
     * @param Database $destination The destination database instance
     * @param string $collectionId The collection identifier
     * @param Document $document The upserted document
     * @return Document The possibly transformed document
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

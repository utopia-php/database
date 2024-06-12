<?php

namespace Utopia\Database\Mirroring;

use Utopia\Database\Database;
use Utopia\Database\Document;

abstract class Filter
{
    /**
     * Called before collection is created in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param Document $collection
     * @return Document
     */
    public function onCreateCollection(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $collection,
    ): Document {
        return $collection;
    }

    /**
     * Called before collection is updated in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param Document $collection
     * @return Document
     */
    public function onUpdateCollection(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $collection,
    ): Document {
        return $collection;
    }

    /**
     * Called after collection is deleted in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @return void
     */
    public function onDeleteCollection(
        Database $source,
        Database $destination,
        string $collectionId,
    ): void {
        return;
    }

    /**
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param string $attributeId
     * @param Document $attribute
     * @return Document
     */
    public function onCreateAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
        Document $attribute,
    ): Document {
        return $attribute;
    }

    /**
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param string $attributeId
     * @param Document $attribute
     * @return Document
     */
    public function onUpdateAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
        Document $attribute,
    ): Document {
        return $attribute;
    }

    /**
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param string $attributeId
     * @return void
     */
    public function onDeleteAttribute(
        Database $source,
        Database $destination,
        string $collectionId,
        string $attributeId,
    ): void {
        return;
    }

    // Indexes

    /**
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param string $indexId
     * @param Document $index
     * @return Document
     */
    public function onCreateIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
        Document $index,
    ): Document {
        return $index;
    }

    /**
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param string $indexId
     * @param Document $index
     * @return Document
     */
    public function onUpdateIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
        Document $index,
    ): Document {
        return $index;
    }

    /**
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param string $indexId
     * @return void
     */
    public function onDeleteIndex(
        Database $source,
        Database $destination,
        string $collectionId,
        string $indexId,
    ): void {
        return;
    }

    /**
     * Called before document is created in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param Document $document
     * @return Document
     */
    public function onCreateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document {
        return $document;
    }


    /**
     * Called before document is updated in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param Document $document
     * @return Document
     */
    public function onUpdateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document {
        return $document;
    }

    /**
     * Called after document is deleted in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param string $documentId
     * @return void
     */
    public function onDeleteDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        string $documentId,
    ): void {
        return;
    }
}

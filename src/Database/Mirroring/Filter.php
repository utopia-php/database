<?php

namespace Utopia\Database\Mirroring;

use Utopia\Database\Database;
use Utopia\Database\Document;

abstract class Filter
{
    /**
     * Called before document is created in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param Document $document
     * @return Document
     */
    abstract public function onCreateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document;


    /**
     * Called before document is updated in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param Document $document
     * @return Document
     */
    abstract public function onUpdateDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        Document $document,
    ): Document;

    /**
     * Called after document is deleted in the destination database
     *
     * @param Database $source
     * @param Database $destination
     * @param string $collectionId
     * @param string $documentId
     * @return void
     */
    abstract public function onDeleteDocument(
        Database $source,
        Database $destination,
        string $collectionId,
        string $documentId,
    ): void;
}

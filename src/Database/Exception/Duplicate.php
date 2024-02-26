<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

class Duplicate extends Exception
{
    protected ?string $collectionId;
    protected ?string $documentId;
    protected ?string $relatedCollectionId;
    protected ?string $relatedDocumentId;

    public function __construct(string $message, mixed $code = 0, \Throwable $previous = null, ?string $collectionId = null, ?string $documentId = null, ?string $relatedCollectionId = null, ?string $relatedDocumentId = null)
    {
        parent::__construct($message, $code, $previous);

        $this->collectionId = $collectionId;
        $this->documentId = $documentId;
        $this->relatedCollectionId = $relatedCollectionId;
        $this->relatedDocumentId = $relatedDocumentId;
    }

    /**
     * @return string|null
     */
    public function getCollectionId(): ?string
    {
        return $this->collectionId;
    }

    /**
     * @return string|null
     */
    public function getDocumentId(): ?string
    {
        return $this->documentId;
    }

    /**
     * @return string|null
     */
    public function getRelatedCollectionId(): ?string
    {
        return $this->relatedCollectionId;
    }

    /**
     * @return string|null
     */
    public function getRelatedDocumentId(): ?string
    {
        return $this->relatedDocumentId;
    }
}

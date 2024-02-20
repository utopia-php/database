<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

class Duplicate extends Exception
{
    protected ?string $collectionId;
    protected ?string $documentId;

    public function __construct($message = '', $code = 0 , $previous = null, $collectionId = null, $documentId = null)
    {
        parent::__construct($message, $code, $previous);
        $this->collectionId = $collectionId;
        $this->documentId = $documentId;
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
}

<?php

namespace Tests\Unit\Collections;

use Utopia\Database\Database;
use Utopia\Database\Document;

final class MetadataWriteRecorder extends Database
{
    /**
     * @var list<bool>
     */
    private array $validations = [];

    /**
     * @return list<bool>
     */
    public function getValidations(): array
    {
        return $this->validations;
    }

    #[\Override]
    public function updateDocument(string $collection, string $id, Document $document): Document
    {
        if ($collection === self::METADATA) {
            $this->validations[] = $this->isValidationEnabled();
        }

        return parent::updateDocument($collection, $id, $document);
    }
}

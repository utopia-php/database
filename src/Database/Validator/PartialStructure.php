<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Document;

/**
 * Validates partial document structures, only requiring attributes that are both marked required and present in the document.
 */
class PartialStructure extends Structure
{
    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param  mixed  $document
     */
    public function isValid($document): bool
    {
        if (! $document instanceof Document) {
            $this->message = 'Value must be an instance of Document';

            return false;
        }

        if (empty($this->collection->getId()) || $this->collection->getCollection() !== Database::METADATA) {
            $this->message = 'Collection not found';

            return false;
        }

        $keys = [];
        $structure = $document->getArrayCopy();
        /** @var array<string, mixed> $collectionAttributes */
        $collectionAttributes = $this->collection->getAttribute('attributes', []);
        /** @var array<string, mixed> $attributes */
        $attributes = \array_merge($this->attributes, $collectionAttributes);

        foreach ($attributes as $attribute) {
            /** @var array<string, mixed> $attribute */
            /** @var string $name */
            $name = $attribute['$id'] ?? '';
            $keys[$name] = $attribute;
        }
        $requiredAttributes = [];
        foreach ($this->attributes as $attribute) {
            /** @var array<string, mixed> $attribute */
            /** @var string $attrId */
            $attrId = $attribute['$id'] ?? '';
            if ($attribute['required'] === true && $document->offsetExists($attrId)) {
                $requiredAttributes[] = $attribute;
            }
        }

        if (! $this->checkForAllRequiredValues($structure, $requiredAttributes, $keys)) {
            return false;
        }
        if (! $this->checkForUnknownAttributes($structure, $keys)) {
            return false;
        }

        if (! $this->checkForInvalidAttributeValues($structure, $keys)) {
            return false;
        }

        return true;
    }
}

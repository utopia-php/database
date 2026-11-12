<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Document;

class PartialStructure extends Structure
{
    #[\Override]
    public function isValid(mixed $value): bool
    {
        if (! $value instanceof Document) {
            $this->message = 'Value must be an instance of Document';

            return false;
        }

        if (empty($this->collection->getId()) || $this->collection->getCollection() !== Database::METADATA) {
            $this->message = 'Collection not found';

            return false;
        }

        $structure = $value->getArrayCopy();
        $definitions = $this->definitions();

        $required = [];
        foreach (self::internalAttributes() as $attribute) {
            if ($attribute->required && $value->offsetExists($attribute->key)) {
                $required[] = $attribute;
            }
        }

        if (! $this->checkForAllRequiredValues($structure, $required)) {
            return false;
        }
        if (! $this->checkForUnknownAttributes($structure, $definitions)) {
            return false;
        }

        if (! $this->checkForInvalidAttributeValues($value, $structure, $definitions)) {
            return false;
        }

        return true;
    }
}

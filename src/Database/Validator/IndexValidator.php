<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Validator;
use Utopia\Database\Document;

class IndexValidator extends Validator
{
    protected string $message = 'Invalid Index';
    protected Document $collection;

    public function __construct(Document $collection)
    {
        $this->collection = $collection;
    }

    /**
     * Returns validator description
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is valid.
     *
     * Returns true index if valid.
     * @param mixed $value as index options
     * @return bool
     */
    public function isValid($value): bool
    {
        $indexType = $value['type'];
        $indexAttributes = $value['attributes'];

        if (empty($indexAttributes)) {
            $this->message = 'Missing attributes';
            return false;
        }

        if($indexType === Database::INDEX_FULLTEXT){
            $collectionAttributes = $this->collection->getAttributes()['attributes'];
            foreach ($collectionAttributes as $attr) {
                foreach ($indexAttributes as $ia) {
                    if($ia === $attr['key']){
                        if($attr['type'] !== Database::VAR_STRING){
                            $this->message = 'Attribute "' . $attr['key'] . '" cannot be part of a FULLTEXT index';
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }
    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }
}

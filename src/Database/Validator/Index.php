<?php

namespace Utopia\Database\Validator;

use Exception;
use Utopia\Database\Database;
use Utopia\Validator;
use Utopia\Database\Document;

class Index extends Validator
{
    protected string $message = 'Invalid Index';
    const MAX = 768;

    /**
     * @var array<Document> $attributes
     */
    protected array $attributes = [];

    public function __construct()
    {
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
     * @param Document $collection
     * @return bool
     * @throws Exception
     */
    public function checkFulltextIndexNonString(Document $collection): bool
    {
        foreach ($collection->getAttribute('indexes', []) as $index){
            if($index->getAttribute('type') === Database::INDEX_FULLTEXT){
                foreach ($index->getAttribute('attributes', []) as $ia) {
                    $attribute = $this->attributes[$ia] ?? new Document([]);
                    if ($attribute->getAttribute('type', '') !== Database::VAR_STRING) {
                        $this->message = 'Attribute "'.$attribute->getAttribute('key', $attribute->getAttribute('$id')).'" cannot be part of a FULLTEXT index';
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /**
     * @param Document $collection
     * @return bool
     */
    public function checkIndexLength(Document $collection): bool
    {

        foreach ($collection->getAttribute('indexes', []) as $index){

            $total = 0;

            foreach ($index->getAttribute('attributes', []) as $ik => $ia){
                // Todo take care Internals...
                if(in_array($ia, ['$id', '$createdAt', '$updatedAt'])){
                    continue;
                }

                $attribute = $this->attributes[$ia];
                $attributeSize = $attribute->getAttribute('size', 0);

                $indexLength = isset($index->getAttribute('lengths')[$ik]) ? $index->getAttribute('lengths')[$ik] : 0;
                $indexLength = $indexLength === 0 ? $attributeSize : $indexLength;

                if($indexLength > $attributeSize){
                    $this->message = 'Index length("'.$indexLength.'") is longer than the key part for "'.$ia.'("'.$attributeSize.'")"';
                    return false;
                }

                // Todo: find tuning for Index type && Attribute type ...
                // $index->getAttribute('type') === 'key'

                var_dump($ia);
                var_dump($attributeSize);
                var_dump($indexLength);

                $total += $indexLength;
            }



            if($total > self::MAX){
                $this->message = 'Index Length is longer that the max ('.self::MAX.'))';
                return false;
            }

        }

        return true;

    }

    /**
     * Is valid.
     *
     * Returns true index if valid.
     * @param Document $value
     * @return bool
     */
    public function isValid($value): bool
    {
        foreach ($value->getAttribute('attributes', []) as $attribute){
            $this->attributes[$attribute->getAttribute('$id', $value->getAttribute('key'))] = $attribute;
        }

        if(!$this->checkFulltextIndexNonString($value)){
            return false;
        }

        if(!$this->checkIndexLength($value)){
            return false;
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

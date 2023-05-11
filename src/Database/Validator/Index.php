<?php

namespace Utopia\Database\Validator;

use Exception;
use Utopia\Database\Database;
use Utopia\Validator;
use Utopia\Database\Document;

class Index extends Validator
{
    protected string $message = 'Invalid Index';
    const MAX = 768; // 3072 bytes / mb4

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
    public function checkEmptyAttributes(Document $collection): bool
    {
        foreach ($collection->getAttribute('indexes', []) as $index){
            if(empty($index->getAttribute('attributes', []))){
                $this->message = 'No attributes provided for index';
                return false;
            }
        }

        return true;
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

            if($index->getAttribute('type') === Database::INDEX_FULLTEXT){
                return true;
            }

            $total = 0;

            foreach ($index->getAttribute('attributes', []) as $ik => $ia){

                // Todo Add internals to attributes collection...
                if(in_array($ia, ['$id', '$createdAt', '$updatedAt'])){
                    continue;
                }

                $attribute = $this->attributes[$ia];

                var_dump($attribute->getAttribute('type'));

                // Todo: find tuning for Index type && Attribute type ..
                switch ($attribute->getAttribute('type')) {
                    case Database::VAR_STRING:
                        $attributeSize = $attribute->getAttribute('size', 0);
                        $indexLength = isset($index->getAttribute('lengths')[$ik]) ? $index->getAttribute('lengths')[$ik] : 0;
                        $indexLength = $indexLength === 0 ? $attributeSize : $indexLength;
                        break;

                    case Database::VAR_FLOAT:
                        $attributeSize = 2; // 8 bytes / 4 mb4
                        $indexLength = 2;
                        break;

                    default:
                        $attributeSize = 1; // 4 bytes / 4 mb4
                        $indexLength = 1;
                        break;
                }

                var_dump($ia);
                var_dump($attributeSize);
                var_dump($indexLength);

                if($indexLength > $attributeSize){
                    $this->message = 'Index length("'.$indexLength.'") is longer than the key part for "'.$ia.'("'.$attributeSize.'")"';
                    return false;
                }

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
     * @throws Exception
     */
    public function isValid($value): bool
    {
        foreach ($value->getAttribute('attributes', []) as $attribute){
            $this->attributes[$attribute->getAttribute('$id', $value->getAttribute('key'))] = $attribute;
        }

        if(!$this->checkEmptyAttributes($value)){
            return false;
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

<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;

class MySQL extends MariaDB
{
    /**
     * Get SQL Index
     *
     * @param string $collection
     * @param string $id
     * @param string $type
     * @param array $attributes
     *
     * @return string
     * @throws Exception
     */
    protected function getSQLIndex(string $collection, string $id, string $type, array $attributes): string
    {
        switch ($type) {
            case Database::INDEX_KEY:
                $type = 'INDEX';
                break;

            case Database::INDEX_ARRAY:
                $type = 'INDEX';

                foreach ($attributes as $key => $value) {
                    $attributes[$key] = '(CAST(' . $value . ' AS char(255) ARRAY))';
                }
                break;

            case Database::INDEX_UNIQUE:
                $type = 'UNIQUE INDEX';
                break;

            case Database::INDEX_FULLTEXT:
                $type = 'FULLTEXT INDEX';
                break;

            default:
                throw new Exception('Unknown Index Type:' . $type);
                break;
        }

        return 'CREATE '.$type.' `'.$id.'` ON `'.$this->getDefaultDatabase().'`.`'.$this->getNamespace().'_'.$collection.'` ( '.implode(', ', $attributes).' );';
    }

    /**
     * This function fixes indexes which has exceeded max default limits
     * with comparing the length of the string length of the collection attribute
     *
     * @param Document $index
     * @param Document[] $attributes
     * @return Document
     * @throws Exception
     */
    public function fixIndex(Document $index, array $attributes): Document {

        $max =  768; // 3072 divided by utf8mb4

        foreach ($attributes as $key => $attribute){
            $size = $index['lengths'][$key] ?? 0;

            if($attribute['type'] === Database::VAR_STRING){
                if($attribute['size'] > $max){
                    $index['lengths'][$key] = $size === 0 || $size > $max ? $max : $size;
                }
            }

        }

        return $index;
    }

}

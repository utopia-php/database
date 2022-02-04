<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Database;

class Postgres extends MariaDB
{
    /**
     * Create Database
     *
     * @param string $name
     * 
     * @return bool
     */
    public function create(string $name): bool
    {
        $name = $this->filter($name);

        return $this->getPDO()
            ->prepare("SELECT 'CREATE DATABASE  \"{$name}\" /*!40100 DEFAULT CHARACTER SET utf8mb4 */'
            WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '\"{$name}\" /*!40100 DEFAULT CHARACTER SET utf8mb4 */')")
            ->execute();
    }

    /**
     * Create Collection
     * 
     * @param string $name
     * @param Document[] $attributes (optional)
     * @param Document[] $indexes (optional)
     * @return bool
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $database = $this->getDefaultDatabase();
        $namespace = $this->getNamespace();
        $id = $this->filter($name);

        if (!empty($attributes) || !empty($indexes)) {
            foreach ($attributes as &$attribute) {
                $attrId = $this->filter($attribute->getId());
                $attrType = $this->getSQLType($attribute->getAttribute('type'), $attribute->getAttribute('size', 0), $attribute->getAttribute('signed', true));

                if($attribute->getAttribute('array')) {
                    $attrType = 'LONGTEXT';
                }

                $attribute = "`{$attrId}` {$attrType}, ";
            }

            foreach ($indexes as &$index) {
                $indexId = $this->filter($index->getId()); 
                $indexType = $this->getSQLIndexType($index->getAttribute('type'));

                $indexAttributes = $index->getAttribute('attributes');
                foreach ($indexAttributes as $key => &$attribute) {
                    $indexLength = $index->getAttribute('lengths')[$key] ?? '';
                    $indexLength = (empty($indexLength)) ? '' : '('.(int)$indexLength.')';
                    $indexOrder = $index->getAttribute('orders')[$key] ?? '';
                    $indexAttribute = $this->filter($attribute);

                    if ($indexType === Database::INDEX_FULLTEXT) {
                        $indexOrder = '';
                    }

                    $attribute = "`{$indexAttribute}`{$indexLength} {$indexOrder}";
                }

                $index = "{$indexType} `{$indexId}` (" . \implode(", ", $indexAttributes) . " ),";

            }

            $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS \"{$database}\".\"{$namespace}_{$id}\" (
                    \"_id\" SERIAL(11) NOT NULL,
                    \"_uid\" CHAR(255) NOT NULL,
                    \"_read\" " . $this->getTypeForReadPermission() . " NOT NULL,
                    \"_write\" TEXT NOT NULL,
                    " . \implode(' ', $attributes) . "
                    PRIMARY KEY (\"_id\"),
                    " . \implode(' ', $indexes) . "
                    UNIQUE KEY \"_index1\" (\"_uid\")
                  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                ->execute();

        } else {
            $this->getPDO()
                ->prepare("CREATE TABLE IF NOT EXISTS \"{$database}\".\"{$namespace}_{$id}\" (
                    \"_id\" int(11) unsigned NOT NULL AUTO_INCREMENT,
                    \"_uid\" CHAR(255) NOT NULL,
                    \"_read\" " . $this->getTypeForReadPermission() . " NOT NULL,
                    \"_write\" TEXT NOT NULL,
                    PRIMARY KEY (\"_id\"),
                    UNIQUE KEY \"_index1\" (\"_uid\")
                  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                ->execute();
        }

        // Update $this->getIndexCount when adding another default index
        return $this->createIndex($id, '_index2', $this->getIndexTypeForReadPermission(), ['_read'], [], []);
    }
}

<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Database;

class Ferret extends Mongo
{
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
        $id = $this->getNamespace() . '_' . $this->filter($name);

        // Returns an array/object with the result document
        if (empty($this->getClient()->createCollection($id))) {
            return false;
        }

        $indexesCreated = $this->client->createIndexes($id, [
            [
                'key' => ['_uid' => $this->getOrder(Database::ORDER_DESC)],
                'name' => '_uid',
                // 'unique' => true,
                // 'collation' => [ // https://docs.mongodb.com/manual/core/index-case-insensitive/#create-a-case-insensitive-index
                //     'locale' => 'en',
                //     'strength' => 1,
                // ]
            ],
            [
                'key' => ['_read' => $this->getOrder(Database::ORDER_DESC)],
                'name' => '_read_permissions',
            ]
        ]);

        if (!$indexesCreated) {
            return false;
        }

        // Since attributes are not used by this adapter
        // Only act when $indexes is provided
        if (!empty($indexes)) {
            /**
             * Each new index has format ['key' => [$attribute => $order], 'name' => $name, 'unique' => $unique]
             * @var array
             */
            $newIndexes = [];

            // using $i and $j as counters to distinguish from $key
            foreach ($indexes as $i => $index) {
                $key = [];
                $name = $this->filter($index->getId());
                $unique = false;

                $attributes = $index->getAttribute('attributes');
                $orders = $index->getAttribute('orders');

                foreach ($attributes as $j => $attribute) {
                    $attribute = $this->filter($attribute);

                    switch ($index->getAttribute('type')) {
                        case Database::INDEX_KEY:
                            $order = $this->getOrder($this->filter($orders[$i] ?? Database::ORDER_ASC));
                            break;
                        case Database::INDEX_FULLTEXT:
                            // MongoDB fulltext index is just 'text'
                            // Not using Database::INDEX_KEY for clarity
                            // $order = 'text';
                            break;
                        case Database::INDEX_UNIQUE:
                            $order = $this->getOrder($this->filter($orders[$i] ?? Database::ORDER_ASC));
                            $unique = true;
                            break;
                        default:
                            // index not supported
                            return false;
                    }

                    $key[$attribute] = $order;
                }

                $newIndexes[$i] = ['key' => $key, 'name' => $name, 'unique' => $unique];
            }

            if (!$this->getClient()->createIndexes($name, $newIndexes)) {
                return false;
            }
        }

        return true;
    }
}

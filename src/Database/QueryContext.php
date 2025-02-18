<?php

namespace Utopia\Database;

class QueryContext
{
    public const TYPE_EQUAL = 'equal';

    protected array $collections;

    protected array $alias;

    protected array $queries;

    /**
     * @param  array<Document>  $collections
     *
     * @throws \Exception
     */
    public function __construct(array $collections, array $queries)
    {
        $this->collections = $collections;

        foreach ($queries as $query) {
            $q = clone $query;

            if (! $q instanceof Query) {
                try {
                    $q = Query::parse($q);
                } catch (\Throwable $e) {
                    throw new \Exception('Invalid query: '.$e->getMessage());
                }
            }

            $this->queries[] = $q;
        }

        //        foreach ($collections as $i => $collection) {
        //            if ($i === 0) {
        //                $this->aliases[''] = $collection->getId();
        //            }
        //
        //            // $this->collections[$collection->getId()] = $collection->getArrayCopy();
        //
        //            $attributes = $collection->getAttribute('attributes', []);
        //            foreach ($attributes as $attribute) {
        //                // todo: internal id's?
        //                $this->schema[$collection->getId()][$attribute->getAttribute('key', $attribute->getAttribute('$id'))] = $attribute->getArrayCopy();
        //            }
        //        }

    }

    public function __clone(): void
    {
        foreach ($this->values as $index => $value) {
            if ($value instanceof self) {
                $this->values[$index] = clone $value;
            }
        }
    }

    public function getCollections(): array
    {
        return $this->collections;
    }

    public function getQueries(): array
    {
        return $this->queries;
    }
}

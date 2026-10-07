<?php

namespace Utopia\Database;

enum Filter: string
{
    case Json = 'json';
    case Datetime = 'datetime';
    case Point = 'point';
    case LineString = 'linestring';
    case Polygon = 'polygon';
    case Vector = 'vector';
    case Object = 'object';

    /**
     * @param  list<self|string>  $filters
     * @return list<string>
     */
    public static function names(array $filters): array
    {
        $names = [];
        foreach ($filters as $filter) {
            $names[] = $filter instanceof self ? $filter->value : $filter;
        }

        return $names;
    }
}

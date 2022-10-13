<?php

namespace Utopia\Database\Adapter\Mongo;

class MongoCommand
{
    /**
     * Defines commands Mongo uses over wire protocol.
     */
    public const CREATE = 'create';

    public const DELETE = 'delete';

    public const FIND = 'find';

    public const FIND_AND_MODIFY = 'findAndModify';

    public const GET_LAST_ERROR = 'getLastError';

    public const GET_MORE = 'getMore';

    public const INSERT = 'insert';

    public const RESET_ERROR = 'resetError';

    public const UPDATE = 'update';

    public const COUNT = 'count';

    public const AGGREGATE = 'aggregate';

    public const DISTINCT = 'distinct';

    public const MAP_REDUCE = 'mapReduce';
}

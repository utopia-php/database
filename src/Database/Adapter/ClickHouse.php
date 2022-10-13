<?php

namespace Utopia\Database\Adapter;

use PDO;
use Exception;
use PDOException;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\ID;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class ClickHouse extends MariaDB
{
}
<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

/**
 * Thrown when a database operation exceeds a configured limit (e.g. max documents, max attributes).
 */
class Limit extends Exception
{
}

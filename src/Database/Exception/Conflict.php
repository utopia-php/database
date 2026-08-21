<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

/**
 * Thrown when a database operation encounters a conflict, such as a concurrent modification.
 */
class Conflict extends Exception
{
}

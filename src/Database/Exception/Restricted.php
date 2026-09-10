<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

/**
 * Thrown when an operation is restricted due to a relationship constraint (e.g. restrict on delete).
 */
class Restricted extends Exception
{
}

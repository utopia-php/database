<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

/**
 * Thrown when a transaction's commit could not be confirmed: its writes may or may not be stored, so it is not run
 * again.
 */
class Unconfirmed extends Exception
{
}

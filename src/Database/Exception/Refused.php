<?php

namespace Utopia\Database\Exception;

use Utopia\Database\Exception;

/**
 * Thrown when an adapter reports, without raising an error, that it did not apply a schema change.
 */
class Refused extends Exception
{
}

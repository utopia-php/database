<?php

namespace Utopia\Database\Exception;

/**
 * Thrown when a resource already exists with a definition other than the requested one, such as another tenant's column of another type in a shared table.
 */
class Mismatch extends Duplicate
{
}

<?php

namespace Utopia\Database\Exception;

/**
 * Thrown when a write violates a unique index; a conflicting document ID throws the parent Duplicate instead.
 */
class Unique extends Duplicate
{
}

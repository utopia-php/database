<?php

namespace Utopia\Database;

/**
 * Defines the modes for setting attribute values on a document.
 */
enum SetType: string
{
    case Assign = 'assign';
    case Prepend = 'prepend';
    case Append = 'append';
}

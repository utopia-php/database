<?php

namespace Utopia\Database;

/**
 * Defines which side of a relationship a collection is on.
 */
enum RelationSide: string
{
    case Parent = 'parent';
    case Child = 'child';
}

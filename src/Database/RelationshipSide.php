<?php

namespace Utopia\Database;

enum RelationshipSide: string
{
    case Parent = 'parent';
    case Child = 'child';
}

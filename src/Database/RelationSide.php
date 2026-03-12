<?php

namespace Utopia\Database;

enum RelationSide: string
{
    case Parent = 'parent';
    case Child = 'child';
}

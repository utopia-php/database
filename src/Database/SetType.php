<?php

namespace Utopia\Database;

enum SetType: string
{
    case Assign = 'assign';
    case Prepend = 'prepend';
    case Append = 'append';
}

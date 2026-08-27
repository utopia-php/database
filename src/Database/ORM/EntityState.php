<?php

namespace Utopia\Database\ORM;

enum EntityState
{
    case New;
    case Managed;
    case Removed;
}

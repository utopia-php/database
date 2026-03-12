<?php

namespace Utopia\Database;

enum CursorDirection: string
{
    case Before = 'before';
    case After = 'after';
}

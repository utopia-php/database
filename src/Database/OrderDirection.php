<?php

namespace Utopia\Database;

enum OrderDirection: string
{
    case ASC = 'ASC';
    case DESC = 'DESC';
    case RANDOM = 'RANDOM';
}

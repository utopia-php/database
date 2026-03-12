<?php

namespace Utopia\Database;

enum PermissionType: string
{
    case Create = 'create';
    case Read = 'read';
    case Update = 'update';
    case Delete = 'delete';
    case Write = 'write';
}

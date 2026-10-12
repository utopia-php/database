<?php

namespace Utopia\Database;

/**
 * Defines the types of permissions that can be granted on database resources.
 */
enum PermissionType: string
{
    case Create = 'create';
    case Read = 'read';
    case Update = 'update';
    case Delete = 'delete';
    case Write = 'write';
}

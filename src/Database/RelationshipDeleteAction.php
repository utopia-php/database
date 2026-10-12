<?php

namespace Utopia\Database;

use Utopia\Query\Schema\ForeignKeyAction;

enum RelationshipDeleteAction: string
{
    case Cascade = 'cascade';
    case Restrict = 'restrict';
    case SetNull = 'setNull';

    public function toForeignKeyAction(): ForeignKeyAction
    {
        return match ($this) {
            self::Cascade => ForeignKeyAction::Cascade,
            self::Restrict => ForeignKeyAction::Restrict,
            self::SetNull => ForeignKeyAction::SetNull,
        };
    }
}

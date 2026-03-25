<?php

namespace Utopia\Database\Schema;

enum SchemaChangeType
{
    case CreateCollection;
    case DropCollection;
    case AddAttribute;
    case DropAttribute;
    case ModifyAttribute;
    case AddIndex;
    case DropIndex;
    case AddRelationship;
    case DropRelationship;
}

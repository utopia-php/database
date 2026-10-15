<?php

namespace Utopia\Database;

enum RelationshipType: string
{
    case OneToOne = 'oneToOne';
    case OneToMany = 'oneToMany';
    case ManyToOne = 'manyToOne';
    case ManyToMany = 'manyToMany';
}

<?php

namespace Utopia\Database;

enum RelationType: string
{
    case OneToOne = 'oneToOne';
    case OneToMany = 'oneToMany';
    case ManyToOne = 'manyToOne';
    case ManyToMany = 'manyToMany';
}

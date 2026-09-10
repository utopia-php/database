<?php

namespace Utopia\Database;

/**
 * Defines the cardinality types for relationships between collections.
 */
enum RelationType: string
{
    case OneToOne = 'oneToOne';
    case OneToMany = 'oneToMany';
    case ManyToOne = 'manyToOne';
    case ManyToMany = 'manyToMany';
}

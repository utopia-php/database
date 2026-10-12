<?php

namespace Utopia\Database\Event\Permission;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

/**
 * The typed form of Event::PermissionsDelete. That event only names the statements a permissions write hook runs to delete
 * permission rows, for the adapter's transforms and timeouts: Database never dispatches it to lifecycle hooks.
 */
final readonly class Deleted extends Domain
{
    /**
     * @param  list<string>  $permissions
     */
    public function __construct(
        public string $collection,
        public string $document,
        public array $permissions,
    ) {
        parent::__construct(Event::PermissionsDelete);
    }
}

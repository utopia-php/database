<?php

namespace Utopia\Database\Event\Permission;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class Read extends Domain
{
    /**
     * @param  list<string>  $permissions
     */
    public function __construct(
        public string $collection,
        public string $document,
        public array $permissions,
    ) {
        parent::__construct(Event::PermissionsRead);
    }
}

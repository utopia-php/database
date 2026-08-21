<?php

namespace Tests\Unit\ORM;

use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Database\ORM\Mapping\Tenant;
use Utopia\Query\Schema\ColumnType;

#[Entity(collection: 'tenant_items')]
class TestTenantEntity
{
    #[Id]
    public string $id = '';

    #[Tenant]
    public ?string $tenantId = null;

    #[Column(type: ColumnType::String, size: 100)]
    public string $name = '';
}

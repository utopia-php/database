<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Capability;

final class MongoAttributesSupportTest extends TestCase
{
    public function testDefinedAttributesFollowsSupportFlag(): void
    {
        $adapter = new class () extends Mongo {
            public function __construct()
            {
            }
        };

        $this->assertTrue($adapter->supports(Capability::DefinedAttributes));

        $adapter->setSupportForAttributes(false);
        $this->assertFalse($adapter->supports(Capability::DefinedAttributes));

        $adapter->setSupportForAttributes(true);
        $this->assertTrue($adapter->supports(Capability::DefinedAttributes));
    }
}

<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Capability;

final class MongoAttributesSupportTest extends TestCase
{
    public function testDefinedAttributesFollowsTheSchemalessMode(): void
    {
        $adapter = new class () extends Mongo {
            public function __construct()
            {
            }
        };

        $this->assertFalse($adapter->isSchemaless());
        $this->assertTrue($adapter->supports(Capability::DefinedAttributes));

        $this->assertSame($adapter, $adapter->setSchemaless(true));
        $this->assertTrue($adapter->isSchemaless());
        $this->assertFalse($adapter->supports(Capability::DefinedAttributes));

        $adapter->setSchemaless(false);
        $this->assertFalse($adapter->isSchemaless());
        $this->assertTrue($adapter->supports(Capability::DefinedAttributes));
    }
}

<?php

namespace Tests\Unit\Adapter;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Capability;

#[RequiresPhpExtension('redis')]
final class AttributeSupportToggleTest extends TestCase
{
    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function schemaAdapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'redis' => [static fn (): Adapter => new RedisAdapter(self::createStub(Redis::class))],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('schemaAdapters')]
    public function testSchemaAdaptersReportThatAttributesStayDefined(Closure $adapter): void
    {
        $adapter = $adapter();

        $this->assertTrue($adapter->setSupportForAttributes(false), 'An adapter that always enforces its schema must not report that attribute support was turned off');
        $this->assertTrue($adapter->supports(Capability::DefinedAttributes));
        $this->assertTrue($adapter->setSupportForAttributes(true));
        $this->assertTrue($adapter->supports(Capability::DefinedAttributes));
    }
}

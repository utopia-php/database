<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Pools\Pool as UtopiaPool;

final class ReadWritePoolRoutingTest extends TestCase
{
    public function testEveryRoutedMethodExistsOnTheAdapterOrAFeature(): void
    {
        $routes = $this->routes();
        $known = $this->adapterMethods();
        $stale = [];

        foreach ($routes as $list => $methods) {
            $this->assertNotSame([], $methods, "{$list} is empty");

            foreach ($methods as $method) {
                if (! isset($known[$method])) {
                    $stale[] = "{$list}: {$method}";
                }
            }
        }

        $this->assertSame([], $stale, 'ReadWritePool routes method names that no adapter or feature declares');
    }

    public function testNoMethodIsRoutedTwice(): void
    {
        $counts = \array_count_values(\array_merge(...\array_values($this->routes())));
        $duplicates = \array_keys(\array_filter($counts, static fn (int $count): bool => $count > 1));

        $this->assertSame([], $duplicates, 'A method in more than one routing list has no single destination');
    }

    /**
     * @return array<string, list<string>>
     */
    private function routes(): array
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);

        $pool = new class ($connections, $connections) extends ReadWritePool {
            /**
             * @return array<string, list<string>>
             */
            public function routes(): array
            {
                return [
                    'READ_METHODS' => self::READ_METHODS,
                    'METADATA_METHODS' => self::METADATA_METHODS,
                    'WRITE_POOL_METADATA_METHODS' => self::WRITE_POOL_METADATA_METHODS,
                ];
            }
        };

        return $pool->routes();
    }

    /**
     * @return array<string, true>
     */
    private function adapterMethods(): array
    {
        $known = [];

        foreach ((new ReflectionClass(Adapter::class))->getMethods() as $method) {
            if (! $method->isPrivate()) {
                $known[$method->getName()] = true;
            }
        }

        $directory = \dirname((string) (new ReflectionClass(Adapter::class))->getFileName()) . '/Adapter/Feature';

        foreach (\glob($directory . '/*.php') ?: [] as $file) {
            /** @var class-string $feature */
            $feature = 'Utopia\\Database\\Adapter\\Feature\\' . \basename($file, '.php');

            foreach ((new ReflectionClass($feature))->getMethods() as $method) {
                $known[$method->getName()] = true;
            }
        }

        return $known;
    }
}

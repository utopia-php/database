<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionMethod;
use ReflectionParameter;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;

final class PoolParityTest extends TestCase
{
    public function testPoolDelegatesEveryFeatureMethod(): void
    {
        $pool = new ReflectionClass(Pool::class);
        $missing = [];

        foreach (self::featureMethods() as $feature => $method) {
            $name = $method->getName();

            if (! $pool->hasMethod($name)) {
                $missing[] = "{$feature}::{$name}() is missing";

                continue;
            }

            $delegate = $pool->getMethod($name);

            if (! $delegate->isPublic() || $delegate->isAbstract()) {
                $missing[] = "{$feature}::{$name}() is not a public concrete method";
            } elseif ($delegate->getDeclaringClass()->getName() !== Pool::class) {
                $missing[] = "{$feature}::{$name}() is inherited from {$delegate->getDeclaringClass()->getName()} instead of delegated";
            }
        }

        $this->assertSame([], $missing, 'Pool must delegate every method of every adapter feature');
    }

    public function testPoolKeepsEveryFeatureParameterName(): void
    {
        $pool = new ReflectionClass(Pool::class);
        $mismatched = [];

        foreach (self::featureMethods() as $feature => $method) {
            $name = $method->getName();

            if (! $pool->hasMethod($name)) {
                continue;
            }

            $expected = self::parameterNames($method);
            $actual = self::parameterNames($pool->getMethod($name));

            if ($expected !== $actual) {
                $mismatched[] = "{$feature}::{$name}(" . \implode(', ', $expected) . ') is Pool::' . $name . '(' . \implode(', ', $actual) . ')';
            }
        }

        $this->assertSame([], $mismatched, 'Named arguments that work on an adapter must work on Pool');
    }

    public function testEveryFeatureFileDeclaresAnInterface(): void
    {
        $features = self::features();

        $this->assertNotSame([], $features, 'No adapter feature interfaces were found');

        foreach ($features as $feature) {
            $this->assertTrue(\interface_exists($feature), "{$feature} is not an interface");
        }
    }

    /**
     * @return list<class-string>
     */
    private static function features(): array
    {
        $directory = \dirname((string) (new ReflectionClass(Adapter::class))->getFileName()) . '/Adapter/Feature';
        $features = [];

        foreach (\glob($directory . '/*.php') ?: [] as $file) {
            /** @var class-string $feature */
            $feature = 'Utopia\\Database\\Adapter\\Feature\\' . \basename($file, '.php');
            $features[] = $feature;
        }

        return $features;
    }

    /**
     * @return iterable<class-string, ReflectionMethod>
     */
    private static function featureMethods(): iterable
    {
        foreach (self::features() as $feature) {
            foreach ((new ReflectionClass($feature))->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
                yield $feature => $method;
            }
        }
    }

    /**
     * @return list<string>
     */
    private static function parameterNames(ReflectionMethod $method): array
    {
        return \array_map(static fn (ReflectionParameter $parameter): string => $parameter->getName(), $method->getParameters());
    }
}

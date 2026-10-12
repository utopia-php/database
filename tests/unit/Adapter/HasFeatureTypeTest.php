<?php

namespace Tests\Unit\Adapter;

use PHPStan\Testing\TypeInferenceTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

class HasFeatureTypeTest extends TypeInferenceTestCase
{
    /**
     * @return iterable<mixed>
     */
    public static function types(): iterable
    {
        yield from self::gatherAssertTypes(__DIR__ . '/Data/HasFeature/narrowing.php');
    }

    #[DataProvider('types')]
    public function testHasFeatureDoesNotClaimTheAdapterImplementsTheFeature(string $assertType, string $file, mixed ...$arguments): void
    {
        $this->assertFileAsserts($assertType, $file, ...$arguments);
    }
}

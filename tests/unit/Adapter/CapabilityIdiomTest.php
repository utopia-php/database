<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use ReflectionClass;
use SplFileInfo;
use Utopia\Database\Adapter;

final class CapabilityIdiomTest extends TestCase
{
    /**
     * A Pool answers for the adapter it borrows without implementing its Feature interfaces, so instanceof on any
     * object other than the asker itself tells a proxied caller the wrong thing. hasFeature() is the one question.
     */
    public function testOnlyAnAdapterAsksItselfForAFeatureWithInstanceof(): void
    {
        $offences = [];

        foreach (self::sources() as $path => $lines) {
            foreach ($lines as $number => $line) {
                if (\preg_match_all('/(\S+)\s+instanceof\s+\\\\?(?:Utopia\\\\Database\\\\Adapter\\\\)?Feature\\\\/', $line, $matches) === 0) {
                    continue;
                }

                foreach ($matches[1] as $subject) {
                    if (\ltrim($subject, '(!') !== '$this') {
                        $offences[] = $path.':'.($number + 1);
                    }
                }
            }
        }

        $this->assertSame([], $offences, 'Ask hasFeature() instead of instanceof Feature\\');
    }

    public function testNoSourceProbesAnAdapterWithMethodExists(): void
    {
        $offences = [];

        foreach (self::sources() as $path => $lines) {
            foreach ($lines as $number => $line) {
                if (\preg_match('/method_exists\(\s*\$(?:this->)?adapter\b/', $line) === 1) {
                    $offences[] = $path.':'.($number + 1);
                }
            }
        }

        $this->assertSame([], $offences, 'Ask hasFeature() instead of method_exists()');
    }

    public function testTheScanFindsTheSources(): void
    {
        $this->assertArrayHasKey('Adapter/Pool.php', self::sources());
        $this->assertArrayHasKey('Database.php', self::sources());
    }

    /**
     * @return array<string, list<string>>
     */
    private static function sources(): array
    {
        $root = \dirname((string) (new ReflectionClass(Adapter::class))->getFileName());
        $sources = [];

        /** @var SplFileInfo $file */
        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root, RecursiveDirectoryIterator::SKIP_DOTS)) as $file) {
            if ($file->getExtension() !== 'php') {
                continue;
            }

            $lines = \file($file->getPathname(), FILE_IGNORE_NEW_LINES);
            $sources[\substr($file->getPathname(), \strlen($root) + 1)] = $lines === false ? [] : $lines;
        }

        return $sources;
    }
}
